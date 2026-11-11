"""Bounded yt-dlp downloads. Only after_move results become persistent videos."""

import asyncio
import contextlib
import json
import os
import signal
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

from media_support import DownloadActivity

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".webm", ".mov", ".m4v"}
MANIFEST = "videos.json"


class VideoDownloadError(RuntimeError):
    def __init__(self, message: str, *, needs_login: bool = False, not_video: bool = False,
                 suggest_login: bool = False):
        super().__init__(message)
        self.needs_login = needs_login
        self.not_video = not_video
        self.suggest_login = suggest_login or needs_login


def classify_download_error(stderr: str) -> VideoDownloadError:
    """Offer an account for ambiguous site responses without claiming login is required."""
    error = stderr.lower()
    if "no module named yt_dlp" in error:
        return VideoDownloadError("На сервері потрібно встановити залежності: pip install -r requirements.txt.")
    if "there is no video in this post" in error:
        return VideoDownloadError("У цьому дописі немає відео.", not_video=True)
    if any(s in error for s in ("log in", "login", "sign in", "sign-in", "logged in", "logged-in",
                                "cookies", "checkpoint", "authentication required", "registered users",
                                "session has expired", "http error 401", "unauthorized")):
        return VideoDownloadError(
            "Сервіс просить авторизацію. Підключи або онови cookies та повтори завантаження.",
            needs_login=True)
    if any(s in error for s in ("no space left", "disk quota exceeded", "read-only file system",
                                "permission denied")):
        return VideoDownloadError("Не вдалося записати відео на диск. Перевір вільне місце та права доступу до сховища.")
    if any(s in error for s in ("timed out", "timeout", "temporary failure in name resolution",
                                "name or service not known", "failed to resolve", "unable to resolve",
                                "connection refused", "connection reset", "certificate verify failed",
                                "network is unreachable")):
        return VideoDownloadError("Не вдалося з’єднатися із сайтом. Спробуй докачати відео пізніше.")
    if any(s in error for s in ("max-filesize", "file is too large")):
        return VideoDownloadError("Відео перевищує налаштований ліміт розміру завантаження.")
    if "unsupported url" in error:
        return VideoDownloadError("Це посилання не підтримується. Спробуй пряму адресу відео або Reel.")
    # Facebook's 'Cannot parse data', 403 and generic unavailable responses do not
    # reliably distinguish a login wall, a deleted post and an extractor failure.
    return VideoDownloadError(
        "Не вдалося отримати відео. Сайт міг обмежити доступ або змінити відповідь.",
        suggest_login=True)


def video_platform(url: str) -> str | None:
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        if (parsed.scheme not in ("https", "http") or parsed.username or parsed.password
                or parsed.port not in (None, 80, 443) or any(ord(c) < 33 for c in url)):
            return None
        for platform in ("facebook", "instagram", "tiktok"):
            root = platform + ".com"
            if host == root or host.endswith("." + root):
                # Leave explicit photo/album links with the existing gallery downloader.
                if platform == "facebook" and (parsed.path.startswith(("/photo", "/album"))
                                                or "/photos/" in parsed.path):
                    return None
                if platform == "tiktok" and "/photo/" in parsed.path:
                    return None
                if platform == "instagram" and not parsed.path.startswith(("/p/", "/reel/", "/reels/", "/tv/", "/share/")):
                    return None  # Profile galleries still use gallery-dl.
                return platform
        if host == "fb.watch" or host == "www.fb.watch":
            return "facebook"
    except ValueError:
        pass
    return None


def video_root(images: Path, configured: str = "") -> Path:
    """Nested by default so images -> SSD also redirects all video/temp files."""
    return Path(configured).expanduser() if configured else images / "videos"


def ensure_video_storage(images: Path, root: Path):
    # Never recreate a missing SSD target behind a dangling symlink.
    for path in (images, root, *root.parents):
        if path.is_symlink() and not path.exists():
            raise VideoDownloadError("SSD недоступний: символьне посилання на сховище не працює.")
    root.mkdir(parents=True, exist_ok=True)


def saved_videos(folder: Path) -> list[dict]:
    """The manifest survives restarts; never discover unfinished files by extension."""
    try:
        items = json.loads((folder / MANIFEST).read_text())
    except (OSError, ValueError):
        return []
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)
            and isinstance(item.get("file"), str) and Path(item["file"]).name == item["file"]
            and Path(item["file"]).suffix.lower() in VIDEO_EXTENSIONS]


def save_video(folder: Path, item: dict):
    items = [old for old in saved_videos(folder) if old["file"] != item["file"]]
    items.append(item)
    temporary = folder / (MANIFEST + ".tmp")
    temporary.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
    temporary.replace(folder / MANIFEST)


def download_command(url: str, folder: Path, cookies: Path | None, max_bytes: int) -> list[str]:
    if not video_platform(url):
        raise VideoDownloadError("Непідтримуване посилання на відео.")
    args = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-plugin-dirs",
            "--no-cache-dir", "--no-progress", "--no-warnings", "--no-simulate",
            "--no-playlist", "--playlist-end", "10", "--socket-timeout", "30",
            "--retries", "2", "--fragment-retries", "2", "--max-filesize", str(max_bytes),
            "--restrict-filenames", "--no-overwrites", "-P", str(folder.resolve()),
            "-o", "%(id)s_%(title).80B.%(ext)s", "-S", "res:1080",
            "-f", "bv*[vcodec^=avc1][ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b",
            "--merge-output-format", "mp4/mkv",
            "--print", 'after_move:%(.{filepath,title,id,duration,width,height})j']
    if cookies:
        args += ["--cookies", str(cookies)]
    return args + ["--", url]


async def download_video(url: str, folder: Path, cookies: bytes | None, *,
                         timeout: int = 1800, stall_timeout: int = 300,
                         max_bytes: int = 2 * 1024 ** 3):
    folder.mkdir(parents=True, exist_ok=True)
    # Cookies never enter the persistent video tree. yt-dlp may rewrite its cookie jar.
    with tempfile.TemporaryDirectory(prefix="pix-video-session-") as private_dir:
        cookie_path = Path(private_dir) / "cookies.txt" if cookies else None
        if cookie_path:
            cookie_path.write_bytes(cookies)
            cookie_path.chmod(0o600)
        process = await asyncio.create_subprocess_exec(
            *download_command(url, folder, cookie_path, max_bytes),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=(os.name == "posix"), limit=256 * 1024)
        loop = asyncio.get_running_loop()
        activity = DownloadActivity(loop.time())
        errors = bytearray()
        completed = 0

        async def output():
            nonlocal completed
            while line := await process.stdout.readline():
                activity.touch(loop.time())
                try:
                    info = json.loads(line)
                    path = Path(info["filepath"])
                    if (path.is_symlink() or path.resolve().parent != folder.resolve()
                            or path.suffix.lower() not in VIDEO_EXTENSIONS or not path.is_file()
                            or path.stat().st_size == 0):
                        continue
                    save_video(folder, {"file": path.name, "title": str(info.get("title") or path.stem),
                                        "duration": info.get("duration"), "width": info.get("width"),
                                        "height": info.get("height")})
                    completed += 1
                except (ValueError, KeyError, TypeError):
                    continue

        async def error_output():
            while chunk := await process.stderr.read(8192):
                errors.extend(chunk)
                del errors[:-65536]

        readers = [asyncio.create_task(output()), asyncio.create_task(error_output())]
        waiter = asyncio.create_task(process.wait())
        try:
            deadline = loop.time() + timeout
            while not waiter.done():
                await asyncio.wait({waiter}, timeout=1)
                for reader in readers:
                    if reader.done() and reader.exception():
                        raise reader.exception()
                if loop.time() >= deadline or activity.stalled(folder, loop.time(), stall_timeout):
                    raise VideoDownloadError("Завантаження відео перервано за таймаутом. Можна докачати з історії.")
                size = 0
                for path in folder.iterdir():
                    with contextlib.suppress(FileNotFoundError):
                        size += path.stat().st_size if path.is_file() else 0
                if size > max_bytes * 3:
                    raise VideoDownloadError("Перевищено ліміт місця для завантаження відео.")
            await asyncio.gather(*readers)
            if waiter.result() != 0 or not completed:
                # Do not expose extractor output: it may contain signed URLs or cookies.
                raise classify_download_error(errors.decode(errors="replace"))
        finally:
            # Stop ffmpeg children as well as yt-dlp on timeout/cancellation.
            with contextlib.suppress(ProcessLookupError):
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                elif process.returncode is None:
                    process.kill()
            await waiter
            for reader in readers:
                if not reader.done():
                    reader.cancel()
            await asyncio.gather(*readers, return_exceptions=True)
