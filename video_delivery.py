"""Small previews and streaming split ZIPs, kept beside the original on its SSD."""

import asyncio
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import threading
import zipfile


class VideoDeliveryError(RuntimeError):
    pass


class SplitZipWriter:
    """An unseekable ZIP output stream with a strict limit for every volume."""

    def __init__(self, folder: Path, name: str, limit: int, cancelled: threading.Event):
        self.folder, self.name, self.limit, self.cancelled = folder, name, limit, cancelled
        self.position = 0
        self.stream = None
        self.paths = []

    def tell(self):
        return self.position

    def write(self, data):
        if self.cancelled.is_set():
            raise InterruptedError("Archive preparation cancelled")
        original_size = len(data)
        view = memoryview(data)
        while view:
            if self.stream is None:
                path = self.folder / f"{self.name}.zip.{len(self.paths) + 1:03d}"
                self.paths.append(path)
                self.stream = path.open("wb")
            size = min(len(view), self.limit - self.position % self.limit)
            self.stream.write(view[:size])
            self.position += size
            view = view[size:]
            if self.position % self.limit == 0:
                self.close()
        return original_size

    def flush(self):
        if self.stream:
            self.stream.flush()

    def close(self):
        if self.stream:
            self.stream.close()
            self.stream = None


def _prepare_video_parts(path: Path, limit: int, cancelled: threading.Event):
    if limit <= 0:
        raise VideoDeliveryError("Некоректний ліміт розміру частин архіву.")
    if path.is_symlink() or not path.is_file():
        raise VideoDeliveryError("Оригінал відео недоступний на диску.")
    source_stat = path.stat()
    fingerprint = [path.name, source_stat.st_size, source_stat.st_mtime_ns, limit, 1]
    key = hashlib.sha256(json.dumps(fingerprint).encode()).hexdigest()[:24]
    root = path.parent / ".delivery"
    target = root / key
    if root.is_symlink() or target.is_symlink():
        raise VideoDeliveryError("Недоступна папка підготовки відео.")
    root.mkdir(exist_ok=True)
    try:
        manifest = json.loads((target / "parts.json").read_text())
        parts = manifest["parts"]
        if manifest["source"] == fingerprint and parts and all(
            part["name"] == f"{path.name}.zip.{i:03d}"
            and 0 < part["size"] <= limit
            and not (target / part["name"]).is_symlink()
            and (target / part["name"]).is_file()
            and (target / part["name"]).stat().st_size == part["size"]
            for i, part in enumerate(parts, 1)
        ):
            return [target / part["name"] for part in parts], True
    except (OSError, ValueError, KeyError, TypeError):
        pass
    if shutil.disk_usage(root).free < source_stat.st_size + 1024 * 1024:
        raise VideoDeliveryError("На SSD бракує місця для частин архіву. Оригінал відео збережено.")
    with tempfile.TemporaryDirectory(prefix=".building-", dir=root) as temporary:
        folder = Path(temporary)
        writer = SplitZipWriter(folder, path.name, limit, cancelled)
        try:
            # Video is already compressed. ZIP_STORED preserves bytes without wasting CPU.
            with zipfile.ZipFile(writer, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                archive.write(path, arcname=path.name)
        finally:
            writer.close()
        current = path.stat()
        if (current.st_size, current.st_mtime_ns) != (source_stat.st_size, source_stat.st_mtime_ns):
            raise VideoDeliveryError("Відео змінилося під час підготовки. Повтори надсилання з історії.")
        if cancelled.is_set():
            raise InterruptedError("Archive preparation cancelled")
        manifest = {"source": fingerprint, "parts": [
            {"name": part.name, "size": part.stat().st_size} for part in writer.paths]}
        (folder / "parts.json").write_text(json.dumps(manifest), encoding="utf-8")
        if target.exists():
            shutil.rmtree(target)
        folder.rename(target)
    return [target / part["name"] for part in manifest["parts"]], False


async def prepare_video_parts(path: Path, limit: int):
    """Finish or cancel the disk worker before releasing the caller's delivery lock."""
    cancelled = threading.Event()
    worker = asyncio.create_task(asyncio.to_thread(_prepare_video_parts, path, limit, cancelled))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        cancelled.set()
        with contextlib.suppress(InterruptedError, OSError, VideoDeliveryError):
            await worker
        raise
    except OSError as exc:
        raise VideoDeliveryError("Не вдалося підготувати архів на SSD. Оригінал відео збережено.") from exc


async def create_video_preview(path: Path, folder: Path, *, timeout: int = 180) -> Path:
    """At most 15 seconds, 360px, H.264/AAC; the source is never transcoded in place."""
    preview = folder / "preview.mp4"
    if preview.is_file() and not preview.is_symlink() and 0 < preview.stat().st_size <= 4 * 1024 ** 2:
        return preview
    temporary = folder / "preview.tmp.mp4"
    temporary.unlink(missing_ok=True)
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
            "-i", str(path), "-t", "15", "-map", "0:v:0", "-map", "0:a?",
            "-vf", "scale=360:360:force_original_aspect_ratio=decrease:force_divisible_by=2",
            "-r", "15", "-c:v", "libx264", "-preset", "veryfast", "-threads", "2",
            "-crf", "32", "-maxrate", "400k", "-bufsize", "800k", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "48k", "-ac", "1", "-movflags", "+faststart",
            "-fs", str(4 * 1024 ** 2), str(temporary),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            start_new_session=(os.name == "posix"))
        code = await asyncio.wait_for(process.wait(), timeout=timeout)
        if code or not temporary.is_file() or not 0 < temporary.stat().st_size <= 4 * 1024 ** 2:
            raise VideoDeliveryError("Не вдалося створити прев’ю; оригінал буде надіслано частинами.")
        temporary.replace(preview)
        return preview
    except (OSError, asyncio.TimeoutError) as exc:
        raise VideoDeliveryError("Не вдалося створити прев’ю; оригінал буде надіслано частинами.") from exc
    finally:
        if process and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            await process.wait()
        temporary.unlink(missing_ok=True)
