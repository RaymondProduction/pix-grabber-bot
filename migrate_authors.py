"""Backfill identities without downloading media or changing existing archives.

Run from the bot directory: python migrate_authors.py --online
Default is offline; --retry-unavailable retries previously checked sources.
Progress and reports contain counts only, never extractor output or credentials.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen
import zipfile

import db
from author_metadata import extract_authors, legacy_pixiv_authors, source_platform, write_author_sidecar
from video_support import video_platform


def owner_chat(entry):
    owner = entry.get("owner_chat_id") or entry.get("auto_resume_chat_id")
    if owner:
        return str(owner)
    chats = {str(m["chat_id"]) for key in ("archive_messages", "image_messages") for m in entry.get(key, [])}
    preview = entry.get("preview_message") or {}
    if preview.get("chat_id"):
        chats.add(str(preview["chat_id"]))
    return next(iter(chats)) if len(chats) == 1 else None


def metadata_authors(data, url):
    found = []
    if isinstance(data, list):
        for item in data:
            found.extend(metadata_authors(item, url))
    elif isinstance(data, dict):
        normalized = data.get("authors")
        if isinstance(normalized, list):
            found.extend(a for a in normalized if isinstance(a, dict)
                         and all(a.get(k) for k in ("platform", "identity_key", "role")))
        found.extend(extract_authors(data, url))
    return list({(a["platform"], a["identity_key"], a["role"]): a for a in found}.values())


def local_authors(entry, archive_dir):
    url = entry["url"]
    paths = [archive_dir / "metadata" / f"download-{entry['_id']}.authors.json"]
    folder = Path(entry.get("download_dir") or "__missing__")
    if folder.is_dir():
        paths.extend(p for p in folder.rglob("*.json") if ".delivery" not in p.parts)
    for path in paths:
        try:
            if path.stat().st_size > 8 * 1024 ** 2:
                continue
            authors = metadata_authors(json.loads(path.read_text()), url)
            if authors:
                return authors, "local-metadata"
        except (OSError, ValueError):
            pass
    authors = legacy_pixiv_authors(entry)
    if authors:
        return authors, "gallery-dl-directory"
    # Old archive layouts may have sidecars even if the working directory was removed.
    for filename in entry.get("zip_parts", []):
        path = Path(filename)
        if not path.is_file():
            path = archive_dir / path.name
        try:
            with zipfile.ZipFile(path) as archive:
                for info in archive.infolist():
                    if info.filename.endswith(".json") and info.file_size <= 8 * 1024 ** 2:
                        authors = metadata_authors(json.loads(archive.read(info)), url)
                        if authors:
                            return authors, "archive-metadata"
        except (OSError, ValueError, zipfile.BadZipFile, RuntimeError):
            pass
    if source_platform(url) == "ehentai":
        match = re.fullmatch(r"/uploader/([^/]+)/?", urlsplit(url).path)
        if match:
            return extract_authors({"uploader": unquote(match[1])}, url), "uploader-url"
    return [], ""


def eh_request(payload):
    request = Request("https://api.e-hentai.org/api.php", data=json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json", "User-Agent": "PixGrabber-author-migration/1"})
    for attempt in range(3):
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except (OSError, ValueError):
            if attempt == 2:
                return {}
            time.sleep(5)


def eh_batches(entries):
    galleries = {}
    pages = []
    for entry in entries:
        path = urlsplit(entry["url"]).path
        # Historical input sometimes contains a repeated URL or text after the
        # final slash. Match the leading gallery like gallery-dl itself does.
        gallery = re.match(r"^/g/(\d+)/([a-fA-F0-9]+)(?:/|$)", path)
        page = re.match(r"^/s/([a-fA-F0-9]+)/(\d+)-(\d+)(?:/|$)", path)
        if gallery:
            key = (int(gallery[1]), gallery[2])
            galleries.setdefault(key, []).append(entry)
        elif page:
            pages.append((entry, [int(page[2]), page[1], int(page[3])]))
    for start in range(0, len(pages), 25):
        batch = pages[start:start + 25]
        tokens = eh_request({"method": "gtoken", "pagelist": [p[1] for p in batch]}).get("tokenlist", [])
        for entry, page in batch:
            for token in tokens:
                if token.get("gid") == page[0] and token.get("token"):
                    galleries.setdefault((page[0], token["token"]), []).append(entry)
                    break
        time.sleep(5)
    keys = list(galleries)
    for start in range(0, len(keys), 25):
        batch = keys[start:start + 25]
        metadata = eh_request({"method": "gdata", "gidlist": batch, "namespace": 1}).get("gmetadata", [])
        result = []
        for key in batch:
            info = next((item for item in metadata if item.get("gid") == key[0] and not item.get("error")), {})
            for entry in galleries[key]:
                result.append((entry, extract_authors(info, entry["url"]), "ehentai-api"))
        yield result
        time.sleep(5)


def fetch_extractor_authors(entry):
    """Only the owner's stored session may be used; temporary jars are private."""
    url = entry["url"]
    platform = video_platform(url)
    owner = owner_chat(entry)
    with tempfile.TemporaryDirectory(prefix="pix-author-session-") as private:
        if platform:
            command = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-plugin-dirs",
                       "--no-cache-dir", "--no-warnings", "--skip-download", "--dump-single-json",
                       "--no-playlist", "--playlist-end", "10", "--socket-timeout", "20", "--retries", "0"]
            if platform == "youtube" and shutil.which("deno"):
                command += ["--js-runtimes", "deno:" + shutil.which("deno")]
            if owner:
                from video_auth import VideoAuth, VideoAuthError
                try:
                    cookies = VideoAuth().read(int(owner), platform)
                except (ValueError, VideoAuthError):
                    cookies = None
                if cookies:
                    jar = Path(private) / "cookies.txt"
                    jar.write_bytes(cookies)
                    jar.chmod(0o600)
                    command += ["--cookies", str(jar)]
            command += ["--", url]
        else:
            command = [sys.executable, "-m", "gallery_dl", "--ignore-config", "-j", "--range", "1",
                       "--retries", "0", "--sleep-request", "1", "-o", "timeout=20"]
            if source_platform(url) == "rule34":
                command += ["-o", "extractor.rule34.tags=true"]
            if source_platform(url) == "pixiv" and owner:
                from pixiv_auth import PixivAuth
                auth = PixivAuth()
                if auth.has_token(int(owner)):
                    command += ["--config", str(auth.token_path(int(owner)).resolve())]
            command += [url]
        try:
            # Bound parsing memory; discard errors that can contain signed media URLs.
            with (Path(private) / "metadata.json").open("w+b") as output:
                process = subprocess.Popen(command, stdout=output, stderr=subprocess.DEVNULL,
                                           start_new_session=(os.name == "posix"))
                try:
                    process.wait(timeout=75)
                except subprocess.TimeoutExpired:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait()
                    return [], "extractor-unavailable"
                if process.returncode:
                    return [], "extractor-unavailable"
                output.seek(0)
                data = json.loads(output.read(8 * 1024 ** 2))
            # yt-dlp playlists may contain multiple posting accounts.
            authors = metadata_authors(data, url)
            if isinstance(data, dict):
                authors += metadata_authors(data.get("entries", []), url)
            return authors, "yt-dlp-metadata" if platform else "gallery-dl-metadata"
        except (OSError, ValueError, subprocess.TimeoutExpired):
            return [], "extractor-unavailable"


def migrate(*, online=False, retry_unavailable=False, archive_dir=Path("arhive"), report=None):
    db.init_db()
    entries = db.load_history()
    pending = [e for e in entries if not e["authors"] and (retry_unavailable or e["author_status"] == "pending")]
    sidecar_errors = 0

    def save(results):
        nonlocal sidecar_errors
        records = [{"id": e["_id"], "authors": authors, "source": source}
                   for e, authors, source in results if authors]
        if records:
            db.set_author_batch(records)
        for entry, authors, source in results:
            if not authors:
                continue
            entry["authors"] = authors
            try:
                write_author_sidecar(archive_dir / "metadata" / f"download-{entry['_id']}.authors.json",
                                     entry["url"], db.get_download_authors(entry["_id"]))
                if entry.get("media_kind") == "video" and Path(entry["download_dir"]).is_dir():
                    write_author_sidecar(Path(entry["download_dir"]) / "authors.json", entry["url"], authors)
            except OSError:
                sidecar_errors += 1

    for start in range(0, len(pending), 100):
        save([(entry, *local_authors(entry, archive_dir)) for entry in pending[start:start + 100]])
        print(json.dumps({"phase": "local", "checked": min(start + 100, len(pending)),
                          "resolved": sum(bool(e["authors"]) for e in pending)}), flush=True)
    if online:
        ehentai = [e for e in pending if not e["authors"] and source_platform(e["url"]) == "ehentai"]
        for results in eh_batches(ehentai):
            save(results)
            print(json.dumps({"phase": "api", "resolved": sum(bool(e["authors"]) for e in pending)}), flush=True)
        remaining = [e for e in pending if not e["authors"] and source_platform(e["url"]) != "ehentai"]
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(fetch_extractor_authors, e): e for e in remaining}
            for future in as_completed(futures):
                save([(futures[future], *future.result())])
                print(json.dumps({"phase": "extractor", "resolved": sum(bool(e["authors"]) for e in pending)}), flush=True)
        for entry in pending:
            if not entry["authors"]:
                db.mark_author_check(entry["_id"], "metadata_unavailable")
    with db.get_db() as conn:
        statuses = dict(conn.execute("SELECT author_status,COUNT(*) FROM downloads GROUP BY author_status").fetchall())
        roles = dict(conn.execute("SELECT role,COUNT(DISTINCT download_id) FROM download_authors GROUP BY role").fetchall())
        author_count = conn.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
        integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
    result = {"downloads": len(entries), "statuses": statuses, "downloads_by_role": roles,
              "unique_authors": author_count, "sidecar_errors": sidecar_errors, "integrity": integrity}
    if report:
        report.write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--online", action="store_true")
    parser.add_argument("--retry-unavailable", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    migrate(online=args.online, retry_unavailable=args.retry_unavailable, report=args.report)
