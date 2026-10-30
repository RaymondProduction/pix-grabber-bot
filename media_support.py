"""Pixiv animation conversion settings and download activity tracking."""

import asyncio
import json
from pathlib import Path


ANIMATION_EXTENSIONS = {".mp4"}

# Download one ZIP of frames, then convert it. Keep the source on failure.
PIXIV_POSTPROCESSORS = [{
    "name": "ugoira", "extension": "mp4", "mode": "concat",
    "keep-files": True, "skip": False, "metadata": False,
    "ffmpeg-args": [
        "-y", "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-threads", "2", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an",
    ],
}]


def download_snapshot(folder: Path):
    """Include .part files and growing FFmpeg output, not just finished photos."""
    snapshot = []
    for path in folder.rglob("*"):
        try:
            if path.is_file():
                stat = path.stat()
                snapshot.append((str(path), stat.st_size, stat.st_mtime_ns))
        except FileNotFoundError:
            pass  # gallery-dl may rename a completed .part file during this scan.
    return tuple(sorted(snapshot))


class DownloadActivity:
    def __init__(self, now: float):
        self.last_activity = now
        self.snapshot = ()

    def touch(self, now: float):
        self.last_activity = now

    def stalled(self, folder: Path, now: float, timeout: float) -> bool:
        snapshot = download_snapshot(folder)
        if snapshot != self.snapshot:
            self.snapshot = snapshot
            self.touch(now)
        return now - self.last_activity >= timeout


async def probe_animation(path: Path) -> dict:
    """Only publish completed, readable H.264 animations."""
    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-show_entries",
        "stream=codec_name,codec_type,width,height:format=duration", "-of", "json", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=30)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode:
        raise ValueError("Анімацію не вдалося прочитати")
    data = json.loads(stdout)
    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), {})
    if (video.get("codec_name") != "h264" or not video.get("width") or not video.get("height")
            or any(s.get("codec_type") == "audio" for s in streams)
            or float(data.get("format", {}).get("duration", 0)) <= 0):
        raise ValueError("Некоректний формат анімації MP4")
    return data
