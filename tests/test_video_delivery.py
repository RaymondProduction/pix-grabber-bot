import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
import zipfile

import video_delivery as delivery


class VideoDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "video.mp4"
        self.source.write_bytes(os.urandom(32000))

    def assert_original_in_parts(self, parts, limit):
        self.assertTrue(all(0 < part.stat().st_size <= limit for part in parts))
        data = b"".join(part.read_bytes() for part in parts)
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            self.assertIsNone(archive.testzip())
            self.assertEqual(archive.namelist(), [self.source.name])
            self.assertEqual(archive.read(self.source.name), self.source.read_bytes())

    async def test_split_zip_reassembles_exactly_and_reuses_completed_volumes(self):
        original = self.source.read_bytes()
        parts, reused = await delivery.prepare_video_parts(self.source, 4096)
        self.assertFalse(reused)
        self.assert_original_in_parts(parts, 4096)
        times = [part.stat().st_mtime_ns for part in parts]
        again, reused = await delivery.prepare_video_parts(self.source, 4096)
        self.assertTrue(reused)
        self.assertEqual(again, parts)
        self.assertEqual([part.stat().st_mtime_ns for part in again], times)
        self.assertEqual(self.source.read_bytes(), original)

    async def test_missing_part_changed_source_and_changed_limit_rebuild_safely(self):
        parts, _ = await delivery.prepare_video_parts(self.source, 4096)
        parts[1].unlink()
        restored, reused = await delivery.prepare_video_parts(self.source, 4096)
        self.assertFalse(reused)
        self.assert_original_in_parts(restored, 4096)
        self.source.write_bytes(os.urandom(16000))
        changed, reused = await delivery.prepare_video_parts(self.source, 4096)
        self.assertFalse(reused)
        self.assertNotEqual(changed[0].parent, parts[0].parent)
        self.assert_original_in_parts(changed, 4096)
        smaller, reused = await delivery.prepare_video_parts(self.source, 2048)
        self.assertFalse(reused)
        self.assert_original_in_parts(smaller, 2048)

    async def test_insufficient_space_and_symlinks_preserve_the_original(self):
        original = self.source.read_bytes()
        with patch.object(delivery.shutil, "disk_usage", return_value=shutil._ntuple_diskusage(1, 1, 0)):
            with self.assertRaisesRegex(delivery.VideoDeliveryError, "бракує місця"):
                await delivery.prepare_video_parts(self.source, 4096)
        self.assertEqual(self.source.read_bytes(), original)
        link = self.root / "linked.mp4"
        link.symlink_to(self.source)
        with self.assertRaises(delivery.VideoDeliveryError):
            await delivery.prepare_video_parts(link, 4096)

    async def test_cancellation_joins_the_writer_and_does_not_publish_partial_archives(self):
        started, release = threading.Event(), threading.Event()
        original_write = delivery.SplitZipWriter.write

        def blocked_write(writer, data):
            started.set()
            release.wait(3)
            return original_write(writer, data)

        with patch.object(delivery.SplitZipWriter, "write", blocked_write):
            task = asyncio.create_task(delivery.prepare_video_parts(self.source, 4096))
            self.assertTrue(await asyncio.to_thread(started.wait, 3))
            task.cancel()
            await asyncio.sleep(0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(list(self.root.rglob("parts.json")))
        self.assertFalse(list(self.root.rglob("*.001")))
        self.assertFalse(list((self.root / ".delivery").glob(".building-*")))
        self.assertTrue(self.source.exists())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg required")
    async def test_real_preview_is_short_small_playable_and_keeps_original_and_audio(self):
        self.source.unlink()
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=blue:s=640x960:r=15",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100", "-t", "18",
            "-c:v", "libx264", "-preset", "ultrafast", "-threads", "2", "-pix_fmt", "yuv420p",
            "-c:a", "aac", str(self.source))
        self.assertEqual(await process.wait(), 0)
        digest = hashlib.sha256(self.source.read_bytes()).digest()
        folder = self.root / "preview"
        folder.mkdir()
        preview = await delivery.create_video_preview(self.source, folder)
        probe = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "stream=codec_type,width,height:format=duration",
            "-of", "json", str(preview), stdout=asyncio.subprocess.PIPE)
        output, _ = await probe.communicate()
        self.assertEqual(probe.returncode, 0)
        info = json.loads(output)
        self.assertLessEqual(float(info["format"]["duration"]), 15.2)
        self.assertEqual({stream["codec_type"] for stream in info["streams"]}, {"video", "audio"})
        visual = next(stream for stream in info["streams"] if stream["codec_type"] == "video")
        self.assertLessEqual(max(visual["width"], visual["height"]), 360)
        self.assertLess(preview.stat().st_size, 4 * 1024 ** 2)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).digest(), digest)
        modified = preview.stat().st_mtime_ns
        self.assertEqual(await delivery.create_video_preview(self.source, folder), preview)
        self.assertEqual(preview.stat().st_mtime_ns, modified)

    async def test_preview_timeout_kills_encoder_and_removes_unfinished_file(self):
        real_spawn = asyncio.create_subprocess_exec
        captured = {}

        async def stalled_encoder(*args, **kwargs):
            (self.root / "preview.tmp.mp4").write_bytes(b"incomplete")
            captured["process"] = await real_spawn(sys.executable, "-c", "import time; time.sleep(30)", **kwargs)
            return captured["process"]

        with patch.object(delivery.asyncio, "create_subprocess_exec", side_effect=stalled_encoder):
            with self.assertRaises(delivery.VideoDeliveryError):
                await delivery.create_video_preview(self.source, self.root, timeout=0.05)
        self.assertIsNotNone(captured["process"].returncode)
        self.assertFalse((self.root / "preview.tmp.mp4").exists())
        self.assertFalse((self.root / "preview.mp4").exists())


if __name__ == "__main__":
    unittest.main()
