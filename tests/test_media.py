import asyncio
import io
import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from PIL import Image
from gallery_dl.postprocessor.ugoira import UgoiraPP

import db
import media_support as media
import test_archives


class ActivityTests(unittest.TestCase):
    def test_partial_download_and_encoding_output_are_activity(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            activity = media.DownloadActivity(0)
            self.assertFalse(activity.stalled(folder, 19, 20))
            partial = folder / "animation.zip.part"
            partial.write_bytes(b"first chunk")
            self.assertFalse(activity.stalled(folder, 25, 20))
            partial.write_bytes(b"another, larger chunk")
            self.assertFalse(activity.stalled(folder, 50, 20))
            output = folder / "animation.mp4"
            output.write_bytes(b"encoding")
            self.assertFalse(activity.stalled(folder, 75, 20))
            self.assertTrue(activity.stalled(folder, 96, 20))
            activity.touch(97)
            self.assertFalse(activity.stalled(folder, 100, 20))


class AnimationTests(unittest.IsolatedAsyncioTestCase):
    state = test_archives.ArchiveTests.state

    async def asyncSetUp(self):
        await test_archives.ArchiveTests.asyncSetUp(self)
        self.message.answer_animation = AsyncMock(return_value=SimpleNamespace(
            chat=SimpleNamespace(id=123), message_id=12))

    def animation(self):
        path = self.folder / "0000.mp4"
        path.write_bytes(b"synthetic-animation")
        return path

    async def test_animation_is_archived_counted_and_sent_as_animation(self):
        path = self.animation()
        state = self.state([])
        state["preview_sent"] = False
        await self.module.finalize_streaming_archives(self.folder, self.message, self.index, state)
        entry = db.load_history()[self.index]
        self.assertEqual((entry["status"], entry["image_count"]), ("done", 1))
        self.message.answer_animation.assert_awaited_once()
        self.message.answer_photo.assert_not_awaited()
        self.assertEqual(entry["image_messages"][0]["kind"], "animation")
        self.assertEqual(entry["preview_message"]["message_id"], 12)
        refs = self.module.get_image_refs_from_zip_parts([Path(p) for p in entry["zip_parts"]])
        self.assertEqual([r["image_name"] for r in refs], [path.name])
        with zipfile.ZipFile(entry["zip_parts"][0]) as archive:
            self.assertEqual(archive.read(path.name), b"synthetic-animation")

    async def test_monitor_excludes_mp4_and_partial_files(self):
        self.animation()
        (self.folder / "source.zip.part").write_bytes(b"partial")
        photo = self.folder / "0000.jpg"
        photo.write_bytes(b"photo")
        self.assertEqual(self.module.get_downloaded_images(self.folder, include_animations=False), [photo])
        self.assertEqual(len(self.module.get_downloaded_images(self.folder)), 2)

    async def test_animation_resend_and_preview_do_not_use_photo_api(self):
        path = self.animation()
        archive = self.root / "animation.zip"
        self.module.create_zip_file(archive, [path])
        refs = self.module.get_image_refs_from_zip_parts([archive])
        await self.module.send_selected_images_from_refs(refs, [0], self.message, self.index)
        await self.module.send_media_preview(self.message, path.read_bytes(), path.name, "Preview")
        self.assertEqual(self.message.answer_animation.await_count, 2)
        self.message.answer_photo.assert_not_awaited()
        self.assertEqual(db.load_history()[self.index]["image_messages"][0]["kind"], "animation")

    async def test_archive_resend_creates_animation_preview(self):
        path = self.animation()
        archive = self.root / "animation.zip"
        self.module.create_zip_file(archive, [path])
        db.update_history_entry(self.index, status="done", image_count=1, zip_parts=[str(archive)])
        await self.module.send_existing_archive_from_history(self.message, self.index)
        self.message.answer_animation.assert_awaited_once()
        self.message.answer_document.assert_awaited_once()
        self.message.answer_photo.assert_not_awaited()

    async def test_mixed_photo_and_animation_do_not_create_single_item_album(self):
        animation = self.animation()
        photo = self.folder / "0001.jpg"
        photo.write_bytes(b"synthetic-photo")
        archive = self.root / "mixed.zip"
        self.module.create_zip_file(archive, [photo, animation])
        refs = self.module.get_image_refs_from_zip_parts([archive])
        self.message.answer_media_group = AsyncMock()
        await self.module.send_selected_images_from_refs(refs, [0, 1], self.message)
        self.message.answer_photo.assert_awaited_once()
        self.message.answer_animation.assert_awaited_once()
        self.message.answer_media_group.assert_not_awaited()

    async def test_single_history_preview_uses_animation(self):
        path = self.animation()
        archive = self.root / "animation.zip"
        self.module.create_zip_file(archive, [path])
        db.update_history_entry(self.index, status="done", image_count=1, zip_parts=[str(archive)])
        self.message.answer_media_group = AsyncMock()
        await self.module.send_history_preview_images(self.message, [(0, db.load_history()[0])], 1)
        self.message.answer_animation.assert_awaited_once()
        self.message.answer_media_group.assert_not_awaited()

    async def test_conversion_failure_preserves_source_and_never_sends_partial_mp4(self):
        path = self.animation()
        source = self.folder / "0000.zip"
        source.write_bytes(b"original-frames")
        self.module.pixiv_auth.save_token(123, "synthetic-token")
        stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
        stdout.feed_eof()
        stderr.feed_data(b"[postprocessor.ugoira][error] Conversion failed\n")
        stderr.feed_eof()
        process = SimpleNamespace(returncode=0, wait=AsyncMock(return_value=0), stdout=stdout, stderr=stderr)
        with patch.object(self.module.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)), \
                patch.object(self.module, "probe_animation", AsyncMock(side_effect=ValueError("incomplete"))):
            await self.module.run_download(self.message, "https://www.pixiv.net/en/artworks/123", self.index, self.folder)
        entry = db.load_history()[self.index]
        self.assertEqual((entry["status"], entry["image_count"]), ("interrupted", 0))
        self.assertTrue(source.exists())
        self.assertTrue(path.with_suffix(".mp4.failed").exists())
        self.message.answer_animation.assert_not_awaited()
        self.message.answer_document.assert_not_awaited()

    async def test_failed_postprocessor_with_zero_exit_code_keeps_source(self):
        source = self.folder / "0000.zip"
        source.write_bytes(b"original-frames")
        self.module.pixiv_auth.save_token(123, "synthetic-token")
        stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
        stdout.feed_eof()
        stderr.feed_data(b"[postprocessor.ugoira][error] Unable to invoke FFmpeg\n")
        stderr.feed_eof()
        process = SimpleNamespace(returncode=0, wait=AsyncMock(return_value=0), stdout=stdout, stderr=stderr)
        with patch.object(self.module.asyncio, "create_subprocess_exec", AsyncMock(return_value=process)):
            await self.module.run_download(self.message, "https://www.pixiv.net/en/artworks/123", self.index, self.folder)
        self.assertEqual(db.load_history()[self.index]["status"], "interrupted")
        self.assertTrue(source.exists())
        self.message.answer_animation.assert_not_awaited()

    async def test_download_enables_zip_conversion_and_process_group(self):
        self.module.pixiv_auth.save_token(123, "synthetic-token")
        with patch.object(self.module.asyncio, "create_subprocess_exec", AsyncMock(side_effect=RuntimeError("before launch"))) as launch:
            with self.assertRaisesRegex(RuntimeError, "before launch"):
                await self.module.run_download(self.message, "https://www.pixiv.net/en/artworks/123", self.index, self.folder)
        args = launch.await_args.args
        self.assertIn("extractor.pixiv.ugoira=true", args)
        options = json.loads(next(a.split("=", 1)[1] for a in args if a.startswith("postprocessors=")))
        self.assertEqual(options[0]["extension"], "mp4")
        self.assertTrue(options[0]["keep-files"])
        self.assertTrue(launch.await_args.kwargs["start_new_session"])

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg required")
    async def test_real_ugoira_conversion_preserves_timing_and_source(self):
        source = self.folder / "animation.zip"
        frames = []
        with zipfile.ZipFile(source, "w") as archive:
            for i, delay in enumerate((100, 200, 300)):
                name = f"{i:06d}.png"
                data = io.BytesIO()
                Image.new("RGB", (32, 32), (i * 80, 0, 0)).save(data, format="PNG")
                archive.writestr(name, data.getvalue())
                frames.append({"file": name, "delay": delay})
        pathfmt = SimpleNamespace(
            kwdict={"_ugoira_frame_data": frames, "id": 123}, extension="zip",
            temppath=str(source), realpath=str(source), realdirectory=str(self.folder),
            filename=source.name, exists=lambda: False, set_mtime=lambda: None,
        )

        def set_extension(extension):
            pathfmt.extension = extension

        def build_path():
            pathfmt.realpath = str(source.with_suffix("." + pathfmt.extension))
            pathfmt.filename = Path(pathfmt.realpath).name

        pathfmt.set_extension = set_extension
        pathfmt.build_path = build_path
        processor = UgoiraPP(MagicMock(), media.PIXIV_POSTPROCESSORS[0])
        processor.prepare(pathfmt)
        await asyncio.to_thread(processor.convert_from_zip, pathfmt)
        result = await media.probe_animation(source.with_suffix(".mp4"))
        self.assertAlmostEqual(float(result["format"]["duration"]), 0.6, delta=0.15)
        self.assertTrue(source.exists())
        self.assertEqual(result["streams"][0]["codec_name"], "h264")

    @unittest.skipUnless(shutil.which("ffprobe"), "FFprobe required")
    async def test_incomplete_animation_is_rejected(self):
        with self.assertRaises(ValueError):
            await media.probe_animation(self.animation())


if __name__ == "__main__":
    unittest.main()
