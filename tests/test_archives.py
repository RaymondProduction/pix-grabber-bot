import asyncio
import importlib.util
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramEntityTooLarge
from aiogram.methods import SendDocument

import db


ROOT = Path(__file__).resolve().parents[1]


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, previous)
        (self.root / "config.json").write_text(json.dumps({
            "telegram_token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789", "sites": {},
        }))
        spec = importlib.util.spec_from_file_location("archive_test_bot", ROOT / "bot.py")
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.addAsyncCleanup(self.module.bot.session.close)
        db_path = patch.object(db, "HISTORY_DB_FILE", self.root / "history.sqlite3")
        db_path.start()
        self.addCleanup(db_path.stop)
        db.init_db()
        self.folder = self.root / "images" / "download"
        self.folder.mkdir()
        self.index = db.add_history_entry("https://www.pixiv.net/en/artworks/123", str(self.folder))
        self.message = SimpleNamespace(
            chat=SimpleNamespace(id=123), answer=AsyncMock(),
            answer_photo=AsyncMock(return_value=SimpleNamespace(chat=SimpleNamespace(id=123), message_id=10)),
            answer_document=AsyncMock(return_value=SimpleNamespace(chat=SimpleNamespace(id=123), message_id=11)),
        )

    def images(self, count, size=800):
        files = []
        for i in range(count):
            path = self.folder / f"{i:04d}.jpg"
            path.write_bytes(os.urandom(size))
            files.append(path)
        return files

    def state(self, files):
        return {
            "processed_files": {str(p) for p in files}, "live_sent_files": set(),
            "pending_files": list(files), "pending_cleanup": [], "zip_paths": [],
            "image_count": 0, "prev_image_count": 0, "part_number": 0,
            "preview_sent": True, "gallery_name": "Gallery", "safe_name": "Gallery",
        }

    def assert_archives(self, paths, files, limit):
        contents = {}
        for path in paths:
            self.assertLessEqual(Path(path).stat().st_size, limit)
            with zipfile.ZipFile(path) as archive:
                self.assertIsNone(archive.testzip())
                for name in archive.namelist():
                    self.assertNotIn(name, contents)
                    contents[name] = archive.read(name)
        self.assertEqual(contents, {p.name: p.read_bytes() for p in files})

    async def test_large_batch_is_split_and_saved_before_upload(self):
        files = self.images(5)
        state = self.state(files)

        async def upload(*args, **kwargs):
            entry = db.load_history()[self.index]
            self.assertEqual(entry["image_count"], 5)
            self.assertEqual(len(entry["zip_parts"]), 3)
            return SimpleNamespace(chat=SimpleNamespace(id=123), message_id=11)

        self.message.answer_document.side_effect = upload
        with patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 2000):
            await self.module.flush_archive_part(self.folder, self.message, self.index, state, files, True)
        entry = db.load_history()[self.index]
        self.assert_archives(entry["zip_parts"], files, 2000)
        self.assertEqual(len(entry["archive_messages"]), 3)
        self.assertEqual(state["pending_files"], [])

    async def test_zip_overhead_cannot_exceed_limit(self):
        files = self.images(2)
        parts = list(self.module.create_sized_zip_parts(self.folder, files, "test", 1, 1650))
        self.assertEqual(len(parts), 2)
        self.assert_archives([p for p, _ in parts], files, 1650)

    async def test_oversized_single_image_is_preserved(self):
        files = self.images(1, 3000)
        state = self.state(files)
        with patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 2000):
            with self.assertRaisesRegex(ValueError, "завелике"):
                await self.module.flush_archive_part(self.folder, self.message, self.index, state, files)
        self.assertTrue(files[0].exists())
        self.assertEqual(state["pending_files"], files)
        self.assertEqual(db.load_history()[self.index]["zip_parts"], [])

    async def test_upload_failure_does_not_erase_downloaded_count_or_completion(self):
        files = self.images(5)
        state = self.state(files)
        self.message.answer_document.side_effect = TelegramEntityTooLarge(
            method=SendDocument(chat_id=123, document="synthetic-file"), message="Request Entity Too Large"
        )
        with patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 2000):
            await self.module.finalize_streaming_archives(self.folder, self.message, self.index, state)
        entry = db.load_history()[self.index]
        self.assertEqual((entry["status"], entry["image_count"]), ("done", 5))
        self.assertEqual(len(entry["zip_parts"]), 3)
        self.assertEqual(entry["archive_messages"], [])
        self.assertTrue(all(Path(p).exists() for p in entry["zip_parts"]))
        self.assertIn("не надіслано", self.message.answer.await_args.args[0])

    async def test_monitor_cancellation_keeps_every_prepared_part(self):
        files = self.images(5)
        state = self.state(files)
        self.message.answer_document.side_effect = asyncio.CancelledError
        with patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 2000):
            with self.assertRaises(asyncio.CancelledError):
                await self.module.flush_archive_part(self.folder, self.message, self.index, state, files)
        entry = db.load_history()[self.index]
        self.assertEqual(entry["image_count"], 5)
        self.assert_archives(entry["zip_parts"], files, 2000)
        await self.module.finalize_streaming_archives(self.folder, self.message, self.index, state)
        self.assertEqual(db.load_history()[self.index]["status"], "done")

    async def test_empty_download_has_terminal_status(self):
        await self.module.finalize_streaming_archives(self.folder, self.message, self.index, self.state([]))
        entry = db.load_history()[self.index]
        self.assertEqual((entry["status"], entry["image_count"]), ("failed", 0))
        self.assertNotEqual(entry["gallery_name"], "in_progress")

    async def test_resend_does_not_skip_parts_missing_from_telegram(self):
        files = self.images(5)
        parts = list(self.module.create_sized_zip_parts(self.folder, files, "test", 1, 2000))
        db.update_history_entry(self.index, status="done", image_count=5,
                                zip_parts=[str(p) for p, _ in parts],
                                archive_messages=[{"chat_id": "123", "message_id": 1}])
        with patch.object(self.module.bot, "forward_message", AsyncMock()) as forward:
            await self.module.send_existing_archive_from_history(self.message, self.index)
            forward.assert_not_awaited()
        self.assertEqual(self.message.answer_document.await_count, 3)
        self.assertEqual(len(db.load_history()[self.index]["archive_messages"]), 3)

    async def test_worker_failure_updates_status_and_continues_after_notification_error(self):
        second = db.add_history_entry("https://example.com/second", str(self.folder))
        queue = self.module.get_download_queue()
        for index in (self.index, second):
            await queue.put((self.message, "https://example.com/gallery", index, self.folder))
        self.message.answer.side_effect = RuntimeError("Telegram unavailable")
        with patch.object(self.module, "run_download", AsyncMock(side_effect=[RuntimeError("download failed"), None])) as run:
            worker = asyncio.create_task(self.module.queue_worker())
            try:
                await asyncio.wait_for(queue.join(), timeout=2)
            finally:
                worker.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await worker
        self.assertEqual(run.await_count, 2)
        self.assertEqual(db.load_history()[self.index]["status"], "interrupted")
        self.assertEqual(self.module.active_downloads, 0)

    async def test_error_status_uses_stable_id_and_does_not_overwrite_done(self):
        second = db.add_history_entry("https://example.com/second", str(self.folder))
        download_id = db.find_download_id_by_index(second)
        db.delete_history_entry(self.index)
        db.mark_download_interrupted(download_id)
        self.assertEqual(db.load_history()[0]["status"], "interrupted")
        db.update_history_entry(0, status="done", image_count=5)
        db.mark_download_interrupted(download_id)
        self.assertEqual(db.load_history()[0]["status"], "done")


if __name__ == "__main__":
    unittest.main()
