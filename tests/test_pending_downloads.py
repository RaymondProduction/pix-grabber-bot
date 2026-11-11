import asyncio
import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import EditMessageText
from aiogram.types import CallbackQuery, Chat, Message, Update, User

import db
from test_pixiv_auth import CALLBACK, GALLERY, RecordingSession


ROOT = Path(__file__).resolve().parents[1]
URL = "https://example.com/gallery/1"


class PendingDownloadTests(unittest.IsolatedAsyncioTestCase):
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
        for name in ("HISTORY_DB_FILE", "HISTORY_FILE"):
            patcher = patch.object(db, name, self.root / getattr(db, name).name)
            patcher.start()
            self.addCleanup(patcher.stop)
        db.init_db()
        self.module = self.load_bot()
        self.message = SimpleNamespace(
            chat=SimpleNamespace(id=123, type="private"), text=URL,
            answer=AsyncMock(), edit_text=AsyncMock(),
        )

    def load_bot(self):
        spec = importlib.util.spec_from_file_location("pending_test_bot", ROOT / "bot.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.addAsyncCleanup(module.bot.session.close)
        module.queue_worker_started = True  # Inspect jobs without real downloads.
        return module

    def callback(self, token):
        return SimpleNamespace(data=f"mode:new:{token}", message=self.message, answer=AsyncMock())

    async def feed(self, text=None, callback_data=None):
        user = User(id=123, is_bot=False, first_name="Test")
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=123, type="private"), from_user=user, text=text)
        if callback_data:
            update = Update(update_id=1, callback_query=CallbackQuery(
                id="test", from_user=user, chat_instance="test", message=message, data=callback_data))
        else:
            update = Update(update_id=1, message=message)
        await self.module.dp.feed_update(self.module.bot, update)

    async def test_all_links_saved_before_slow_or_failed_reply(self):
        urls = [URL, "https://example.com/gallery/2"]
        self.message.text = "\n".join(urls + [URL])

        async def failed_reply(*args, **kwargs):
            self.assertEqual([r["url"] for r in db.list_pending_download_requests(123)], urls)
            raise RuntimeError("Telegram unavailable")

        self.message.answer.side_effect = failed_reply
        with self.assertRaises(RuntimeError):
            await self.module.on_link(self.message)
        self.assertEqual(len(db.list_pending_download_requests(123)), 2)
        self.assertEqual(db.load_history(), [])

    async def test_restart_keeps_links_and_original_buttons(self):
        await self.module.handle_url(self.message, URL)
        keyboard = self.message.answer.await_args.kwargs["reply_markup"]
        token = keyboard.inline_keyboard[0][0].callback_data.split(":", 2)[2]
        self.module = self.load_bot()
        db.init_db()
        await self.module.process_mode(self.callback(token))
        self.assertEqual(self.module.get_download_queue().qsize(), 1)
        self.assertEqual(db.list_pending_download_requests(123), [])
        self.assertEqual(db.load_history()[0]["url"], URL)

    async def test_batch_is_scoped_ordered_and_uses_unique_directories(self):
        urls = [URL, "https://example.com/gallery?id=1", "https://example.com/gallery?id=2"]
        for url in urls + [URL]:
            db.register_download_request(123, url)
        other = db.register_download_request(456, "https://example.com/private")
        await self.module.grab_pending_downloads(self.message)
        queue = self.module.get_download_queue()
        jobs = [queue.get_nowait() for _ in range(queue.qsize())]
        self.assertEqual([job[1] for job in jobs], urls)
        self.assertEqual(len({job[3] for job in jobs}), 3)
        self.assertEqual(db.list_pending_download_requests(123), [])
        self.assertIsNotNone(db.get_download_request(other, 456))
        await self.module.grab_pending_downloads(self.message)
        self.assertIn("Немає відкладених", self.message.answer.await_args.args[0])
        self.assertEqual(len(db.load_history()), 3)

    async def test_batch_and_individual_click_cannot_duplicate_jobs(self):
        first = db.register_download_request(123, URL)
        db.register_download_request(123, "https://example.com/gallery/2")
        await asyncio.gather(
            self.module.process_mode(self.callback(first)),
            self.module.grab_pending_downloads(self.message),
            self.module.process_mode(self.callback(first)),
            self.module.grab_pending_downloads(self.message),
        )
        self.assertEqual(self.module.get_download_queue().qsize(), 2)
        self.assertEqual(len(db.load_history()), 2)

    async def test_foreign_button_cannot_consume_another_chats_link(self):
        token = db.register_download_request(456, URL)
        callback = self.callback(token)
        await self.module.process_mode(callback)
        self.assertTrue(callback.answer.await_args.kwargs["show_alert"])
        self.assertIsNotNone(db.get_download_request(token, 456))
        self.assertEqual(db.load_history(), [])

    async def test_telegram_edit_failure_does_not_lose_queued_job(self):
        token = db.register_download_request(123, URL)
        self.message.edit_text.side_effect = TelegramBadRequest(
            method=EditMessageText(chat_id=123, message_id=1, text="test"), message="cannot edit")
        await self.module.process_mode(self.callback(token))
        self.assertEqual(self.module.get_download_queue().qsize(), 1)
        self.assertEqual(db.list_pending_download_requests(123), [])
        self.message.answer.assert_awaited_once()

    async def test_existing_downloads_and_explicit_redownloads_are_not_batched(self):
        for i, status in enumerate(("done", "in_progress", "interrupted")):
            url = f"https://example.com/gallery/{i}"
            db.register_download_request(123, url)
            index = db.add_history_entry(url, "images/test")
            db.update_history_entry(index, status=status,
                                    archive_messages=[{"chat_id": "123", "message_id": 11}])
        keyboard = self.module.build_redownload_keyboard("https://example.com/old", 123)
        await self.module.grab_pending_downloads(self.message)
        self.assertTrue(self.module.get_download_queue().empty())
        self.assertEqual(db.list_pending_download_requests(123), [])
        token = keyboard.inline_keyboard[0][0].callback_data.split(":", 2)[2]
        await self.module.process_mode(self.callback(token))
        self.assertEqual(self.module.get_download_queue().qsize(), 1)

    async def test_missing_pixiv_login_preserves_links_while_other_sites_start(self):
        pixiv_urls = [GALLERY, "https://www.pixiv.net/artworks/87654321"]
        for url in pixiv_urls + [URL]:
            db.register_download_request(123, url)
        await self.module.grab_pending_downloads(self.message)
        self.assertEqual(self.module.get_download_queue().qsize(), 1)
        self.assertEqual([r["url"] for r in db.list_pending_download_requests(123)], pixiv_urls)
        self.module.pixiv_auth.save_token(123, "synthetic-token")
        await self.module.grab_pending_downloads(self.message)
        self.assertEqual(self.module.get_download_queue().qsize(), 3)
        self.assertEqual(db.list_pending_download_requests(123), [])

    async def test_links_during_login_are_saved_without_restarting_login_or_saving_codes(self):
        self.module.bot.session = RecordingSession()
        await self.feed(GALLERY)
        session = self.module.pixiv_auth.sessions[123]
        await self.feed("https://www.pixiv.net/artworks/87654321")
        await self.feed(URL)
        self.assertIs(self.module.pixiv_auth.sessions[123], session)
        self.assertEqual(len(db.list_pending_download_requests(123)), 3)
        await self.feed("/grab_pending")
        self.assertEqual(self.module.get_download_queue().qsize(), 1)
        with patch.object(self.module.pixiv_auth, "finish", AsyncMock(return_value=None)):
            await self.feed(CALLBACK)
        self.assertEqual(len(db.list_pending_download_requests(123)), 2)
        self.assertFalse(any(CALLBACK in r["url"] for r in db.list_pending_download_requests(123)))

    async def test_menu_callback_routes_to_batch(self):
        self.module.bot.session = RecordingSession()
        db.register_download_request(123, URL)
        await self.feed(callback_data="grab_pending")
        self.assertEqual(self.module.get_download_queue().qsize(), 1)

    async def test_oauth_url_embedded_after_gallery_is_never_saved(self):
        self.message.text = URL + "\n" + CALLBACK
        await self.module.on_link(self.message)
        self.assertEqual([r["url"] for r in db.list_pending_download_requests(123)], [URL])

    async def test_worker_processes_batch_after_earlier_history_item_is_deleted(self):
        old = db.add_history_entry("https://example.com/old", "images/old")
        urls = [URL, "https://example.com/gallery/2"]
        for url in urls:
            db.register_download_request(123, url)
        await self.module.grab_pending_downloads(self.message)
        db.delete_history_entry(old)

        async def run(message, url, index, folder):
            self.assertEqual(db.load_history()[index]["url"], url)
            db.update_history_entry(index, status="done")

        with patch.object(self.module, "run_download", AsyncMock(side_effect=run)) as download:
            worker = asyncio.create_task(self.module.queue_worker())
            try:
                await asyncio.wait_for(self.module.get_download_queue().join(), timeout=2)
            finally:
                worker.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await worker
        self.assertEqual([call.args[1] for call in download.await_args_list], urls)
        self.assertTrue(all(r["status"] == "done" for r in db.load_history()))

    def test_failed_claim_rolls_back_and_leaves_link_pending(self):
        token = db.register_download_request(123, URL)
        with db.get_db() as conn:
            conn.execute("""CREATE TRIGGER fail_download BEFORE INSERT ON downloads
                            BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            db.claim_download_request(token, 123, "images/test")
        self.assertIsNotNone(db.get_download_request(token, 123))
        self.assertEqual(db.load_history(), [])

    def test_v4_upgrade_preserves_history_and_pending_links(self):
        db.add_history_entry(URL, "images/test")
        before = db.load_history()
        with db.get_db() as conn:
            conn.execute("DROP TABLE download_requests")
            conn.execute("DELETE FROM schema_version WHERE version=5")
            conn.execute("INSERT OR REPLACE INTO schema_version VALUES (4)")
        db.init_db()
        token = db.register_download_request(123, URL)
        db.init_db()
        self.assertEqual(db.load_history(), before)
        self.assertIsNotNone(db.get_download_request(token, 123))


if __name__ == "__main__":
    unittest.main()
