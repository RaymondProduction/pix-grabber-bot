import asyncio
import functools
import http.server
import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import threading
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramBadRequest, TelegramEntityTooLarge
from aiogram.methods import SendVideo
from aiogram.types import CallbackQuery, Chat, Document, Message, Update, User
from yt_dlp.cookies import YoutubeDLCookieJar

import db
import video_auth as auth
import video_support as video
from test_pixiv_auth import RecordingSession

ROOT = Path(__file__).resolve().parents[1]
URLS = ["https://www.facebook.com/reel/123", "https://www.instagram.com/reel/abc/",
        "https://www.tiktok.com/@test/video/123"]


def cookies(platform="facebook", expiry="0"):
    names = {"facebook": ["c_user", "xs"], "instagram": ["sessionid"], "tiktok": ["sid_tt"]}[platform]
    return ("# Netscape HTTP Cookie File\n" + "".join(
        f".{platform}.com\tTRUE\t/\tTRUE\t{expiry}\t{name}\tsynthetic-{name}\n"
        for name in names)).encode()


class CookieTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.auth = auth.VideoAuth(Path(temporary.name) / "sessions")

    def test_each_platform_filters_other_cookies_and_is_cookiejar_compatible(self):
        for platform in auth.PLATFORMS:
            raw = cookies(platform) + b".other.com\tTRUE\t/\tTRUE\t0\tsecret\tother-secret\n"
            self.auth.save(123, platform, raw)
            path = self.auth.path(123, platform)
            jar = YoutubeDLCookieJar(str(path))
            jar.load()
            self.assertTrue(list(jar))
            self.assertNotIn(b"other-secret", path.read_bytes())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)

    def test_expired_malformed_and_wrong_site_sessions_rejected_without_overwrite(self):
        self.auth.save(123, "facebook", cookies())
        for raw in (cookies(expiry="1"), cookies("instagram"), b"{}", b"a" * (auth.COOKIE_LIMIT + 1),
                    cookies() + b"broken-row\n", cookies().replace(b"\tTRUE\t/", b"\tINVALID\t/")):
            with self.subTest(raw=raw[:30]):
                with self.assertRaises(auth.VideoAuthError):
                    self.auth.save(123, "facebook", raw)
                self.assertEqual(self.auth.read(123, "facebook"), cookies())

    def test_restart_chat_isolation_disconnect_and_anonymous_groups(self):
        self.auth.save(123, "facebook", cookies())
        other = auth.VideoAuth(self.auth.root)
        self.assertEqual(other.read(123, "facebook"), cookies())
        self.assertIsNone(other.read(456, "facebook"))
        self.assertIsNone(other.read(-100, "facebook"))
        with self.assertRaises(auth.VideoAuthError):
            other.save(-100, "facebook", cookies())
        other.disconnect(123, "facebook")
        self.assertIsNone(self.auth.read(123, "facebook"))

    def test_http_only_bom_and_session_cookies(self):
        raw = b"\xef\xbb\xbf" + cookies().replace(b".facebook.com", b"#HttpOnly_.facebook.com")
        self.auth.save(123, "facebook", raw)
        jar = YoutubeDLCookieJar(str(self.auth.path(123, "facebook")))
        jar.load()
        self.assertEqual({c.name for c in jar}, {"c_user", "xs"})


class RoutingStorageTests(unittest.TestCase):
    def test_platforms_short_links_and_spoofed_domains(self):
        for url, expected in zip(URLS, ("facebook", "instagram", "tiktok")):
            self.assertEqual(video.video_platform(url), expected)
        for url in ("https://fb.watch/abc", "https://m.facebook.com/watch/?v=123",
                    "https://www.facebook.com/share/r/abc"):
            self.assertEqual(video.video_platform(url), "facebook")
        for url in ("https://vm.tiktok.com/abc", "https://vt.tiktok.com/abc"):
            self.assertEqual(video.video_platform(url), "tiktok")
        for url in ("https://facebook.com.evil.test/x", "https://evil.test/facebook.com",
                    "https://user:password@facebook.com/x", "https://[", "file:///facebook.com",
                    "https://facebook.com:999/x", "https://www.pixiv.net/artworks/123"):
            self.assertIsNone(video.video_platform(url))
        for url in ("https://www.facebook.com/photos/123", "https://www.facebook.com/photo.php?fbid=123",
                    "https://www.tiktok.com/@test/photo/123"):
            self.assertIsNone(video.video_platform(url))

    def test_symlink_storage_and_unavailable_ssd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ssd = root / "ssd" / "images"
            ssd.mkdir(parents=True)
            images = root / "images"
            images.symlink_to(ssd, target_is_directory=True)
            videos = video.video_root(images)
            video.ensure_video_storage(images, videos)
            (videos / "clip.mp4").write_bytes(b"video")
            self.assertEqual((ssd / "videos" / "clip.mp4").read_bytes(), b"video")
            shutil.rmtree(root / "ssd")
            with self.assertRaises(video.VideoDownloadError):
                video.ensure_video_storage(images, videos)
            self.assertFalse(ssd.exists())

    def test_command_parses_with_installed_ytdlp_and_keeps_audio(self):
        from yt_dlp import parse_options
        args = video.download_command(URLS[0], Path("output"), None, 1024)
        options = parse_options(args[3:]).ydl_opts
        self.assertIn("+ba", options["format"])
        self.assertTrue(options["noplaylist"])
        self.assertFalse(options["simulate"])
        self.assertIn("after_move", options["forceprint"])
        self.assertEqual(args[-2:], ["--", URLS[0]])


class DownloaderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name) / "videos"
        self.folder.mkdir()

    async def run_script(self, source, **kwargs):
        with patch.object(video, "download_command", return_value=[sys.executable, "-c", source]):
            await video.download_video(URLS[0], self.folder, None, **kwargs)

    async def test_only_after_move_files_are_saved_and_manifest_is_deduplicated(self):
        path = self.folder / "clip.mp4"
        path.write_bytes(b"finished-video")
        (self.folder / "unfinished.mp4").write_bytes(b"not-published")
        (self.folder / "clip.mp4.part").write_bytes(b"part")
        record = {"filepath": str(path), "title": "Title <with> & symbols", "duration": 2}
        source = f"print({json.dumps(json.dumps(record))})"
        await self.run_script(source)
        await self.run_script(source)
        self.assertEqual([x["file"] for x in video.saved_videos(self.folder)], ["clip.mp4"])
        self.assertFalse(list(self.folder.glob("*.zip")))

    async def test_partial_success_is_retained_and_extractor_secrets_are_not_exposed(self):
        path = self.folder / "clip.mp4"
        path.write_bytes(b"finished-video")
        record = {"filepath": str(path), "title": "clip"}
        source = (f"import sys; print({json.dumps(json.dumps(record))}); "
                  "print('login required cookie=synthetic-secret', file=sys.stderr); sys.exit(1)")
        with self.assertRaises(video.VideoDownloadError) as raised:
            await self.run_script(source)
        self.assertTrue(raised.exception.needs_login)
        self.assertNotIn("synthetic-secret", str(raised.exception))
        self.assertEqual(len(video.saved_videos(self.folder)), 1)

    async def test_timeout_stops_process_and_preserves_partial_file(self):
        partial = self.folder / "clip.mp4.part"
        partial.write_bytes(b"partial")
        with self.assertRaisesRegex(video.VideoDownloadError, "таймаут"):
            await self.run_script("import time; time.sleep(30)", timeout=0)
        self.assertTrue(partial.exists())
        self.assertEqual(video.saved_videos(self.folder), [])

    async def test_cookie_copy_is_private_and_removed_after_failure(self):
        captured = []

        def command(url, folder, cookie_path, max_bytes):
            captured.append(cookie_path)
            self.assertEqual(cookie_path.read_bytes(), cookies())
            self.assertEqual(cookie_path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn(self.folder, cookie_path.parents)
            return [sys.executable, "-c", "import sys; sys.exit(1)"]

        with patch.object(video, "download_command", side_effect=command):
            with self.assertRaises(video.VideoDownloadError):
                await video.download_video(URLS[0], self.folder, cookies())
        self.assertFalse(captured[0].exists())

    async def test_outside_files_and_symlinks_are_not_published(self):
        outside = self.folder.parent / "outside.mp4"
        outside.write_bytes(b"private")
        link = self.folder / "link.mp4"
        link.symlink_to(outside)
        records = [{"filepath": str(p), "title": "bad"} for p in (outside, link)]
        source = "\n".join(f"print({json.dumps(json.dumps(record))})" for record in records)
        with self.assertRaises(video.VideoDownloadError):
            await self.run_script(source)
        self.assertEqual(video.saved_videos(self.folder), [])

    async def test_cancellation_kills_process_and_removes_cookie_copy(self):
        launched = asyncio.Event()
        captured = {}
        original_launch = asyncio.create_subprocess_exec

        async def launch(*args, **kwargs):
            process = await original_launch(*args, **kwargs)
            captured["process"] = process
            launched.set()
            return process

        def command(url, folder, cookie_path, max_bytes):
            captured["cookie_path"] = cookie_path
            return [sys.executable, "-c", "import time; time.sleep(30)"]

        with patch.object(video, "download_command", side_effect=command), \
                patch.object(video.asyncio, "create_subprocess_exec", side_effect=launch):
            task = asyncio.create_task(video.download_video(URLS[0], self.folder, cookies()))
            await launched.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertIsNotNone(captured["process"].returncode)
        self.assertFalse(captured["cookie_path"].exists())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg required")
    async def test_real_ytdlp_local_mp4_keeps_audio_and_resumes_without_duplicates(self):
        source_dir = self.folder.parent / "source"
        source_dir.mkdir()
        source = source_dir / "fixture.mp4"
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=blue:s=64x64:r=10",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100", "-t", "0.4",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(source))
        self.assertEqual(await process.wait(), 0)

        class QuietHandler(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), functools.partial(QuietHandler, directory=str(source_dir)))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        original_command = video.download_command

        def command(url, folder, cookie_path, max_bytes):
            args = original_command(url, folder, cookie_path, max_bytes)
            args[-1] = f"http://127.0.0.1:{server.server_port}/fixture.mp4"
            return args

        try:
            with patch.object(video, "download_command", side_effect=command):
                await video.download_video(URLS[0], self.folder, None, timeout=30)
                await video.download_video(URLS[0], self.folder, None, timeout=30)
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join()
        items = video.saved_videos(self.folder)
        self.assertEqual(len(items), 1)
        downloaded = self.folder / items[0]["file"]
        self.assertEqual(downloaded.read_bytes(), source.read_bytes())
        probe = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "stream=codec_type", "-of", "json", str(downloaded),
            stdout=asyncio.subprocess.PIPE)
        output, _ = await probe.communicate()
        self.assertEqual({s["codec_type"] for s in json.loads(output)["streams"]}, {"video", "audio"})


class VideoBotTests(unittest.IsolatedAsyncioTestCase):
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
        # Model the production images -> SSD link, before importing bot.py.
        self.ssd = self.root / "ssd" / "images"
        self.ssd.mkdir(parents=True)
        (self.root / "images").symlink_to(self.ssd, target_is_directory=True)
        for name in ("HISTORY_DB_FILE", "HISTORY_FILE"):
            patcher = patch.object(db, name, self.root / getattr(db, name).name)
            patcher.start()
            self.addCleanup(patcher.stop)
        db.init_db()
        spec = importlib.util.spec_from_file_location("video_test_bot", ROOT / "bot.py")
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.addAsyncCleanup(self.module.bot.session.close)
        self.module.queue_worker_started = True
        sent = SimpleNamespace(chat=SimpleNamespace(id=123), message_id=10)
        self.message = SimpleNamespace(
            chat=SimpleNamespace(id=123, type="private"), from_user=SimpleNamespace(id=123),
            answer=AsyncMock(), answer_video=AsyncMock(return_value=sent),
            answer_document=AsyncMock(return_value=sent), answer_photo=AsyncMock(),
            answer_animation=AsyncMock(), delete=AsyncMock(),
        )

    def enqueue(self, url=URLS[0], chat_id=123):
        token = db.register_download_request(chat_id, url)
        message = self.message if chat_id == 123 else SimpleNamespace(chat=SimpleNamespace(id=chat_id))
        status, _ = self.module.enqueue_download_request(message, token)
        self.assertEqual(status, "queued")
        return self.module.get_download_queue().get_nowait()

    async def completed_download(self, url, folder, cookie_data, **kwargs):
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "clip.mp4").write_bytes(b"synthetic-video-with-audio")
        video.save_video(folder, {"file": "clip.mp4", "title": "Clip <title>"})

    async def download(self, url=URLS[0], failure=None):
        message, url, download_id, folder = self.enqueue(url)
        index = db.find_history_index_by_id(download_id)
        with patch.object(self.module, "download_video", AsyncMock(side_effect=failure or self.completed_download)), \
                patch.object(self.module.shutil, "which", return_value="/usr/bin/ffmpeg"):
            await self.module.run_download(message, url, index, folder)
        return index, folder

    async def test_all_platforms_use_ssd_keep_video_without_zip_and_record_history(self):
        for url in URLS:
            index, folder = await self.download(url)
            entry = db.load_history()[index]
            self.assertEqual((entry["media_kind"], entry["status"], entry["image_count"]), ("video", "done", 1))
            self.assertEqual(entry["owner_chat_id"], "123")
            self.assertEqual(entry["zip_parts"], [])
            self.assertEqual(entry["image_messages"][0]["kind"], "video")
            self.assertTrue((folder / "clip.mp4").is_file())
            self.assertIn(self.ssd.resolve(), folder.resolve().parents)
            self.module.cleanup_download_folder(folder)
            self.assertTrue((folder / "clip.mp4").exists())
        self.assertEqual(self.message.answer_video.await_count, 3)
        self.message.answer_document.assert_not_awaited()
        self.message.answer_animation.assert_not_awaited()
        self.assertFalse(list(self.ssd.rglob("*.zip")))

    async def test_resend_after_restart_uses_disk_when_telegram_message_gone(self):
        index, folder = await self.download()
        self.message.answer_video.reset_mock()
        db.init_db()
        error = TelegramBadRequest(method=SendVideo(chat_id=123, video="test"), message="not found")
        with patch.object(self.module.bot, "forward_message", AsyncMock(side_effect=error)):
            await self.module.send_existing_archive_from_history(self.message, index)
        self.message.answer_video.assert_awaited_once()
        self.assertTrue((folder / "clip.mp4").exists())

    async def test_upload_failure_and_oversized_files_remain_done_on_disk(self):
        error = TelegramEntityTooLarge(method=SendVideo(chat_id=123, video="test"), message="too large")
        self.message.answer_video.side_effect = error
        self.message.answer_document.side_effect = error
        index, folder = await self.download()
        self.assertEqual(db.load_history()[index]["status"], "done")
        self.assertEqual(db.load_history()[index]["image_messages"], [])
        self.message.answer_video.reset_mock()
        with patch.object(self.module, "MAX_VIDEO_UPLOAD_SIZE", 1):
            await self.module.send_existing_archive_from_history(self.message, index)
        self.message.answer_video.assert_not_awaited()
        self.assertTrue((folder / "clip.mp4").exists())

    async def test_auth_failure_keeps_resume_and_does_not_publish_partial_mp4(self):
        async def fail(url, folder, cookie_data, **kwargs):
            folder.mkdir(parents=True)
            (folder / "unfinished.mp4").write_bytes(b"not-finished")
            raise video.VideoDownloadError("Потрібен вхід", needs_login=True)

        index, folder = await self.download(failure=fail)
        entry = db.load_history()[index]
        self.assertEqual((entry["status"], entry["resume_url"], entry["image_count"]), ("interrupted", URLS[0], 0))
        self.message.answer_video.assert_not_awaited()
        self.assertTrue((folder / "unfinished.mp4").exists())
        self.assertTrue(any("/facebook_login" in call.args[0] for call in self.message.answer.await_args_list))

    async def test_instagram_photo_posts_still_use_gallery_downloader(self):
        with patch.object(self.module, "run_gallery_download", AsyncMock()) as gallery:
            index, _ = await self.download("https://www.instagram.com/p/abc/", failure=video.VideoDownloadError(
                "There is no video in this post", not_video=True))
        gallery.assert_awaited_once()
        entry = db.load_history()[index]
        self.assertEqual(entry["media_kind"], "gallery")
        self.assertEqual(Path(entry["download_dir"]).parent, self.module.BASE_DOWNLOAD_DIR)

    async def test_disconnect_during_download_does_not_send_authenticated_video(self):
        self.module.video_auth.save(123, "facebook", cookies())

        async def disconnect(url, folder, cookie_data, **kwargs):
            await self.completed_download(url, folder, cookie_data)
            self.module.video_auth.disconnect(123, "facebook")

        index, _ = await self.download(failure=disconnect)
        self.assertEqual(db.load_history()[index]["status"], "interrupted")
        self.message.answer_video.assert_not_awaited()

    async def test_cookies_are_only_passed_for_the_matching_chat_and_site(self):
        self.module.video_auth.save(123, "facebook", cookies())
        captured = []

        async def download(url, folder, cookie_data, **kwargs):
            captured.append(cookie_data)
            await self.completed_download(url, folder, cookie_data)

        await self.download(failure=download)
        await self.download(URLS[1], failure=download)
        self.assertEqual(captured, [cookies(), None])

    async def test_pending_dedup_and_privacy_keep_different_chats_separate(self):
        index, _ = await self.download()
        self.assertEqual(self.module.find_done_history_entry_by_url(URLS[0], 123), index)
        self.assertIsNone(self.module.find_done_history_entry_by_url(URLS[0], 456))
        self.assertEqual(self.module.visible_history(456), [])
        self.assertEqual(self.module.search_history_entries("facebook", 456), [])
        with patch.object(self.module.bot, "forward_message", AsyncMock()):
            await self.module.handle_url(self.message, URLS[0])
        self.assertEqual(len(db.load_history()), 1)
        self.enqueue(chat_id=456)
        self.assertEqual(len(db.load_history()), 2)
        self.assertEqual(db.dedup_history(), (0, 0))

    async def test_foreign_history_callback_is_blocked_by_dispatcher(self):
        index, _ = await self.download()
        user = User(id=456, is_bot=False, first_name="Other")
        message = Message(message_id=1, date=datetime.now(timezone.utc), chat=Chat(id=456, type="private"), from_user=user)
        self.module.bot.session = RecordingSession()
        with patch.object(self.module, "send_saved_videos", AsyncMock()) as send:
            for prefix in ("resend", "history_item", "resume", "get_url", "partial", "delete_history_item"):
                update = Update(update_id=1, callback_query=CallbackQuery(
                    id="test", from_user=user, chat_instance="test", message=message, data=f"{prefix}:{index}"))
                await self.module.dp.feed_update(self.module.bot, update)
            send.assert_not_awaited()
        self.assertEqual(len(db.load_history()), 1)

    async def test_history_buttons_keep_original_index_after_filtering(self):
        self.enqueue(chat_id=456)
        index, _ = await self.download()
        await self.module.send_history(self.message)
        keyboard = self.message.answer.await_args.kwargs["reply_markup"]
        items = [b.callback_data for row in keyboard.inline_keyboard for b in row
                 if b.callback_data.startswith("history_item:")]
        self.assertEqual(items, [f"history_item:{index}"])

    async def test_cookie_import_and_cancel_route_through_telegram_handlers(self):
        self.module.bot.session = RecordingSession()
        user = User(id=123, is_bot=False, first_name="Test")

        async def feed(text=None, document=None):
            message = Message(message_id=1, date=datetime.now(timezone.utc), chat=Chat(id=123, type="private"),
                              from_user=user, text=text, document=document)
            await self.module.dp.feed_update(self.module.bot, Update(update_id=1, message=message))

        await feed("/facebook_login")
        self.assertEqual(self.module.video_auth.pending[123], "facebook")

        async def download_file(document, destination):
            destination.write(cookies())

        with patch.object(self.module.bot, "download", AsyncMock(side_effect=download_file)):
            await feed(document=Document(file_id="test", file_unique_id="unique", file_name="cookies.txt",
                                         file_size=len(cookies())))
        self.assertEqual(self.module.video_auth.read(123, "facebook"), cookies())
        self.assertNotIn(123, self.module.video_auth.pending)
        await feed("/tiktok_login")
        await feed("/cancel")
        self.assertNotIn(123, self.module.video_auth.pending)

    async def test_schema_v5_upgrade_preserves_history(self):
        # Simulate the production schema before the new columns existed.
        with sqlite3.connect(db.HISTORY_DB_FILE) as conn:
            conn.execute("ALTER TABLE downloads DROP COLUMN media_kind")
            conn.execute("ALTER TABLE downloads DROP COLUMN owner_chat_id")
            conn.execute("DELETE FROM schema_version")
            conn.execute("INSERT INTO schema_version VALUES (5)")
        index = db.add_history_entry("https://example.test/gallery", "images/old")
        db.init_db()
        self.assertEqual(db.load_history()[index]["media_kind"], "gallery")
        self.assertEqual(db.load_history()[index]["owner_chat_id"], "")
        await self.download()
        self.assertEqual(len(db.load_history()), 2)

    async def test_restart_recovers_queued_videos_and_keeps_completed_downloads(self):
        done_index, _ = await self.download()
        _, url, download_id, folder = self.enqueue(URLS[1])
        gallery_index = db.add_history_entry("https://example.test/gallery", "images/gallery")
        db.recover_video_downloads()
        history = db.load_history()
        recovered = history[db.find_history_index_by_id(download_id)]
        self.assertEqual((recovered["status"], recovered["resume_url"]), ("interrupted", url))
        self.assertEqual(recovered["download_dir"], str(folder))
        self.assertEqual(history[done_index]["status"], "done")
        self.assertEqual(history[gallery_index]["status"], "in_progress")


if __name__ == "__main__":
    unittest.main()
