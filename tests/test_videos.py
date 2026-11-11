import asyncio
import functools
import http.server
import importlib.util
import io
import json
import os
import shutil
import sqlite3
import sys
import threading
import tempfile
import unittest
import zipfile
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
from video_delivery import VideoDeliveryError
from test_pixiv_auth import RecordingSession

ROOT = Path(__file__).resolve().parents[1]
URLS = ["https://www.facebook.com/reel/123", "https://www.instagram.com/reel/abc/",
        "https://www.tiktok.com/@test/video/123",
        "https://youtube.com/shorts/pHofASeGcds?si=We57kcQUClqtbdMX"]


def cookies(platform="facebook", expiry="0"):
    names = {"facebook": ["c_user", "xs"], "instagram": ["sessionid"], "tiktok": ["sid_tt"],
             "youtube": ["SAPISID"]}[platform]
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

    def test_youtube_accepts_secure_session_and_filters_google_cookies(self):
        raw = cookies("youtube").replace(b"SAPISID", b"__Secure-3PAPISID")
        raw += b".google.com\tTRUE\t/\tTRUE\t0\tSAPISID\tprivate-google-cookie\n"
        self.auth.save(123, "youtube", raw)
        self.assertIn(b"__Secure-3PAPISID", self.auth.read(123, "youtube"))
        self.assertNotIn(b"google.com", self.auth.read(123, "youtube"))
        with self.assertRaises(auth.VideoAuthError):
            self.auth.save(123, "youtube", cookies("youtube").replace(b"SAPISID", b"VISITOR_INFO1_LIVE"))


class ErrorClassificationTests(unittest.TestCase):
    def test_ambiguous_site_failures_offer_login_without_claiming_it_is_required(self):
        for text in ("ERROR: [facebook] 123: Cannot parse data; please report this issue",
                     "HTTP Error 403: Forbidden", "This video is unavailable", "Unable to extract video data"):
            with self.subTest(error=text):
                error = video.classify_download_error(text)
                self.assertTrue(error.suggest_login)
                self.assertFalse(error.needs_login)
                self.assertFalse(error.not_video)

    def test_explicit_login_messages_and_unrelated_failures_are_distinguished(self):
        for text in ("You must be logged in", "Only available for registered users", "Login required",
                     "HTTP Error 401: Unauthorized", "Session has expired"):
            with self.subTest(error=text):
                error = video.classify_download_error(text)
                self.assertTrue(error.needs_login)
                self.assertTrue(error.suggest_login)
        for text in ("No module named yt_dlp", "No space left on device", "Read-only file system",
                     "Connection timed out", "Failed to resolve host", "File is larger than max-filesize",
                     "Unsupported URL: https://example.test", "There is no video in this post"):
            with self.subTest(error=text):
                self.assertFalse(video.classify_download_error(text).suggest_login)
        self.assertTrue(video.classify_download_error("There is no video in this post").not_video)


class RoutingStorageTests(unittest.TestCase):
    def test_platforms_short_links_and_spoofed_domains(self):
        for url, expected in zip(URLS, ("facebook", "instagram", "tiktok", "youtube")):
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

    def test_youtube_direct_urls_exclude_playlists_redirects_and_spoofed_hosts(self):
        for url in (URLS[3], "https://youtu.be/pHofASeGcds?si=share",
                    "https://m.youtube.com/watch?v=pHofASeGcds&list=playlist",
                    "https://www.youtube.com/live/pHofASeGcds",
                    "https://www.youtube-nocookie.com/embed/pHofASeGcds"):
            self.assertEqual(video.video_platform(url), "youtube")
        for url in ("https://youtube.com.evil.test/shorts/pHofASeGcds",
                    "https://youtube.com/playlist?list=playlist", "https://youtube.com/@channel",
                    "https://youtube.com/redirect?q=https://example.test", "https://youtu.be/",
                    "https://youtube.com/watch?v=invalid", "https://youtu.be:8080/pHofASeGcds"):
            self.assertIsNone(video.video_platform(url))

    def test_youtube_uses_explicit_deno_and_reports_missing_runtime_without_login_prompt(self):
        from yt_dlp import parse_options
        with patch.object(video.shutil, "which", return_value="/runtime/deno"):
            options = parse_options(video.download_command(URLS[3], Path("output"), None, 1024)[3:]).ydl_opts
        self.assertEqual(options["js_runtimes"]["deno"]["path"], "/runtime/deno")
        self.assertEqual(options["format_sort"], ["res:1080"])
        with patch.object(video.shutil, "which", return_value=None):
            with self.assertRaises(video.VideoDownloadError) as raised:
                video.download_command(URLS[3], Path("output"), None, 1024)
        self.assertIn("Deno", str(raised.exception))
        self.assertFalse(raised.exception.suggest_login)

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

    async def test_facebook_parse_failure_offers_cookies_without_exposing_extractor_output(self):
        source = "import sys; print('ERROR: [facebook] 123: Cannot parse data; https://private.test/signed-secret', file=sys.stderr); sys.exit(1)"
        with self.assertRaises(video.VideoDownloadError) as raised:
            await self.run_script(source)
        self.assertTrue(raised.exception.suggest_login)
        self.assertFalse(raised.exception.needs_login)
        self.assertNotIn("signed-secret", str(raised.exception))

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

    def video_entry(self, url=URLS[0], chat_id=123, status="interrupted", image_count=0):
        index = db.add_history_entry(url, "")
        folder = self.root / "images" / "videos" / f"existing-{index}"
        db.update_history_entry(index, media_kind="video", owner_chat_id=str(chat_id),
                                status=status, image_count=image_count, resume_url=url,
                                download_dir=str(folder))
        return index, folder

    async def completed_download(self, url, folder, cookie_data, **kwargs):
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "clip.mp4").write_bytes(b"synthetic-video-with-audio")
        video.save_video(folder, {"file": "clip.mp4", "title": "Clip <title>"})

    def large_video_entry(self):
        index, folder = self.video_entry(status="done", image_count=1)
        folder.mkdir(parents=True)
        (folder / "clip.mp4").write_bytes(os.urandom(11000))
        video.save_video(folder, {"file": "clip.mp4", "title": "Large <video>"})
        return index, folder

    async def fake_preview(self, source, folder):
        preview = folder / "preview.mp4"
        preview.write_bytes(b"small-preview")
        return preview

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
        self.assertEqual(self.message.answer_video.await_count, len(URLS))
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

    async def test_large_video_sends_preview_and_lossless_volumes_on_ssd(self):
        index, folder = self.large_video_entry()
        original = (folder / "clip.mp4").read_bytes()
        events = []

        async def uploaded(file, **kwargs):
            path = Path(file.path)
            events.append(path)
            return SimpleNamespace(chat=SimpleNamespace(id=123), message_id=len(events) + 100)

        self.message.answer_video.side_effect = uploaded
        self.message.answer_document.side_effect = uploaded
        with patch.object(self.module, "MAX_DOCUMENT_UPLOAD_SIZE", 4096), \
                patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 3000), \
                patch.object(self.module, "create_video_preview", side_effect=self.fake_preview):
            sent = await self.module.send_saved_videos(self.message, index, db.load_history()[index])
        self.assertEqual(sent, 1)
        self.assertEqual(events[0].name, "preview.mp4")
        parts = events[1:]
        self.assertEqual(len(parts), 4)
        self.assertTrue(all(part.stat().st_size <= 3000 for part in parts))
        self.assertTrue(all(self.ssd.resolve() in part.resolve().parents for part in parts))
        with zipfile.ZipFile(io.BytesIO(b"".join(part.read_bytes() for part in parts))) as archive:
            self.assertEqual(archive.read("clip.mp4"), original)
        entry = db.load_history()[index]
        self.assertEqual((entry["status"], entry["image_count"], entry["zip_parts"]), ("done", 1, []))
        self.assertEqual(len([ref for ref in entry["image_messages"] if ref["kind"] == "video_part"]), 4)
        self.assertEqual(entry["preview_message"]["message_id"], 101)
        self.assertEqual((folder / "clip.mp4").read_bytes(), original)

    async def test_failed_middle_part_retries_from_disk_without_losing_later_parts(self):
        index, folder = self.large_video_entry()
        failed_once = False

        async def uploaded(file, **kwargs):
            nonlocal failed_once
            number = int(Path(file.path).suffix[1:])
            if number == 2 and not failed_once:
                failed_once = True
                raise TelegramBadRequest(method=SendVideo(chat_id=123, video="test"), message="temporary failure")
            return SimpleNamespace(chat=SimpleNamespace(id=123), message_id=100 + number)

        self.message.answer_document.side_effect = uploaded
        with patch.object(self.module, "MAX_DOCUMENT_UPLOAD_SIZE", 4096), \
                patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 3000), \
                patch.object(self.module, "create_video_preview", side_effect=self.fake_preview) as preview:
            sent = await self.module.send_saved_videos(self.message, index, db.load_history()[index])
            self.assertEqual(sent, 0)
            entry = db.load_history()[index]
            refs = [ref for ref in entry["image_messages"] if ref["kind"] == "video_part"]
            self.assertEqual([Path(ref["file_name"]).name for ref in refs],
                             ["clip.mp4.zip.001", "clip.mp4.zip.003", "clip.mp4.zip.004"])
            self.assertEqual(entry["status"], "done")
            db.init_db()
            self.message.answer_document.reset_mock()
            with patch.object(self.module.bot, "forward_message", AsyncMock()) as forward:
                sent = await self.module.send_saved_videos(self.message, index, entry)
            self.assertEqual(sent, 1)
            self.assertEqual(forward.await_count, 4)  # Preview and three previously delivered parts.
            self.message.answer_document.assert_awaited_once()
            self.assertEqual(Path(self.message.answer_document.await_args.args[0].path).name, "clip.mp4.zip.002")
            self.assertEqual(preview.await_count, 1)
        self.assertTrue((folder / "clip.mp4").exists())

    async def test_preview_failure_does_not_prevent_original_delivery(self):
        index, _ = self.large_video_entry()
        with patch.object(self.module, "MAX_DOCUMENT_UPLOAD_SIZE", 4096), \
                patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 3000), \
                patch.object(self.module, "create_video_preview", side_effect=VideoDeliveryError("Preview failed")):
            self.assertEqual(await self.module.send_saved_videos(self.message, index, db.load_history()[index]), 1)
        self.message.answer_video.assert_not_awaited()
        self.assertEqual(self.message.answer_document.await_count, 4)

    async def test_document_button_sends_original_instead_of_forwarding_video(self):
        index, folder = await self.download()
        self.message.answer_video.reset_mock()
        callback = SimpleNamespace(data=f"video_document:{index}", message=self.message, answer=AsyncMock())
        with patch.object(self.module.bot, "forward_message", AsyncMock()) as forward:
            await self.module.video_document_callback(callback)
        forward.assert_not_awaited()
        self.message.answer_video.assert_not_awaited()
        self.message.answer_document.assert_awaited_once()
        self.assertEqual(Path(self.message.answer_document.await_args.args[0].path), folder / "clip.mp4")
        self.assertIn("video_document", {ref["kind"] for ref in db.load_history()[index]["image_messages"]})

    async def test_document_callback_dispatch_cannot_access_another_chat(self):
        index, _ = self.large_video_entry()
        self.module.bot.session = RecordingSession()
        user = User(id=456, is_bot=False, first_name="Other")
        message = Message(message_id=1, date=datetime.now(timezone.utc), chat=Chat(id=456, type="private"), from_user=user)
        update = Update(update_id=1, callback_query=CallbackQuery(
            id="test", from_user=user, chat_instance="test", message=message, data=f"video_document:{index}"))
        with patch.object(self.module, "send_saved_videos", AsyncMock()) as send:
            await self.module.dp.feed_update(self.module.bot, update)
        send.assert_not_awaited()

    async def test_telegram_size_rejection_falls_back_to_parts_even_below_configured_limit(self):
        index, _ = self.large_video_entry()
        error = TelegramEntityTooLarge(method=SendVideo(chat_id=123, video="test"), message="too large")
        self.message.answer_video.side_effect = error

        async def uploaded(file, **kwargs):
            if Path(file.path).suffix == ".mp4":
                raise error
            return SimpleNamespace(chat=SimpleNamespace(id=123), message_id=100)

        self.message.answer_document.side_effect = uploaded
        with patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 3000), \
                patch.object(self.module, "create_video_preview", side_effect=self.fake_preview):
            self.assertEqual(await self.module.send_saved_videos(self.message, index, db.load_history()[index]), 1)
        self.assertEqual(self.message.answer_document.await_count, 5)
        self.assertEqual(db.load_history()[index]["status"], "done")

    async def test_concurrent_resends_share_preparation_and_reuse_telegram_parts(self):
        index, _ = self.large_video_entry()
        entry = db.load_history()[index]
        with patch.object(self.module, "MAX_DOCUMENT_UPLOAD_SIZE", 4096), \
                patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", 3000), \
                patch.object(self.module, "create_video_preview", side_effect=self.fake_preview), \
                patch.object(self.module.bot, "forward_message", AsyncMock()) as forward:
            counts = await asyncio.gather(
                self.module.send_saved_videos(self.message, index, entry),
                self.module.send_saved_videos(self.message, index, entry))
        self.assertEqual(counts, [1, 1])
        self.assertEqual(self.message.answer_document.await_count, 4)
        self.assertEqual(forward.await_count, 5)

    async def test_changing_part_size_never_mixes_telegram_volume_generations(self):
        index, _ = self.large_video_entry()
        with patch.object(self.module, "MAX_DOCUMENT_UPLOAD_SIZE", 4096), \
                patch.object(self.module, "create_video_preview", side_effect=self.fake_preview), \
                patch.object(self.module.bot, "forward_message", AsyncMock()) as forward:
            for limit, expected_parts in ((3000, 4), (2000, 6), (3000, 4)):
                self.message.answer_document.reset_mock()
                with patch.object(self.module, "MAX_ARCHIVE_PART_SIZE", limit):
                    await self.module.send_saved_videos(self.message, index, db.load_history()[index])
                self.assertEqual(self.message.answer_document.await_count, expected_parts)
                refs = [ref for ref in db.load_history()[index]["image_messages"] if ref["kind"] == "video_part"]
                self.assertEqual(len(refs), expected_parts)
                self.assertEqual(len({Path(ref["file_name"]).parent for ref in refs}), 1)
        forward.assert_not_awaited()

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

    async def test_ambiguous_failure_provides_working_login_button_and_keeps_resume(self):
        index, _ = await self.download(failure=video.classify_download_error("Cannot parse data"))
        prompts = [call for call in self.message.answer.await_args_list if "/facebook_login" in call.args[0]]
        self.assertEqual(len(prompts), 1)
        self.assertIn("може допомогти", prompts[0].args[0])
        button = prompts[0].kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual((button.callback_data, button.text), ("video_login:facebook", "🔐 Підключити Facebook"))
        await self.module.video_login_callback(SimpleNamespace(
            data=button.callback_data, message=self.message, answer=AsyncMock()))
        self.assertEqual(self.module.video_auth.pending[123], "facebook")
        self.assertIn("Netscape", self.message.answer.await_args.args[0])
        entry = db.load_history()[index]
        self.assertEqual((entry["status"], entry["resume_url"]), ("interrupted", URLS[0]))
        keyboard = self.module.build_video_history_keyboard(index, entry)
        self.assertIn("video_login:facebook", [b.callback_data for row in keyboard.inline_keyboard for b in row])

    async def test_existing_session_offers_cookie_refresh_after_ambiguous_failure(self):
        self.module.video_auth.save(123, "facebook", cookies())
        await self.download(failure=video.classify_download_error("Cannot parse data"))
        prompt = next(call for call in self.message.answer.await_args_list if "/facebook_login" in call.args[0])
        self.assertEqual(prompt.kwargs["reply_markup"].inline_keyboard[0][0].text, "🔐 Оновити cookies Facebook")

    async def test_network_failure_does_not_request_cookie_import(self):
        await self.download(failure=video.classify_download_error("Connection timed out"))
        self.assertFalse(any("/facebook_login" in call.args[0] for call in self.message.answer.await_args_list))
        self.assertEqual(self.module.video_auth.pending, {})

    async def test_group_auth_suggestion_directs_user_to_private_chat(self):
        self.message.chat = SimpleNamespace(id=-100, type="group")
        await self.module.suggest_video_login(self.message, "facebook", required=False)
        self.assertIn("особистому чаті", self.message.answer.await_args.args[0])
        self.assertNotIn("reply_markup", self.message.answer.await_args.kwargs)
        self.assertEqual(self.module.video_auth.pending, {})

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

        facebook_index, folder = self.video_entry()
        instagram_index, _ = self.video_entry(URLS[1])
        await feed("/facebook_login")
        self.assertEqual(self.module.video_auth.pending[123], "facebook")

        async def download_file(document, destination):
            destination.write(cookies())

        with patch.object(self.module.bot, "download", AsyncMock(side_effect=download_file)):
            await feed(document=Document(file_id="test", file_unique_id="unique", file_name="cookies.txt",
                                         file_size=len(cookies())))
        self.assertEqual(self.module.video_auth.read(123, "facebook"), cookies())
        self.assertNotIn(123, self.module.video_auth.pending)
        queue = self.module.get_download_queue()
        self.assertEqual(queue.qsize(), 1)
        message, url, download_id, queued_folder = queue.get_nowait()
        self.assertEqual((message.chat.id, url, download_id, queued_folder),
                         (123, URLS[0], db.find_download_id_by_index(facebook_index), folder))
        self.assertEqual(db.load_history()[instagram_index]["status"], "interrupted")
        await feed("/tiktok_login")
        await feed("/cancel")
        self.assertNotIn(123, self.module.video_auth.pending)

    async def test_batch_retry_is_scoped_to_chat_and_site_and_preserves_history(self):
        first, first_folder = self.video_entry()
        second, second_folder = self.video_entry(URLS[0] + "?v=2", status="failed")
        other_site, _ = self.video_entry(URLS[1])
        other_chat, _ = self.video_entry(chat_id=456)
        done, _ = self.video_entry(URLS[0] + "?v=3", status="done")
        active, _ = self.video_entry(URLS[0] + "?v=4", status="in_progress")
        gallery = db.add_history_entry(URLS[0] + "?v=5", "images/gallery")
        db.update_history_entry(gallery, status="interrupted")
        self.module.video_auth.save(123, "facebook", cookies())
        self.assertEqual(self.module.enqueue_failed_videos(123, "facebook"), 2)
        self.assertEqual(self.module.enqueue_failed_videos(123, "facebook"), 0)
        queue = self.module.get_download_queue()
        self.assertEqual([queue.get_nowait()[2:] for _ in range(queue.qsize())], [
            (db.find_download_id_by_index(first), first_folder),
            (db.find_download_id_by_index(second), second_folder)])
        history = db.load_history()
        self.assertEqual(len(history), 7)
        for index in (other_site, other_chat, gallery):
            self.assertEqual(history[index]["status"], "interrupted")
        self.assertEqual(history[done]["status"], "done")
        self.assertEqual(history[active]["status"], "in_progress")

    async def test_batch_retry_deduplicates_urls_and_prefers_existing_progress(self):
        best, folder = self.video_entry(image_count=1)
        folder.mkdir(parents=True)
        partial = folder / "clip.mp4.part"
        partial.write_bytes(b"existing-fragments")
        duplicate, _ = self.video_entry(URLS[0] + "#same")
        completed_url = URLS[0] + "?v=completed"
        self.video_entry(completed_url)
        self.video_entry(completed_url, status="done")
        active_url = URLS[0] + "?v=active"
        self.video_entry(active_url)
        self.video_entry(active_url, status="in_progress")
        db.update_history_entry(best, auto_resume_at="2099-01-01 00:00:00", auto_resume_chat_id="123")
        self.module.video_auth.save(123, "facebook", cookies())
        self.assertEqual(self.module.enqueue_failed_videos(123, "facebook"), 1)
        self.assertEqual(self.module.get_download_queue().get_nowait()[2], db.find_download_id_by_index(best))
        self.assertEqual(partial.read_bytes(), b"existing-fragments")
        history = db.load_history()
        self.assertEqual(history[best]["image_count"], 1)
        self.assertEqual(history[best]["auto_resume_at"], "")
        self.assertEqual(history[duplicate]["status"], "interrupted")

    async def test_batch_retry_requires_valid_cookies_and_private_chat(self):
        index, _ = self.video_entry()
        for chat_id, platform in ((123, "facebook"), (-100, "facebook"), (123, "unknown")):
            with self.assertRaises(auth.VideoAuthError):
                self.module.enqueue_failed_videos(chat_id, platform)
        self.module.video_auth.save(123, "facebook", cookies())
        self.module.video_auth.path(123, "facebook").write_bytes(cookies(expiry="1"))
        with self.assertRaises(auth.VideoAuthError):
            self.module.enqueue_failed_videos(123, "facebook")
        self.assertEqual(self.module.get_download_queue().qsize(), 0)
        self.assertEqual(db.load_history()[index]["status"], "interrupted")

    async def test_video_retry_claim_checks_owner_state_and_media_type(self):
        index, folder = self.video_entry()
        download_id = db.find_download_id_by_index(index)
        self.assertFalse(db.claim_video_retry(download_id, 456, str(folder)))
        self.assertTrue(db.claim_video_retry(download_id, 123, str(folder)))
        self.assertFalse(db.claim_video_retry(download_id, 123, str(folder)))
        gallery = db.add_history_entry(URLS[0], "images/gallery")
        db.update_history_entry(gallery, status="interrupted", owner_chat_id="123")
        self.assertFalse(db.claim_video_retry(db.find_download_id_by_index(gallery), 123, "images/gallery"))

    async def test_individual_and_batch_retries_do_not_duplicate_a_queued_job(self):
        index, _ = self.video_entry()
        self.module.video_auth.save(123, "facebook", cookies())
        callback = SimpleNamespace(data=f"resume:{index}", message=self.message, answer=AsyncMock())
        await self.module.resume_download(callback)
        self.assertEqual(self.module.enqueue_failed_videos(123, "facebook"), 0)
        await self.module.resume_download(callback)
        self.assertEqual(self.module.get_download_queue().qsize(), 1)

    async def test_retry_callback_queues_before_failed_telegram_reply(self):
        index, _ = self.video_entry()
        self.module.video_auth.save(123, "facebook", cookies())
        callback = SimpleNamespace(data="video_retry:facebook", message=self.message,
                                   answer=AsyncMock(side_effect=RuntimeError("Telegram unavailable")))
        with self.assertRaisesRegex(RuntimeError, "Telegram unavailable"):
            await self.module.video_retry_callback(callback)
        self.assertEqual(self.module.get_download_queue().qsize(), 1)
        self.assertEqual(db.load_history()[index]["status"], "in_progress")

    async def test_cookie_import_queues_retries_even_when_confirmation_cannot_be_sent(self):
        index, _ = self.video_entry()
        self.module.video_auth.pending[123] = "facebook"
        self.message.document = SimpleNamespace(file_size=len(cookies()))
        self.message.answer.side_effect = RuntimeError("Telegram unavailable")

        async def download_file(document, destination):
            destination.write(cookies())

        with patch.object(self.module.bot, "download", AsyncMock(side_effect=download_file)):
            with self.assertRaisesRegex(RuntimeError, "Telegram unavailable"):
                await self.module.receive_video_cookies(self.message)
        self.assertEqual(self.module.get_download_queue().qsize(), 1)
        self.assertEqual(db.load_history()[index]["status"], "in_progress")
        self.assertEqual(self.module.video_auth.read(123, "facebook"), cookies())
        self.message.delete.assert_awaited_once()

    async def test_retry_button_dispatch_uses_only_the_current_chat(self):
        index, _ = self.video_entry()
        self.module.video_auth.save(456, "facebook", cookies())
        self.module.bot.session = RecordingSession()
        user = User(id=456, is_bot=False, first_name="Other")
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=456, type="private"), from_user=user)
        update = Update(update_id=1, callback_query=CallbackQuery(
            id="test", from_user=user, chat_instance="test", message=message, data="video_retry:facebook"))
        await self.module.dp.feed_update(self.module.bot, update)
        self.assertEqual(self.module.get_download_queue().qsize(), 0)
        self.assertEqual(db.load_history()[index]["status"], "interrupted")

    async def test_retry_without_old_directory_gets_a_stable_ssd_path(self):
        index, _ = self.video_entry()
        db.update_history_entry(index, download_dir="")
        self.module.video_auth.save(123, "facebook", cookies())
        self.assertEqual(self.module.enqueue_failed_videos(123, "facebook"), 1)
        _, _, download_id, folder = self.module.get_download_queue().get_nowait()
        self.assertEqual(folder, self.module.VIDEO_DOWNLOAD_DIR / "facebook" / "123" / f"retry_{download_id}")
        self.assertEqual(db.load_history()[index]["download_dir"], str(folder))

    async def test_legacy_youtube_recovery_preserves_id_owner_and_uses_ssd(self):
        index = db.add_history_entry(URLS[3], "images/old-youtube")
        download_id = db.find_download_id_by_index(index)
        db.update_history_entry(index, status="interrupted", resume_url=URLS[3],
                                auto_resume_chat_id="123", auto_resume_at="2099-01-01 00:00:00")
        self.module.recover_legacy_youtube_downloads()
        self.module.recover_legacy_youtube_downloads()
        entry = db.load_history()[index]
        self.assertEqual((entry["_id"], entry["media_kind"], entry["owner_chat_id"], entry["status"]),
                         (download_id, "video", "123", "interrupted"))
        self.assertEqual(entry["auto_resume_at"], "")
        self.assertIn(self.ssd.resolve(), Path(entry["download_dir"]).resolve().parents)
        self.assertFalse(self.module.can_access_entry(entry, 456))
        self.module.video_auth.save(123, "youtube", cookies("youtube"))
        self.assertEqual(self.module.enqueue_failed_videos(123, "youtube"), 1)
        self.assertEqual(self.module.get_download_queue().get_nowait()[2], download_id)

    async def test_legacy_recovery_skips_unknown_owner_completed_and_nonempty_galleries(self):
        for updates in ({}, {"image_count": 1}, {"status": "done"}, {"zip_parts": ["images/archive.zip"]}):
            index = db.add_history_entry(URLS[3], "images/old-youtube")
            values = {"status": "interrupted", "auto_resume_chat_id": "123" if updates else ""}
            db.update_history_entry(index, **(values | updates))
        gallery = db.add_history_entry(URLS[1], "images/instagram-photos")
        db.update_history_entry(gallery, status="interrupted", auto_resume_chat_id="123")
        self.module.recover_legacy_youtube_downloads()
        self.assertTrue(all(entry["media_kind"] == "gallery" for entry in db.load_history()))

    async def test_legacy_manual_resume_routes_youtube_to_video_storage(self):
        old_folder = self.root / "old-images-on-sd"
        index = db.add_history_entry(URLS[3], str(old_folder))
        with patch.object(self.module, "download_video", AsyncMock(side_effect=self.completed_download)), \
                patch.object(self.module.shutil, "which", return_value="/usr/bin/ffmpeg"), \
                patch.object(self.module, "run_gallery_download", AsyncMock()) as gallery:
            await self.module.run_download(self.message, URLS[3], index, old_folder)
        gallery.assert_not_awaited()
        entry = db.load_history()[index]
        self.assertEqual(entry["status"], "done")
        self.assertIn(self.ssd.resolve(), Path(entry["download_dir"]).resolve().parents)
        self.assertFalse(old_folder.exists())

    async def test_youtube_login_command_is_registered(self):
        self.module.bot.session = RecordingSession()
        user = User(id=123, is_bot=False, first_name="User")
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=123, type="private"), from_user=user, text="/youtube_login")
        await self.module.dp.feed_update(self.module.bot, Update(update_id=1, message=message))
        self.assertEqual(self.module.video_auth.pending[123], "youtube")

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
