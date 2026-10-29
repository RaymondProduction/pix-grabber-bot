import asyncio
import base64
import hashlib
import importlib.util
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import aiohttp
from aiogram.client.session.base import BaseSession
from aiogram.types import Chat, Message, Update, User

import pixiv_auth as auth


CODE = "synthetic-code-1234567890"
CALLBACK = auth.REDIRECT_URI + "?code=" + CODE + "&state=opaque-state"
GALLERY = "https://www.pixiv.net/en/artworks/12345678"
ROOT = Path(__file__).resolve().parents[1]


class ParsingTests(unittest.TestCase):
    def test_code_and_callback_variants(self):
        for text in (CODE, "  " + CODE + "\n", CALLBACK,
                     "state=opaque&code=" + CODE, "code=" + CODE,
                     "pixiv://account/login?code=" + CODE):
            with self.subTest(text=text):
                self.assertEqual(auth.parse_code(text), CODE)

    def test_invalid_input_is_rejected(self):
        for text in ("", "hello", "https://[", auth.LOGIN_URL + "?code_challenge=abc",
                     "code=", "code=one-code&code=two-code", "code=bad%0Acode", "x" * 4097):
            with self.subTest(text=text[:60]):
                with self.assertRaises(auth.PixivAuthError):
                    auth.parse_code(text)

    def test_host_matching_and_stale_callback_detection(self):
        self.assertTrue(auth.is_pixiv_url(GALLERY))
        self.assertFalse(auth.is_pixiv_url("https://pixiv.net.example.com/a"))
        self.assertFalse(auth.is_pixiv_url("https://example.com/pixiv.net"))
        self.assertFalse(auth.is_pixiv_url("https://["))
        self.assertTrue(auth.is_oauth_response(CALLBACK))
        self.assertTrue(auth.is_oauth_response("pixiv://account/login?code=" + CODE))
        self.assertFalse(auth.is_oauth_response(GALLERY))
        self.assertFalse(auth.is_oauth_response("https://example.com/gallery?code=gallery-id"))


class AuthTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.auth = auth.PixivAuth(Path(self.temporary.name) / "auth")

    def test_pkce_and_session_isolation(self):
        first = self.auth.begin(123, GALLERY)
        other = self.auth.begin(456)
        query = parse_qs(urlsplit(first.login_url).query)
        expected = base64.urlsafe_b64encode(hashlib.sha256(first.verifier.encode()).digest()).rstrip(b"=").decode()
        self.assertEqual(query["code_challenge"], [expected])
        self.assertEqual(query["code_challenge_method"], ["S256"])
        self.assertNotEqual(first.verifier, other.verifier)
        self.assertNotIn(first.verifier, repr(first))
        self.assertNotIn(first.verifier, first.login_url)
        new = self.auth.begin(123)
        self.assertNotEqual(first.verifier, new.verifier)
        self.assertEqual(new.gallery_url, GALLERY)

    def test_group_chat_cannot_store_or_start_login(self):
        self.assertFalse(self.auth.has_token(-100))
        with self.assertRaises(auth.PixivAuthError):
            self.auth.begin(-100)

    async def test_success_saves_private_config_and_consumes_session(self):
        session = self.auth.begin(123, GALLERY)
        with patch.object(auth, "exchange_code", AsyncMock(return_value="synthetic-refresh-token")) as exchange:
            self.assertEqual(await self.auth.finish(123, CALLBACK), GALLERY)
            exchange.assert_awaited_once_with(CODE, session.verifier)
            with self.assertRaises(auth.PixivAuthError):
                await self.auth.finish(123, CODE)
        self.assertTrue(self.auth.has_token(123))
        self.assertFalse(self.auth.has_token(456))
        path = self.auth.token_path(123)
        data = json.loads(path.read_text())
        self.assertEqual(data["cache"]["file"], ":memory:")
        self.assertIsNone(data["username"])
        self.assertIsNone(data["password"])
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertNotIn(CODE, path.read_text())
        self.assertNotIn(session.verifier, path.read_text())
        self.assertTrue(auth.PixivAuth(path.parent).has_token(123))

    async def test_bad_input_can_be_corrected_without_new_session(self):
        session = self.auth.begin(123)
        with self.assertRaises(auth.PixivAuthError):
            await self.auth.finish(123, "not a valid code")
        self.assertIs(self.auth.sessions[123], session)

    async def test_expired_session_keeps_gallery_for_retry(self):
        with patch.object(auth.time, "monotonic", return_value=10):
            self.auth.begin(123, GALLERY)
        with patch.object(auth.time, "monotonic", return_value=10 + auth.SESSION_TTL + 1):
            with self.assertRaises(auth.PixivAuthError):
                await self.auth.finish(123, CODE)
            self.assertEqual(self.auth.begin(123).gallery_url, GALLERY)

    async def test_failed_exchange_preserves_existing_token_and_pending_gallery(self):
        self.auth.save_token(123, "old-synthetic-token")
        self.auth.begin(123, GALLERY)
        with patch.object(auth, "exchange_code", AsyncMock(side_effect=auth.PixivAuthError("Спробуй знову."))):
            with self.assertRaises(auth.PixivAuthError):
                await self.auth.finish(123, CODE)
        self.assertEqual(json.loads(self.auth.token_path(123).read_text())["refresh-token"], "old-synthetic-token")
        self.assertEqual(self.auth.begin(123).gallery_url, GALLERY)

    async def test_parallel_code_and_restart_do_not_replace_inflight_session(self):
        self.auth.begin(123)
        ready, proceed = asyncio.Event(), asyncio.Event()

        async def exchange(*args):
            ready.set()
            await proceed.wait()
            return "synthetic-token"

        with patch.object(auth, "exchange_code", exchange):
            task = asyncio.create_task(self.auth.finish(123, CODE))
            await ready.wait()
            try:
                with self.assertRaises(auth.PixivAuthError):
                    await self.auth.finish(123, CODE)
                with self.assertRaises(auth.PixivAuthError):
                    self.auth.begin(123)
                with self.assertRaises(auth.PixivAuthError):
                    self.auth.cancel(123)
            finally:
                proceed.set()
                await task
        self.assertTrue(self.auth.has_token(123))
        self.assertNotIn(123, self.auth.exchanging)

    def test_failed_atomic_write_keeps_previous_token(self):
        self.auth.save_token(123, "previous-token")
        with patch.object(auth.os, "replace", side_effect=OSError("synthetic failure")):
            with self.assertRaises(OSError):
                self.auth.save_token(123, "replacement-token")
        self.assertEqual(json.loads(self.auth.token_path(123).read_text())["refresh-token"], "previous-token")
        self.assertEqual([p.name for p in self.auth.directory.iterdir()], ["123.json"])

    def test_cancel_keeps_existing_credentials(self):
        self.auth.save_token(123, "existing-token")
        self.auth.begin(123, GALLERY)
        self.auth.cancel(123)
        self.assertNotIn(123, self.auth.sessions)
        self.assertTrue(self.auth.has_token(123))

    def test_gallery_dl_reads_explicit_token_without_shared_cache(self):
        import requests
        from gallery_dl import config, extractor
        from gallery_dl.extractor.pixiv import PixivAppAPI

        self.auth.save_token(123, "private-synthetic-token")
        data = json.loads(self.auth.token_path(123).read_text())
        with patch.dict(config._config, data, clear=True):
            found = extractor.find(GALLERY)
            found.session = requests.Session()
            api = PixivAppAPI(found)
            self.assertEqual(api.refresh_token, "private-synthetic-token")
            self.assertIsNone(api.username)
            self.assertEqual(config.get(("cache",), "file"), ":memory:")
            found.session.close()


class ExchangeTests(unittest.IsolatedAsyncioTestCase):
    async def call_exchange(self, status, payload):
        response = MagicMock(status=status)
        response.json = AsyncMock(return_value=payload)
        post = MagicMock()
        post.__aenter__ = AsyncMock(return_value=response)
        post.__aexit__ = AsyncMock(return_value=None)
        client = MagicMock()
        client.post.return_value = post
        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=client)
        session.__aexit__ = AsyncMock(return_value=None)
        with patch.object(auth, "client_credentials", return_value=("test-client", "test-secret")), \
                patch.object(auth.aiohttp, "ClientSession", return_value=session):
            result = await auth.exchange_code(CODE, "synthetic-verifier")
        return result, client.post.call_args

    async def test_token_request_uses_original_verifier_and_disallows_redirects(self):
        token, args = await self.call_exchange(200, {"refresh_token": "synthetic-token"})
        self.assertEqual(token, "synthetic-token")
        self.assertEqual(args.args, (auth.TOKEN_URL,))
        self.assertFalse(args.kwargs["allow_redirects"])
        self.assertEqual(args.kwargs["data"]["code_verifier"], "synthetic-verifier")
        self.assertEqual(args.kwargs["data"]["code"], CODE)
        self.assertEqual(args.kwargs["data"]["redirect_uri"], auth.REDIRECT_URI)

    async def test_provider_errors_are_translated_without_echoing_secrets(self):
        for status, payload in [(400, {"error": "invalid_grant", "error_description": CODE}),
                                (403, {}), (429, {}), (500, {}), (200, {}),
                                (200, []), (302, {"refresh_token": CODE})]:
            with self.subTest(status=status, payload=payload):
                with self.assertRaises(auth.PixivAuthError) as caught:
                    await self.call_exchange(status, payload)
                self.assertNotIn(CODE, str(caught.exception))
                self.assertRegex(str(caught.exception), "[а-яіїєґА-ЯІЇЄҐ]")

    async def test_network_error_is_sanitized(self):
        with patch.object(auth, "client_credentials", return_value=("test", "test")), \
                patch.object(auth.aiohttp, "ClientSession", side_effect=aiohttp.ClientError(CODE)):
            with self.assertRaises(auth.PixivAuthError) as caught:
                await auth.exchange_code(CODE, "verifier")
        self.assertNotIn(CODE, str(caught.exception))


class RecordingSession(BaseSession):
    def __init__(self):
        super().__init__()
        self.calls = []

    async def close(self):
        pass

    async def make_request(self, bot, method, timeout=None):
        self.calls.append(method)
        if method.__api_method__ == "deleteMessage":
            return True
        return Message(message_id=900, date=datetime.now(timezone.utc),
                       chat=Chat(id=int(method.chat_id), type="private"),
                       text=getattr(method, "text", None))

    async def stream_content(self, *args, **kwargs):
        if False:
            yield b""


class BotFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        scratch = Path(self.temporary.name)
        (scratch / "config.json").write_text(json.dumps({
            "telegram_token": "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789", "sites": {},
        }))
        previous = Path.cwd()
        try:
            os.chdir(scratch)
            spec = importlib.util.spec_from_file_location("pixgrabber_test_bot", ROOT / "bot.py")
            self.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.module)
        finally:
            os.chdir(previous)
        self.module.pixiv_auth = auth.PixivAuth(scratch / "auth")
        self.session = RecordingSession()
        self.module.bot.session = self.session
        for name, value in (("load_history", []), ("find_done_entry_by_url", None)):
            mock = patch.object(self.module.db, name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)

    async def feed(self, text, chat_id=123, chat_type="private"):
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=chat_id, type=chat_type),
                          from_user=User(id=123, is_bot=False, first_name="Test"), text=text)
        await self.module.dp.feed_update(self.module.bot, Update(update_id=1, message=message))
        await asyncio.sleep(0)

    async def test_command_and_full_callback_use_auth_handler_before_url_handler(self):
        self.module.PENDING_SEARCH_REQUESTS.add(123)
        await self.feed("/pixiv_login")
        self.assertIn(123, self.module.pixiv_auth.sessions)
        self.assertNotIn(123, self.module.PENDING_SEARCH_REQUESTS)
        self.assertIn("Підключення Pixiv", self.session.calls[0].text)
        with patch.object(auth, "exchange_code", AsyncMock(return_value="synthetic-token")), \
                patch.object(self.module, "handle_url", AsyncMock()) as download:
            await self.feed(CALLBACK)
            download.assert_not_awaited()
        self.assertTrue(self.module.pixiv_auth.has_token(123))
        self.assertTrue(any(call.__api_method__ == "deleteMessage" for call in self.session.calls))
        self.assertFalse(any(CODE in str(getattr(call, "text", "")) for call in self.session.calls))

    async def test_authorization_returns_to_original_gallery(self):
        self.module.pixiv_auth.begin(123, GALLERY)
        with patch.object(auth, "exchange_code", AsyncMock(return_value="synthetic-token")), \
                patch.object(self.module, "handle_url", AsyncMock()) as download:
            await self.feed(CALLBACK)
            self.assertEqual(download.await_args.args[1], GALLERY)

    async def test_stale_callback_never_starts_download(self):
        with patch.object(self.module, "handle_url", AsyncMock()) as download:
            for text in (CALLBACK, "code=" + CODE, "pixiv://account/login?code=" + CODE):
                await self.feed(text)
            download.assert_not_awaited()
        self.assertTrue(any("немає активного входу" in (getattr(call, "text", "") or "") for call in self.session.calls))

    async def test_login_is_rejected_in_group(self):
        await self.feed("/pixiv_login", -123, "group")
        self.assertFalse(self.module.pixiv_auth.sessions)
        self.assertIn("особистому чаті", self.session.calls[0].text)

    async def test_cancel_command(self):
        self.module.pixiv_auth.begin(123)
        await self.feed("/cancel")
        self.assertFalse(self.module.pixiv_auth.sessions)
        self.assertIn("скасовано", self.session.calls[0].text)

    async def test_interrupted_download_without_resume_can_be_retried(self):
        self.module.pixiv_auth.save_token(123, "synthetic-token")
        entry = {"url": GALLERY, "status": "interrupted", "resume_url": ""}
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=123, type="private")).as_(self.module.bot)
        with patch.object(self.module.db, "load_history", return_value=[entry]):
            await self.module.handle_url(message, GALLERY)
        self.assertIn("наново", self.session.calls[0].text)
        self.assertTrue(self.session.calls[0].reply_markup.inline_keyboard[0][0].callback_data.startswith("mode:new:"))

    async def test_download_uses_only_requesting_chats_credentials(self):
        self.module.pixiv_auth.save_token(123, "synthetic-token")
        self.module.CONFIG["sites"] = {"pixiv.net": {"username": "placeholder-user", "password": "placeholder-pass"}}
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=123, type="private")).as_(self.module.bot)
        with patch.object(self.module.asyncio, "create_subprocess_exec", AsyncMock(side_effect=RuntimeError("stop before launch"))) as launch:
            with self.assertRaises(RuntimeError):
                await self.module.run_download(message, GALLERY, 0, Path(self.temporary.name) / "download")
        args = launch.await_args.args
        self.assertEqual(args[:3], (self.module.sys.executable, "-m", "gallery_dl"))
        self.assertEqual(args[args.index("--config") + 1], str(self.module.pixiv_auth.token_path(123).resolve()))
        self.assertNotIn("--username", args)
        self.assertNotIn("--password", args)
        self.assertNotIn("synthetic-token", args)

    async def test_queued_job_without_token_requests_login_without_starting_process(self):
        message = Message(message_id=1, date=datetime.now(timezone.utc),
                          chat=Chat(id=123, type="private")).as_(self.module.bot)
        with patch.object(self.module.asyncio, "create_subprocess_exec", AsyncMock()) as launch, \
                patch.object(self.module.db, "update_history_entry") as update:
            await self.module.run_download(message, GALLERY, 0, Path(self.temporary.name) / "download")
            launch.assert_not_awaited()
            self.assertEqual(update.call_args.kwargs["status"], "interrupted")
        self.assertEqual(self.module.pixiv_auth.pending_urls[123], GALLERY)


if __name__ == "__main__":
    unittest.main()
