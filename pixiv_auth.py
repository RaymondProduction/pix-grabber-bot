"""Pixiv PKCE login and private, per-chat gallery-dl credentials."""

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp


LOGIN_URL = "https://app-api.pixiv.net/web/v1/login"
TOKEN_URL = "https://oauth.secure.pixiv.net/auth/token"
REDIRECT_URI = "https://app-api.pixiv.net/web/v1/users/auth/pixiv/callback"
SESSION_TTL = 10 * 60


class PixivAuthError(Exception):
    """A safe, Ukrainian error message; never include provider responses."""


@dataclass
class LoginSession:
    verifier: str = field(repr=False)
    created_at: float
    gallery_url: Optional[str] = None

    @property
    def login_url(self) -> str:
        challenge = base64.urlsafe_b64encode(
            hashlib.sha256(self.verifier.encode("ascii")).digest()
        ).rstrip(b"=").decode("ascii")
        return LOGIN_URL + "?" + urlencode({
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "client": "pixiv-android",
        })


def is_pixiv_url(url: str) -> bool:
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return False
    return host == "pixiv.net" or host.endswith(".pixiv.net")


def is_oauth_response(text: str) -> bool:
    """Keep stale callback URLs out of gallery downloads and history."""
    value = text.strip()
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    if parsed.scheme:
        return parsed.scheme == "pixiv" or (
            parsed.hostname == "app-api.pixiv.net"
            and parsed.path.startswith("/web/v1/")
        )
    return bool(re.search(r"(?:^|[?&])code=", value))


def parse_code(text: str) -> str:
    value = text.strip()
    if len(value) > 4096:
        raise PixivAuthError("Повідомлення завелике. Надішли лише код або адресу callback.")
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise PixivAuthError("Некоректна адреса. Надішли значення code або повну адресу callback.") from None
    if parsed.scheme or "?" in value or "=" in value:
        query = parsed.query if parsed.scheme or "?" in value else value.lstrip("?")
        codes = parse_qs(query, keep_blank_values=True).get("code", [])
        if len(codes) != 1:
            raise PixivAuthError("Не знайшов код. Надішли значення code або повну адресу callback із code=…")
        value = codes[0]
    if not re.fullmatch(r"[A-Za-z0-9._~-]{8,512}", value):
        raise PixivAuthError("Не схоже на код Pixiv. Скопіюй значення code з останнього запиту callback.")
    return value


def client_credentials() -> tuple[str, str]:
    # Use the same public mobile-client credentials as the installed downloader.
    try:
        from gallery_dl.extractor.pixiv import PixivAppAPI
        return PixivAppAPI.CLIENT_ID, PixivAppAPI.CLIENT_SECRET
    except (ImportError, AttributeError):
        raise PixivAuthError("Для входу потрібен gallery-dl у середовищі бота. Звернися до адміністратора.") from None


async def exchange_code(code: str, verifier: str) -> str:
    client_id, client_secret = client_credentials()
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "code_verifier": verifier,
        "grant_type": "authorization_code",
        "include_policy": "true",
        "redirect_uri": REDIRECT_URI,
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "PixivAndroidApp/5.0.234 (Android 11; Pixel 5)"},
        ) as client:
            async with client.post(TOKEN_URL, data=data, allow_redirects=False) as response:
                if response.status == 429:
                    raise PixivAuthError("Pixiv обмежив кількість спроб. Зачекай кілька хвилин і почни вхід знову.")
                if response.status == 403:
                    raise PixivAuthError("Pixiv відхилив запит із сервера бота. Перевір доступ сервера до Pixiv і спробуй пізніше.")
                if response.status >= 500:
                    raise PixivAuthError("Pixiv тимчасово недоступний. Спробуй увійти пізніше.")
                payload = await response.json()
                if not isinstance(payload, dict):
                    raise PixivAuthError("Pixiv повернув неочікувану відповідь. Почни вхід знову.")
                if payload.get("error") in ("invalid_grant", "invalid_request"):
                    raise PixivAuthError("Код прострочений або не належить цьому входу. Почни вхід знову й надішли новий код протягом 30 секунд після входу.")
                token = payload.get("refresh_token")
                if response.status != 200 or payload.get("error") or not isinstance(token, str) or not token:
                    raise PixivAuthError("Не вдалося підтвердити вхід у Pixiv. Почни вхід знову й використай новий код.")
                return token
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        raise PixivAuthError("Не вдалося отримати відповідь від Pixiv. Перевір з’єднання сервера й почни вхід знову.") from None


class PixivAuth:
    def __init__(self, directory: Path = Path(".pixiv-auth")):
        self.directory = directory
        self.sessions: dict[int, LoginSession] = {}
        self.pending_urls: dict[int, str] = {}
        self.exchanging: set[int] = set()

    def token_path(self, chat_id: int) -> Path:
        if chat_id <= 0:
            raise PixivAuthError("Підключення Pixiv доступне лише в особистому чаті з ботом.")
        return self.directory / f"{chat_id}.json"

    def has_token(self, chat_id: int) -> bool:
        try:
            data = json.loads(self.token_path(chat_id).read_text(encoding="utf-8"))
            token = data["refresh-token"]
            return isinstance(token, str) and bool(token)
        except (OSError, ValueError, KeyError, TypeError, PixivAuthError):
            return False

    def begin(self, chat_id: int, gallery_url: Optional[str] = None) -> LoginSession:
        self.token_path(chat_id)
        if chat_id in self.exchanging:
            raise PixivAuthError("Вхід уже перевіряється. Зачекай на результат.")
        previous = self.sessions.get(chat_id)
        now = time.monotonic()
        for key, session in list(self.sessions.items()):
            if now - session.created_at > SESSION_TTL:
                self.sessions.pop(key, None)
        pending_url = self.pending_urls.pop(chat_id, None)
        gallery_url = gallery_url or pending_url
        session = LoginSession(
            secrets.token_urlsafe(32), now,
            gallery_url or (previous.gallery_url if previous else None),
        )
        self.sessions[chat_id] = session
        return session

    def cancel(self, chat_id: int):
        if chat_id in self.exchanging:
            raise PixivAuthError("Код уже перевіряється. Зачекай на результат.")
        self.sessions.pop(chat_id, None)
        self.pending_urls.pop(chat_id, None)

    def save_token(self, chat_id: int, token: str):
        path = self.token_path(chat_id)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        # This file is passed only for this chat's Pixiv jobs. Top-level values
        # override inherited credentials; in-memory caching prevents gallery-dl
        # from reusing another chat's (or a previous account's) access token.
        data = {
            "refresh-token": token, "username": None, "password": None,
            "netrc": False, "cache": {"file": ":memory:"},
        }
        fd, temporary = tempfile.mkstemp(prefix=".token-", dir=self.directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(data, file)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    async def finish(self, chat_id: int, text: str) -> Optional[str]:
        if chat_id in self.exchanging:
            raise PixivAuthError("Код уже перевіряється. Зачекай на результат.")
        session = self.sessions.get(chat_id)
        if session is None or time.monotonic() - session.created_at > SESSION_TTL:
            self.sessions.pop(chat_id, None)
            if session and session.gallery_url:
                self.pending_urls[chat_id] = session.gallery_url
            raise PixivAuthError("Сесія входу закінчилася. Натисни «Підключити Pixiv» і відкрий нове посилання.")
        code = parse_code(text)
        # Consume before the first await: parallel messages cannot redeem twice.
        self.sessions.pop(chat_id)
        self.exchanging.add(chat_id)
        try:
            token = await exchange_code(code, session.verifier)
            try:
                self.save_token(chat_id, token)
            except OSError:
                raise PixivAuthError("Pixiv підтвердив вхід, але бот не зміг зберегти його. Звернися до адміністратора.") from None
            return session.gallery_url
        except PixivAuthError:
            if session.gallery_url:
                self.pending_urls[chat_id] = session.gallery_url
            raise
        finally:
            self.exchanging.discard(chat_id)
