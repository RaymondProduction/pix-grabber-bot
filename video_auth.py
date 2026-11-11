"""Per-chat browser sessions, using the Netscape import used by ai-chat."""

import os
import re
import tempfile
import time
from pathlib import Path

PLATFORMS = {"facebook": "Facebook", "instagram": "Instagram", "tiktok": "TikTok"}
COOKIE_LIMIT = 256 * 1024
LOGIN_COOKIES = {
    "facebook": ({"c_user", "xs"},),
    "instagram": ({"sessionid"},),
    "tiktok": ({"sessionid"}, {"sessionid_ss"}, {"sid_tt"}),
}


class VideoAuthError(ValueError):
    pass


def normalize_cookies(raw: bytes, platform: str) -> bytes:
    if platform not in PLATFORMS:
        raise VideoAuthError("Невідомий сервіс.")
    if len(raw) > COOKIE_LIMIT:
        raise VideoAuthError("Файл cookies має бути не більшим за 256 КіБ.")
    try:
        lines = raw.decode("utf-8-sig").splitlines()
    except UnicodeError:
        raise VideoAuthError("Потрібен текстовий файл cookies у форматі Netscape.") from None
    if not lines or lines[0] not in ("# Netscape HTTP Cookie File", "# HTTP Cookie File"):
        raise VideoAuthError("Потрібен файл cookies у форматі Netscape, не JSON.")
    kept, names = [], set()
    domain_root = platform + ".com"
    for line in lines[1:]:
        http_only = line.startswith("#HttpOnly_")
        if http_only:
            line = line[len("#HttpOnly_"):]
        elif not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != 7:
            raise VideoAuthError("Некоректний рядок у файлі cookies.")
        domain, subdomains, path, secure, expires, name, value = fields
        host = domain.lstrip(".").lower()
        if host != domain_root and not host.endswith("." + domain_root):
            continue
        if (not re.fullmatch(r"\.?[a-zA-Z0-9.-]+", domain)
                or not path.startswith("/") or subdomains not in ("TRUE", "FALSE")
                or secure not in ("TRUE", "FALSE") or not expires.isdigit()
                or not re.fullmatch(r"[A-Za-z0-9_-]+", name)
                or any(ord(c) < 32 or ord(c) == 127 for c in line if c != "\t")):
            raise VideoAuthError("Некоректний рядок у файлі cookies.")
        if int(expires) and int(expires) <= time.time():
            continue
        # Netscape's domain flag must agree with the leading dot (CookieJar requires it).
        fields[0] = ("." if subdomains == "TRUE" else "") + host
        fields[3] = "TRUE"
        kept.append(("#HttpOnly_" if http_only else "") + "\t".join(fields))
        if value and path == "/" and host == domain_root:
            names.add(name)
    if not any(required <= names for required in LOGIN_COOKIES[platform]):
        raise VideoAuthError(f"Не знайдено чинної сесії {PLATFORMS[platform]}. Увійди в браузері та експортуй cookies знову.")
    return ("# Netscape HTTP Cookie File\n" + "\n".join(kept) + "\n").encode()


class VideoAuth:
    def __init__(self, root: Path = Path(".video-auth")):
        self.root = root
        self.pending: dict[int, str] = {}

    def path(self, chat_id: int, platform: str) -> Path:
        if chat_id <= 0 or platform not in PLATFORMS:
            raise VideoAuthError("Підключення доступне лише в особистому чаті.")
        return self.root / f"{chat_id}-{platform}.txt"

    def save(self, chat_id: int, platform: str, raw: bytes):
        path = self.path(chat_id, platform)
        data = normalize_cookies(raw, platform)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        fd, name = tempfile.mkstemp(dir=self.root)
        try:
            with os.fdopen(fd, "wb") as file:
                file.write(data)
            os.replace(name, path)
        finally:
            Path(name).unlink(missing_ok=True)

    def read(self, chat_id: int, platform: str) -> bytes | None:
        if chat_id <= 0:  # Groups always download anonymously.
            return None
        path = self.path(chat_id, platform)
        if not path.exists():
            return None
        return normalize_cookies(path.read_bytes(), platform)

    def disconnect(self, chat_id: int, platform: str):
        self.path(chat_id, platform).unlink(missing_ok=True)

    def status(self, chat_id: int, platform: str) -> str:
        try:
            return "підключено" if self.read(chat_id, platform) else "не підключено"
        except VideoAuthError:
            return "потрібно оновити cookies"
