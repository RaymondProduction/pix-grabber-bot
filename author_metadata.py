"""Normalize creator identities without confusing them with the posting account."""

import hashlib
import json
from pathlib import Path
import re
import unicodedata
from urllib.parse import quote, urlsplit, urlunsplit

AUTHOR_FIELDS = ["category", "subcategory", "id", "gid", "num", "user", "owner", "author",
                 "username", "user_id", "user_name", "account", "artist", "artists", "creator",
                 "uploader", "uploader_id", "uploader_url", "channel", "channel_id", "channel_url",
                 "tags", "tags_artist", "tag_string_artist"]
GALLERY_AUTHOR_POSTPROCESSOR = {
    "name": "metadata", "mode": "jsonl", "filename": "-", "event": ["prepare", "skip"],
    "include": AUTHOR_FIELDS, "separators": [",", ":"],
}
YTDLP_AUTHOR_FIELDS = "uploader,uploader_id,uploader_url,channel,channel_id,channel_url,creator"


def clean(value, limit=300):
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        return ""
    text = unicodedata.normalize("NFC", str(value))
    return " ".join("".join(c for c in text if c.isspace() or unicodedata.category(c) != "Cc").split())[:limit]


def public_url(value):
    if not isinstance(value, str):
        return ""
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
            return ""
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))[:2000]
    except ValueError:
        return ""


def source_platform(url: str) -> str:
    try:
        host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    except ValueError:
        return ""
    for root, platform in (("pixiv.net", "pixiv"), ("e-hentai.org", "ehentai"),
                           ("exhentai.org", "ehentai"), ("youtube.com", "youtube"),
                           ("youtu.be", "youtube"), ("youtube-nocookie.com", "youtube"),
                           ("facebook.com", "facebook"), ("fb.watch", "facebook"),
                           ("instagram.com", "instagram"), ("tiktok.com", "tiktok"),
                           ("rule34.xxx", "rule34")):
        if host == root or host.endswith("." + root):
            return platform
    return host


def author_record(platform, role, *, name="", external_id="", username="", profile_url="", source_url="", tag=False):
    name, external_id, username = clean(name), clean(external_id), clean(username).lstrip("@")
    profile_url = public_url(profile_url)
    if not (name or external_id or username):
        return None
    name = name or username or external_id
    if tag:
        identity = "artist-tag:" + name.casefold()
    elif external_id:
        identity = "id:" + external_id
    elif username:
        identity = "username:" + username.casefold()
    elif profile_url:
        identity = "url:" + profile_url
    else:
        # A display name alone must not merge unrelated accounts with the same name.
        identity = "unverified:" + hashlib.sha256(f"{role}\0{name}\0{source_url}".encode()).hexdigest()
    return {"platform": platform, "identity_key": identity, "external_id": external_id,
            "username": username, "name": name, "profile_url": profile_url, "role": role}


def extract_authors(metadata: dict, url: str) -> list[dict]:
    if not isinstance(metadata, dict):
        return []
    platform = source_platform(url)
    found = []

    def add(role, **fields):
        author = author_record(platform, role, source_url=url, **fields)
        if author:
            found.append(author)

    user = metadata.get("user") or metadata.get("owner") or metadata.get("author")
    if isinstance(user, dict):
        uid = user.get("id") or user.get("user_id") or user.get("uid")
        username = user.get("account") or user.get("username") or user.get("uniqueId") or user.get("unique_id")
        name = user.get("name") or user.get("full_name") or user.get("nickname") or user.get("nickName")
        profile = user.get("url") or ""
        if platform == "pixiv" and uid:
            profile = f"https://www.pixiv.net/users/{quote(str(uid))}"
        elif platform == "instagram" and username:
            profile = f"https://www.instagram.com/{quote(str(username))}/"
        elif platform == "tiktok" and username:
            profile = f"https://www.tiktok.com/@{quote(str(username))}"
        add("creator", name=name, external_id=uid, username=username, profile_url=profile)
    elif platform == "pixiv" and metadata.get("user_id"):
        uid = metadata["user_id"]
        add("creator", name=metadata.get("user_name"), external_id=uid, username=metadata.get("account"),
            profile_url=f"https://www.pixiv.net/users/{quote(str(uid))}")

    if platform in ("ehentai", "rule34"):
        artists = metadata.get("tags_artist") or metadata.get("tag_string_artist") or []
        if isinstance(artists, str):
            artists = artists.split()  # Booru tag names use underscores.
        if not isinstance(artists, list):
            artists = []
        artists = list(artists)
        tags = metadata.get("tags") or []
        if isinstance(tags, dict):
            extra = tags.get("artist", [])
            artists += [extra] if isinstance(extra, str) else list(extra)
        elif isinstance(tags, list):
            artists += [tag[7:] for tag in tags if isinstance(tag, str) and tag.startswith("artist:")]
        for artist in artists:
            if clean(artist).casefold() in ("unknown", "unknown_artist", "unknown artist"):
                continue
            profile = f"https://e-hentai.org/tag/artist:{quote(clean(artist))}" if platform == "ehentai" else ""
            add("artist", name=artist, profile_url=profile, tag=True)

    channel_id = metadata.get("channel_id") or metadata.get("uploader_id")
    channel = metadata.get("channel") or metadata.get("uploader")
    profile = metadata.get("channel_url") or metadata.get("uploader_url") or ""
    if isinstance(channel, dict):
        channel_id = channel.get("id") or channel_id
        profile = channel.get("url") or profile
        channel = channel.get("name") or channel.get("username")
    if channel or channel_id:
        username = ""
        if platform == "ehentai":
            username = clean(channel)
            profile = f"https://e-hentai.org/uploader/{quote(username)}"
        add("uploader", name=channel, external_id=channel_id, username=username, profile_url=profile)
    creator = metadata.get("creator")
    if creator and clean(creator) != clean(channel):
        add("creator", name=creator)
    if platform not in ("ehentai", "rule34"):
        artists = metadata.get("artists") or metadata.get("artist") or []
        for artist in ([artists] if isinstance(artists, str) else artists if isinstance(artists, list) else []):
            add("artist", name=artist)
    if not found and isinstance(user, str):
        add("uploader" if platform in ("ehentai", "rule34") else "creator", name=user)
    if not found and metadata.get("username"):
        add("creator", name=metadata.get("user_name"), username=metadata["username"],
            external_id=metadata.get("user_id"))
    return list({(a["platform"], a["identity_key"], a["role"]): a for a in found}.values())


def legacy_pixiv_authors(entry: dict) -> list[dict]:
    """gallery-dl's Pixiv directory is '{user[id]} {user[account]}', not the work title."""
    if source_platform(entry["url"]) != "pixiv":
        return []
    names = []
    folder = Path(entry.get("download_dir") or "__missing__") / "pixiv"
    if folder.is_dir():
        names.extend(child.name for child in folder.iterdir() if child.is_dir())
    names.append(entry.get("gallery_name", ""))
    found = []
    for name in names:
        match = re.fullmatch(r"([1-9][0-9]{0,12}) ([A-Za-z0-9_.-]+)", name)
        if match:
            uid, account = match.groups()
            found.extend(extract_authors({"user": {"id": uid, "account": account, "name": account}}, entry["url"]))
    return list({a["identity_key"]: a for a in found}.values())


def parse_gallery_authors(line: str, url: str):
    """None is an ordinary log line; a list consumes metadata even if no author exists."""
    if not line.startswith("{"):
        return None
    try:
        metadata = json.loads(line)
    except (ValueError, TypeError):
        return None
    if not isinstance(metadata, dict):
        return None
    return extract_authors(metadata, url)


def write_author_sidecar(path: Path, url: str, authors: list[dict]):
    """Only normalized identity fields: never persist cookies or extractor responses."""
    path.parent.mkdir(parents=True, exist_ok=True)
    allowed = {"platform", "identity_key", "external_id", "username", "name", "profile_url", "role", "source"}
    payload = {"version": 1, "source_url": url,
               "authors": [{k: v for k, v in author.items() if k in allowed} for author in authors]}
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
