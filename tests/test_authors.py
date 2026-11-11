import asyncio
import contextlib
import io
import json
import logging
from pathlib import Path
import sqlite3
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from gallery_dl.postprocessor.metadata import MetadataPP
from types import SimpleNamespace

import author_metadata as meta
import db
import migrate_authors as migration
import video_support as video


class IdentityTests(unittest.TestCase):
    def test_stable_identity_keeps_name_account_and_platform_separate(self):
        author = meta.extract_authors({"user": {"id": 42, "account": "nick", "name": "Artist"}},
                                      "https://www.pixiv.net/artworks/123")[0]
        self.assertEqual((author["external_id"], author["username"], author["name"]), ("42", "nick", "Artist"))
        self.assertEqual(author["profile_url"], "https://www.pixiv.net/users/42")
        a = meta.author_record("facebook", "uploader", name="Same", source_url="post1")
        b = meta.author_record("facebook", "uploader", name="Same", source_url="post2")
        self.assertNotEqual(a["identity_key"], b["identity_key"])

    def test_multiple_artists_are_not_the_uploader_and_input_is_not_mutated(self):
        info = {"tags_artist": ["one"], "tags": ["artist:two", "artist:one"], "uploader": "poster"}
        authors = meta.extract_authors(info, "https://e-hentai.org/g/1/abc/")
        self.assertEqual(info["tags_artist"], ["one"])
        self.assertEqual([(a["name"], a["role"]) for a in authors],
                         [("one", "artist"), ("two", "artist"), ("poster", "uploader")])

    def test_public_profiles_strip_credentials_query_and_fragment(self):
        self.assertEqual(meta.public_url("https://example.test/user?a=secret#x"), "https://example.test/user")
        self.assertEqual(meta.public_url("https://user:password@example.test/user"), "")

    def test_legacy_pixiv_requires_extractor_directory_shape(self):
        entry = {"url": "https://www.pixiv.net/artworks/1", "gallery_name": "42 account"}
        self.assertEqual(meta.legacy_pixiv_authors(entry)[0]["external_id"], "42")
        for title in ("Drawing by Someone", "42 A work title", "in_progress"):
            entry["gallery_name"] = title
            self.assertEqual(meta.legacy_pixiv_authors(entry), [])

    def test_metadata_postprocessor_emits_only_selected_fields(self):
        output = io.StringIO()
        job = SimpleNamespace(pathfmt=None, register_hooks=lambda *args: None, get_logger=logging.getLogger)
        with patch("sys.stdout", output):
            pp = MetadataPP(job, meta.GALLERY_AUTHOR_POSTPROCESSOR)
            pp.run(SimpleNamespace(kwdict={"category": "pixiv", "user": {"id": 42, "account": "nick"}, "secret": "omit"}))
        line = output.getvalue().strip()
        self.assertNotIn("omit", line)
        self.assertEqual(meta.parse_gallery_authors(line, "https://pixiv.net/artworks/1")[0]["external_id"], "42")
        self.assertEqual(meta.parse_gallery_authors('{"category":"unknown"}', "https://example.test"), [])
        self.assertIsNone(meta.parse_gallery_authors("[log] ordinary", "https://example.test"))

    def test_owner_is_not_guessed_when_history_has_multiple_chats(self):
        entry = {"archive_messages": [{"chat_id": "1"}, {"chat_id": "2"}]}
        self.assertIsNone(migration.owner_chat(entry))
        entry["owner_chat_id"] = "3"
        self.assertEqual(migration.owner_chat(entry), "3")

    def test_metadata_probe_uses_only_owner_cookie_copy_and_never_downloads_media(self):
        from video_auth import VideoAuth
        launch = subprocess.Popen
        jars = []

        def process(command, **kwargs):
            self.assertIn("--skip-download", command)
            self.assertIn("--ignore-config", command)
            jar = Path(command[command.index("--cookies") + 1])
            self.assertEqual(jar.stat().st_mode & 0o777, 0o600)
            self.assertEqual(jar.read_bytes(), b"synthetic-cookies")
            jars.append(jar)
            return launch([sys.executable, "-c", 'print(\'{"uploader":"Poster","uploader_id":"42"}\')'], **kwargs)

        with patch.object(VideoAuth, "read", return_value=b"synthetic-cookies") as read, \
                patch.object(migration.subprocess, "Popen", side_effect=process):
            authors, source = migration.fetch_extractor_authors({
                "url": "https://facebook.com/reel/123", "owner_chat_id": "7"})
        read.assert_called_once_with(7, "facebook")
        self.assertEqual(authors[0]["external_id"], "42")
        self.assertEqual(source, "yt-dlp-metadata")
        self.assertFalse(jars[0].exists())

    def test_booru_probe_requests_typed_artist_tags(self):
        launch = subprocess.Popen

        def process(command, **kwargs):
            self.assertIn("extractor.rule34.tags=true", command)
            self.assertIn("-j", command)
            return launch([sys.executable, "-c", 'print(\'[[3,"file",{"tags_artist":"one two"}]]\')'], **kwargs)

        with patch.object(migration.subprocess, "Popen", side_effect=process):
            authors, _ = migration.fetch_extractor_authors({"url": "https://rule34.xxx/index.php?page=post&s=view&id=1"})
        self.assertEqual([a["name"] for a in authors], ["one", "two"])


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.patcher = patch.multiple(db, HISTORY_DB_FILE=self.root / "history.sqlite3", HISTORY_FILE=self.root / "absent.json")
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        db.init_db()

    def entry(self, url="https://pixiv.net/artworks/1"):
        index = db.add_history_entry(url, str(self.root / "absent"))
        return db.find_download_id_by_index(index)

    def test_schema6_upgrade_preserves_history_and_archive_messages(self):
        did = self.entry()
        db.append_zip_part(did, "stored.zip")
        db.append_archive_message(did, "123", 45)
        with db.get_db() as conn:
            conn.execute("DELETE FROM schema_version WHERE version=7")
            conn.execute("INSERT INTO schema_version VALUES (6)")
            for name in ("author_status", "author_checked_at", "author_error"):
                conn.execute("ALTER TABLE downloads DROP COLUMN " + name)
            conn.execute("DROP TABLE download_authors")
            conn.execute("DROP TABLE authors")
        db.init_db()
        entry = db.load_history()[0]
        self.assertEqual(entry["zip_parts"], ["stored.zip"])
        self.assertEqual(entry["archive_messages"][0]["message_id"], 45)
        self.assertEqual(entry["author_status"], "pending")
        self.assertEqual(entry["authors"], [])

    def test_repeated_updates_are_idempotent_and_do_not_downgrade_name(self):
        did = self.entry()
        author = meta.extract_authors({"user": {"id": 42, "account": "nick", "name": "Artist"}}, "https://pixiv.net/artworks/1")
        db.set_download_authors(did, author, "gallery-dl")
        author[0]["name"] = "nick"
        db.set_download_authors(did, author, "legacy")
        db.mark_author_check(did)
        entry = db.load_history()[0]
        self.assertEqual(entry["author"], "Artist")
        self.assertEqual(entry["author_status"], "resolved")
        self.assertEqual(len(entry["authors"]), 1)
        db.save_history([entry])
        self.assertEqual(db.load_history()[0]["author"], "Artist")

    def test_offline_migration_survives_removed_folders_and_is_repeatable(self):
        self.entry()
        db.update_history_entry(0, gallery_name="42 nick")
        other = self.entry("https://example.test/unavailable")
        with contextlib.redirect_stdout(io.StringIO()):
            result = migration.migrate(archive_dir=self.root / "ssd")
            again = migration.migrate(archive_dir=self.root / "ssd")
        self.assertEqual(result, again)
        self.assertEqual(result["statuses"], {"pending": 1, "resolved": 1})
        self.assertEqual(db.load_history()[0]["authors"][0]["external_id"], "42")
        self.assertEqual(len(list((self.root / "ssd" / "metadata").glob("*.json"))), 1)

    def test_online_migration_marks_unknown_and_does_not_overwrite_existing(self):
        known = self.entry()
        db.set_download_authors(known, meta.extract_authors({"user": {"id": 42}}, "https://pixiv.net/artworks/1"), "test")
        self.entry("https://example.test/missing")
        with patch.object(migration, "fetch_extractor_authors", return_value=([], "unavailable")) as fetch:
            with contextlib.redirect_stdout(io.StringIO()):
                result = migration.migrate(online=True, archive_dir=self.root / "ssd")
        fetch.assert_called_once()
        self.assertEqual(result["statuses"], {"resolved": 1, "unavailable": 1})

    def test_api_batch_resolves_page_urls_and_extracts_artist_namespace(self):
        entries = [{"url": "https://e-hentai.org/s/abc/42-1"}, {"url": "https://e-hentai.org/g/43/def/"}]
        replies = [{"tokenlist": [{"gid": 42, "token": "aaa"}]},
                   {"gmetadata": [{"gid": 42, "tags": ["artist:one"]}, {"gid": 43, "uploader": "two"}]}]
        with patch.object(migration, "eh_request", side_effect=replies) as request, patch.object(migration.time, "sleep"):
            results = list(migration.eh_batches(entries))[0]
        self.assertEqual(len(results), 2)
        self.assertEqual(request.call_args_list[0].args[0]["pagelist"], [[42, "abc", 1]])
        self.assertEqual({a["name"] for _, authors, _ in results for a in authors}, {"one", "two"})


class VideoAuthorTests(unittest.IsolatedAsyncioTestCase):
    async def test_author_is_retained_even_if_download_fails_after_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            info = {"uploader": "Poster", "uploader_id": "42", "cookies": "secret"}
            command = [sys.executable, "-c", f"import sys; print({json.dumps(json.dumps(info))}); sys.exit(1)"]
            authors = []
            with patch.object(video, "download_command", return_value=command):
                with self.assertRaises(video.VideoDownloadError):
                    await video.download_video("https://facebook.com/reel/123", folder, None, on_authors=authors.extend)
            self.assertEqual(authors[0]["external_id"], "42")
            self.assertNotIn("secret", (folder / "authors.json").read_text())
            self.assertEqual(video.saved_videos(folder), [])

    async def test_finished_video_manifest_contains_normalized_author(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            path = folder / "clip.mp4"
            path.write_bytes(b"video")
            info = {"filepath": str(path), "uploader": "Poster", "channel_id": "42"}
            command = [sys.executable, "-c", f"print({json.dumps(json.dumps(info))})"]
            with patch.object(video, "download_command", return_value=command):
                await video.download_video("https://facebook.com/reel/123", folder, None)
            self.assertEqual(video.saved_videos(folder)[0]["authors"][0]["name"], "Poster")
            self.assertEqual(path.read_bytes(), b"video")
