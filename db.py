"""
db.py — нормалізована SQLite-БД для PixGrabber Bot.

Схема:
  downloads        — основний запис про завантаження
  zip_parts        — частини ZIP-архіву
  archive_messages — Telegram-повідомлення з архівами
  image_messages   — Telegram-повідомлення з окремими фото
  preview_messages — Telegram-повідомлення з превʼю

Публічний API повністю сумісний зі старим (history як list[dict]),
тому bot.py потребує мінімальних змін.
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

HISTORY_DB_FILE = Path("history.sqlite3")
HISTORY_FILE = Path("history.json")  # старий файл — для міграції


@contextmanager
def get_db():
    """Context-manager: відкриває з'єднання, комітить або rollback-ає."""
    conn = sqlite3.connect(HISTORY_DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS downloads (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    url           TEXT    NOT NULL,
    gallery_name  TEXT    NOT NULL DEFAULT 'in_progress',
    image_count   INTEGER NOT NULL DEFAULT 0,
    status        TEXT    NOT NULL DEFAULT 'in_progress',
    resume_url    TEXT    NOT NULL DEFAULT '',
    download_dir  TEXT    NOT NULL DEFAULT '',
    date          TEXT    NOT NULL,
    archived_at   TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_downloads_url    ON downloads(url);
CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);

CREATE TABLE IF NOT EXISTS zip_parts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    path        TEXT    NOT NULL,
    part_number INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_zip_parts_download ON zip_parts(download_id);

CREATE TABLE IF NOT EXISTS archive_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    chat_id     TEXT    NOT NULL,
    message_id  INTEGER NOT NULL,
    part_number INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_archive_messages_download ON archive_messages(download_id);

CREATE TABLE IF NOT EXISTS image_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    file_name   TEXT    NOT NULL DEFAULT '',
    chat_id     TEXT    NOT NULL,
    message_id  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_image_messages_download ON image_messages(download_id);

CREATE TABLE IF NOT EXISTS preview_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    chat_id     TEXT    NOT NULL,
    message_id  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_preview_messages_download ON preview_messages(download_id);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""

CURRENT_SCHEMA_VERSION = 1


def init_db():
    """Створює таблиці та запускає міграцію зі старої схеми."""
    with get_db() as conn:
        conn.executescript(_SCHEMA)

        version_row = conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        current_version = version_row["version"] if version_row else 0

        if current_version < CURRENT_SCHEMA_VERSION:
            _migrate(conn, current_version)
            conn.execute(
                "INSERT OR REPLACE INTO schema_version(version) VALUES (?)",
                (CURRENT_SCHEMA_VERSION,)
            )
            log.info(f"БД оновлено до версії {CURRENT_SCHEMA_VERSION}")


def _migrate(conn: sqlite3.Connection, from_version: int):
    """Запускає всі pending-міграції по порядку."""
    if from_version < 1:
        _migrate_v0_to_v1(conn)


def _migrate_v0_to_v1(conn: sqlite3.Connection):
    """
    Конвертує дані зі старої схеми (одна таблиця history_entries з JSON-блобом)
    або зі старого history.json у нормалізовані таблиці.
    """
  # 1) Спробуємо взяти дані зі старої таблиці history_entries
    old_entries: list[dict] = []

    try:
        rows = conn.execute(
            "SELECT data FROM history_entries ORDER BY id"
        ).fetchall()
        old_entries = [json.loads(row["data"]) for row in rows]
        log.info(f"Міграція: знайдено {len(old_entries)} записів у history_entries")
    except Exception:
        pass  # таблиці немає — не страшно

  # 2) Якщо в старій таблиці нічого — беремо з history.json
    if not old_entries and HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                old_entries = json.load(f)
            log.info(f"Міграція: знайдено {len(old_entries)} записів у history.json")
        except Exception as e:
            log.error(f"Міграція: не вдалося прочитати history.json: {e}")

    if not old_entries:
        log.info("Міграція v0→v1: нічого переносити")
        return

    for entry in old_entries:
        _insert_legacy_entry(conn, entry)

    log.info(f"Міграція v0→v1: перенесено {len(old_entries)} записів")


def _insert_legacy_entry(conn: sqlite3.Connection, entry: dict):
    """Вставляє один legacy-запис у нормалізовані таблиці."""
    cur = conn.execute(
        """
        INSERT INTO downloads
            (url, gallery_name, image_count, status, resume_url, download_dir, date, archived_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry.get("url", ""),
            entry.get("gallery_name", "in_progress"),
            int(entry.get("image_count", 0)),
            entry.get("status", "done"),
            entry.get("resume_url", ""),
            entry.get("download_dir", ""),
            entry.get("date", datetime.now().strftime("%Y-%m-%d %H:%M")),
            entry.get("archived_at", ""),
        )
    )
    download_id = cur.lastrowid

  # zip_parts
    zip_parts: list[str] = entry.get("zip_parts") or []
    if not zip_parts and entry.get("zip_path"):
        zip_parts = [entry["zip_path"]]
    for i, path in enumerate(zip_parts, start=1):
        conn.execute(
            "INSERT INTO zip_parts(download_id, path, part_number) VALUES (?, ?, ?)",
            (download_id, path, i)
        )

  # archive_messages
    archive_messages: list[dict] = entry.get("archive_messages") or []
    if not archive_messages:
        chat_id = entry.get("archive_chat_id", "")
        msg_id = entry.get("archive_message_id", "")
        if chat_id and msg_id:
            archive_messages = [{"chat_id": chat_id, "message_id": int(msg_id)}]
    for i, am in enumerate(archive_messages, start=1):
        conn.execute(
            "INSERT INTO archive_messages(download_id, chat_id, message_id, part_number) VALUES (?,?,?,?)",
            (download_id, str(am.get("chat_id", "")), int(am.get("message_id", 0)), i)
        )

  # image_messages
    for im in (entry.get("image_messages") or []):
        conn.execute(
            "INSERT INTO image_messages(download_id, file_name, chat_id, message_id) VALUES (?,?,?,?)",
            (
                download_id,
                im.get("file_name", ""),
                str(im.get("chat_id", "")),
                int(im.get("message_id", 0))
            )
        )

  # preview_message
    preview = entry.get("preview_message") or {}
    if not preview:
        chat_id = entry.get("preview_chat_id", "")
        msg_id = entry.get("preview_message_id", "")
        if chat_id and msg_id:
            preview = {"chat_id": chat_id, "message_id": int(msg_id)}
    if preview.get("chat_id") and preview.get("message_id"):
        conn.execute(
            "INSERT INTO preview_messages(download_id, chat_id, message_id) VALUES (?,?,?)",
            (download_id, str(preview["chat_id"]), int(preview["message_id"]))
        )


def _row_to_entry(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    """Збирає повний dict-запис (сумісний зі старим форматом) з нормалізованих таблиць."""
    did = row["id"]

    zip_parts_rows = conn.execute(
        "SELECT path FROM zip_parts WHERE download_id=? ORDER BY part_number", (did,)
    ).fetchall()
    zip_parts = [r["path"] for r in zip_parts_rows]

    archive_msg_rows = conn.execute(
        "SELECT chat_id, message_id FROM archive_messages WHERE download_id=? ORDER BY part_number",
        (did,)
    ).fetchall()
    archive_messages = [{"chat_id": r["chat_id"], "message_id": r["message_id"]} for r in archive_msg_rows]

    image_msg_rows = conn.execute(
        "SELECT file_name, chat_id, message_id FROM image_messages WHERE download_id=? ORDER BY id",
        (did,)
    ).fetchall()
    image_messages = [
        {"file_name": r["file_name"], "chat_id": r["chat_id"], "message_id": r["message_id"]}
        for r in image_msg_rows
    ]

    preview_row = conn.execute(
        "SELECT chat_id, message_id FROM preview_messages WHERE download_id=? LIMIT 1", (did,)
    ).fetchone()
    preview_message = (
        {"chat_id": preview_row["chat_id"], "message_id": preview_row["message_id"]}
        if preview_row else {}
    )

    return {
        "_id":              did,
        "url":              row["url"],
        "gallery_name":     row["gallery_name"],
        "image_count":      row["image_count"],
        "status":           row["status"],
        "resume_url":       row["resume_url"],
        "download_dir":     row["download_dir"],
        "date":             row["date"],
        "archived_at":      row["archived_at"],
  # zip
        "zip_parts":        zip_parts,
        "zip_path":         zip_parts[0] if zip_parts else "",
  # archive messages
        "archive_messages": archive_messages,
        "archive_chat_id":  archive_messages[0]["chat_id"]     if archive_messages else "",
        "archive_message_id": archive_messages[0]["message_id"] if archive_messages else "",
  # image messages
        "image_messages":   image_messages,
  # preview
        "preview_message":  preview_message,
        "preview_chat_id":  preview_message.get("chat_id", ""),
        "preview_message_id": preview_message.get("message_id", ""),
    }


def load_history() -> list[dict]:
    """Повертає всі записи у вигляді list[dict] (сумісно зі старим форматом)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM downloads ORDER BY id"
        ).fetchall()
        return [_row_to_entry(conn, row) for row in rows]


def add_history_entry(url: str, download_dir: str) -> int:
    """
    Додає новий запис. Повертає його порядковий індекс у списку (0-based),
    сумісно зі старим API.
    """
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO downloads(url, gallery_name, image_count, status,
                                  resume_url, download_dir, date)
            VALUES (?, 'in_progress', 0, 'in_progress', '', ?, ?)
            """,
            (url, download_dir, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )
        total = conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
    return total - 1


def update_history_entry(index: int, **kwargs):
    """
    Оновлює запис за порядковим індексом.
    Підтримує всі поля що раніше були в JSON-блобі.
    """
    download_id = _index_to_id(index)
    if download_id is None:
        return

    with get_db() as conn:
        scalar_map = {
            "gallery_name": "gallery_name",
            "image_count":  "image_count",
            "status":       "status",
            "resume_url":   "resume_url",
            "download_dir": "download_dir",
            "date":         "date",
            "archived_at":  "archived_at",
        }
        scalar_updates = {
            col: kwargs[key]
            for key, col in scalar_map.items()
            if key in kwargs
        }
        if scalar_updates:
            set_clause = ", ".join(f"{col}=?" for col in scalar_updates)
            values = list(scalar_updates.values()) + [download_id]
            conn.execute(f"UPDATE downloads SET {set_clause} WHERE id=?", values)

        if "zip_parts" in kwargs:
            conn.execute("DELETE FROM zip_parts WHERE download_id=?", (download_id,))
            for i, path in enumerate(kwargs["zip_parts"] or [], start=1):
                conn.execute(
                    "INSERT INTO zip_parts(download_id, path, part_number) VALUES (?,?,?)",
                    (download_id, path, i)
                )
        elif "zip_path" in kwargs and kwargs["zip_path"]:
            existing = conn.execute(
                "SELECT id FROM zip_parts WHERE download_id=? ORDER BY part_number LIMIT 1",
                (download_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE zip_parts SET path=? WHERE id=?",
                    (kwargs["zip_path"], existing["id"])
                )
            else:
                conn.execute(
                    "INSERT INTO zip_parts(download_id, path, part_number) VALUES (?,?,1)",
                    (download_id, kwargs["zip_path"])
                )

        if "archive_messages" in kwargs:
            conn.execute("DELETE FROM archive_messages WHERE download_id=?", (download_id,))
            for i, am in enumerate(kwargs["archive_messages"] or [], start=1):
                conn.execute(
                    "INSERT INTO archive_messages(download_id, chat_id, message_id, part_number) VALUES (?,?,?,?)",
                    (download_id, str(am["chat_id"]), int(am["message_id"]), i)
                )
        elif "archive_message_id" in kwargs and kwargs.get("archive_message_id"):
            chat_id = kwargs.get("archive_chat_id", "")
            msg_id = int(kwargs["archive_message_id"])
            existing = conn.execute(
                "SELECT id FROM archive_messages WHERE download_id=? ORDER BY part_number LIMIT 1",
                (download_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE archive_messages SET chat_id=?, message_id=? WHERE id=?",
                    (chat_id, msg_id, existing["id"])
                )
            else:
                conn.execute(
                    "INSERT INTO archive_messages(download_id, chat_id, message_id, part_number) VALUES (?,?,?,1)",
                    (download_id, chat_id, msg_id)
                )

        _update_preview(conn, download_id, kwargs)


def _update_preview(conn: sqlite3.Connection, download_id: int, kwargs: dict):
    preview = kwargs.get("preview_message") or {}
    chat_id = str(preview.get("chat_id", "")  or kwargs.get("preview_chat_id", ""))
    msg_id_v = preview.get("message_id", "")    or kwargs.get("preview_message_id", "")

    if not chat_id or not msg_id_v:
        return

    msg_id = int(msg_id_v)
    existing = conn.execute(
        "SELECT id FROM preview_messages WHERE download_id=? LIMIT 1", (download_id,)
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE preview_messages SET chat_id=?, message_id=? WHERE id=?",
            (chat_id, msg_id, existing["id"])
        )
    else:
        conn.execute(
            "INSERT INTO preview_messages(download_id, chat_id, message_id) VALUES (?,?,?)",
            (download_id, chat_id, msg_id)
        )


def delete_history_entry(index: int) -> Optional[dict]:
    """Видаляє запис за індексом. Повертає видалений dict або None."""
    history = load_history()
    if index < 0 or index >= len(history):
        return None
    entry = history[index]
    download_id = entry["_id"]
    with get_db() as conn:
        conn.execute("DELETE FROM downloads WHERE id=?", (download_id,))
    return entry


def append_zip_part(download_id: int, path: str):
    with get_db() as conn:
        max_part = conn.execute(
            "SELECT COALESCE(MAX(part_number),0) FROM zip_parts WHERE download_id=?",
            (download_id,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO zip_parts(download_id, path, part_number) VALUES (?,?,?)",
            (download_id, path, max_part + 1)
        )


def append_archive_message(download_id: int, chat_id: str, message_id: int):
    with get_db() as conn:
        max_part = conn.execute(
            "SELECT COALESCE(MAX(part_number),0) FROM archive_messages WHERE download_id=?",
            (download_id,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO archive_messages(download_id, chat_id, message_id, part_number) VALUES (?,?,?,?)",
            (download_id, chat_id, message_id, max_part + 1)
        )


def append_image_message(download_id: int, file_name: str, chat_id: str, message_id: int):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO image_messages(download_id, file_name, chat_id, message_id) VALUES (?,?,?,?)",
            (download_id, file_name, chat_id, message_id)
        )


def set_preview_message(download_id: int, chat_id: str, message_id: int):
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM preview_messages WHERE download_id=? LIMIT 1", (download_id,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE preview_messages SET chat_id=?, message_id=? WHERE id=?",
                (chat_id, message_id, existing["id"])
            )
        else:
            conn.execute(
                "INSERT INTO preview_messages(download_id, chat_id, message_id) VALUES (?,?,?)",
                (download_id, chat_id, message_id)
            )


def find_download_id_by_index(index: int) -> Optional[int]:
    return _index_to_id(index)


def find_done_entry_by_url(normalized_url: str) -> Optional[int]:
    """
    Повертає порядковий індекс найкращого done-запису для URL або None.
    """
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id FROM downloads WHERE url=? AND status='done' ORDER BY id",
            (normalized_url,)
        ).fetchall()
        if not rows:
            return None

        best_index = None
        best_score = -1

        all_history = load_history()
        id_to_index = {entry["_id"]: i for i, entry in enumerate(all_history)}

        for row in rows:
            did = row["id"]
            idx = id_to_index.get(did)
            if idx is None:
                continue
            entry = all_history[idx]

            has_zip = any(Path(p).exists() for p in entry["zip_parts"])
            has_archive_tg = bool(entry["archive_messages"])
            has_preview = bool(entry["preview_message"])

            if not has_zip and not has_archive_tg:
                continue

            score = 0
            if has_archive_tg: score += 100
            if has_preview:     score += 50
            if entry["zip_parts"]: score += 20
            if entry["zip_path"]:  score += 10
            score += idx

            if score > best_score:
                best_score = score
                best_index = idx

        return best_index


def dedup_history() -> tuple[int, int]:
    """
    Видаляє дублікати (однаковий URL).
    Повертає (кількість видалених записів, кількість видалених ZIP-файлів).
    """
    history = load_history()

    from collections import defaultdict
    url_groups: dict[str, list[int]] = defaultdict(list)
    for idx, entry in enumerate(history):
        url_groups[entry["url"]].append(idx)

    indices_to_delete: set[int] = set()
    deleted_files = 0

    def _score(entry: dict, idx: int) -> int:
        s = 0
        if entry["archive_messages"] or entry["archive_message_id"]: s += 100
        if entry["preview_message"] or entry["preview_message_id"]:  s += 50
        if entry["zip_parts"]:  s += 20
        if entry["zip_path"]:   s += 10
        s += idx
        return s

    for _url, indices in url_groups.items():
        if len(indices) <= 1:
            continue

        done = [i for i in indices if history[i]["status"] == "done"]
        non_done = [i for i in indices if history[i]["status"] != "done"]

        if done:
            best = max(done, key=lambda i: _score(history[i], i))
            to_remove = non_done + [i for i in done if i != best]
        else:
            newest = max(non_done)
            to_remove = [i for i in non_done if i != newest]

        for i in to_remove:
            for path_str in history[i]["zip_parts"]:
                p = Path(path_str)
                try:
                    if p.exists():
                        p.unlink()
                        deleted_files += 1
                except Exception as e:
                    log.error(f"dedup: не вдалося видалити {p}: {e}")
            indices_to_delete.add(i)

    if not indices_to_delete:
        return 0, 0

    ids_to_delete = [history[i]["_id"] for i in indices_to_delete]
    with get_db() as conn:
        conn.executemany(
            "DELETE FROM downloads WHERE id=?",
            [(did,) for did in ids_to_delete]
        )

    return len(indices_to_delete), deleted_files


HISTORY_PAGE_SIZE = 5

def get_history_page_count(total_items: int) -> int:
    if total_items <= 0:
        return 1
    return (total_items + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE


def normalize_history_page(page: int, total_items: int) -> int:
    return max(0, min(page, get_history_page_count(total_items) - 1))


def save_history(history: list[dict]):
    """
    Повністю замінює вміст БД на переданий список.
    Використовується тільки для сумісності зі старими викликами.
    """
    with get_db() as conn:
        conn.execute("DELETE FROM downloads")  # CASCADE видалить дочірні
        for entry in history:
            _insert_legacy_entry(conn, entry)


def _index_to_id(index: int) -> Optional[int]:
    """Конвертує порядковий індекс (0-based) у primary key downloads.id."""
    with get_db() as conn:
        rows = conn.execute("SELECT id FROM downloads ORDER BY id").fetchall()
        if index < 0 or index >= len(rows):
            return None
        return rows[index]["id"]


def _id_to_index(download_id: int) -> Optional[int]:
    with get_db() as conn:
        rows = conn.execute("SELECT id FROM downloads ORDER BY id").fetchall()
        for i, row in enumerate(rows):
            if row["id"] == download_id:
                return i
        return None
