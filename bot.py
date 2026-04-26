import asyncio
import json
import logging
import re
import html
import signal
import zipfile
import shutil
from typing import Optional
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram import F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

bot = Bot(token=CONFIG["telegram_token"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

BASE_DOWNLOAD_DIR = Path("images")
BASE_DOWNLOAD_DIR.mkdir(exist_ok=True)

ARCHIVE_DIR = Path("arhive")
ARCHIVE_DIR.mkdir(exist_ok=True)

HISTORY_PAGE_SIZE = db.HISTORY_PAGE_SIZE

PENDING_PARTIAL_REQUESTS = {}
PENDING_SEARCH_REQUESTS = set()
PENDING_DOWNLOAD_REQUESTS = {}
PENDING_DELETE_REQUESTS = {}  # user_id -> history_index
download_queue: Optional[asyncio.Queue] = None
queue_worker_started = False
active_downloads = 0

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
MAX_ARCHIVE_PART_SIZE = int(CONFIG.get("max_archive_part_size_mb", 45)) * 1024 * 1024
ARCHIVE_FLUSH_RATIO = float(CONFIG.get("archive_flush_ratio", 0.90))
ARCHIVE_FLUSH_SIZE = max(1, int(MAX_ARCHIVE_PART_SIZE * ARCHIVE_FLUSH_RATIO))
STALL_TIMEOUT = int(CONFIG.get("stall_timeout_sec", 20))
AUTO_RESUME_DELAY = int(CONFIG.get("auto_resume_delay_min", 40)) * 60  # 0 = вимкнено


def get_download_queue() -> asyncio.Queue:
    global download_queue
    if download_queue is None:
        download_queue = asyncio.Queue()
    return download_queue


def get_site_config(url: str):
    url_lower = url.lower()
    for domain, cfg in CONFIG.get("sites", {}).items():
        if domain in url_lower:
            return cfg
    return CONFIG.get("default", {})


def normalize_gallery_url(url: str) -> str:
    parsed = urlparse(url.strip())
    return parsed._replace(query="", fragment="").geturl().rstrip("/")


def get_zip_parts(entry: dict) -> list[Path]:
    zip_parts = entry.get("zip_parts") or []
    if zip_parts:
        return [Path(p) for p in zip_parts]
    if entry.get("zip_path"):
        return [Path(entry["zip_path"])]
    return []


def get_image_refs_from_zip_parts(zip_parts: list[Path]) -> list[dict]:
    image_refs = []
    for zip_path in zip_parts:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                if Path(name).suffix.lower() in IMAGE_EXTENSIONS and not name.endswith('/'):
                    image_refs.append({"zip_path": str(zip_path), "image_name": name})
    return image_refs


def get_image_names_from_zip(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path, 'r') as zf:
        return [
            name for name in zf.namelist()
            if Path(name).suffix.lower() in IMAGE_EXTENSIONS and not name.endswith('/')
        ]


def get_image_messages(entry: dict) -> list[dict]:
    return entry.get("image_messages") or []


def prepare_partial_request(entry: dict, index: int) -> Optional[dict]:
    zip_parts = get_zip_parts(entry)
    image_messages = get_image_messages(entry)
    existing_zip_parts = [p for p in zip_parts if p.exists()]

    image_refs = []
    if existing_zip_parts:
        try:
            image_refs = get_image_refs_from_zip_parts(existing_zip_parts)
        except Exception as e:
            logging.error(f"Не вдалося прочитати ZIP parts: {e}")

    if not image_refs and not image_messages:
        return None

    image_count = len(image_refs) if image_refs else len(image_messages)
    return {
        "history_index": index,
        "zip_parts": [str(p) for p in existing_zip_parts],
        "image_count": image_count,
        "prefer_telegram_history": bool(image_messages),
    }


def make_unique_path(target_path: Path) -> Path:
    if not target_path.exists():
        return target_path
    stem, suffix, parent = target_path.stem, target_path.suffix, target_path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def split_images_by_size(images: list[Path], max_part_size: int) -> list[list[Path]]:
    parts, current_part, current_size = [], [], 0
    for img in images:
        img_size = img.stat().st_size
        if current_part and current_size + img_size > max_part_size:
            parts.append(current_part)
            current_part, current_size = [], 0
        current_part.append(img)
        current_size += img_size
    if current_part:
        parts.append(current_part)
    return parts


def create_zip_file(zip_path: Path, images: list[Path]):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for img in images:
            zf.write(img, img.name)


def get_first_preview_image(images: list[Path]) -> Optional[Path]:
    existing = [img for img in images if img.exists() and img.suffix.lower() in IMAGE_EXTENSIONS]
    return sorted(existing, key=lambda x: x.name.lower())[0] if existing else None


def get_gallery_folder_and_names(folder: Path) -> tuple[str, str]:
    gallery_folder = None
    for p in folder.rglob("*"):
        if p.is_dir():
            try:
                if any(img.is_file() and img.suffix.lower() in IMAGE_EXTENSIONS for img in p.iterdir()):
                    gallery_folder = p
                    break
            except Exception:
                continue
    gallery_name = (gallery_folder or folder).name
    safe_name = re.sub(r'[\\/*?:"<>|]', '_', gallery_name)[:120]
    return gallery_name, safe_name


def get_downloaded_images(folder: Path) -> list[Path]:
    return sorted(
        [f for f in folder.rglob("*.*") if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS],
        key=lambda x: x.stat().st_mtime
    )


def cleanup_images_after_zip(images: list[Path]):
    for img in images:
        try:
            if img.exists():
                img.unlink()
        except Exception as e:
            logging.error(f"Не вдалося видалити файл {img}: {e}")


def cleanup_all_downloaded_images(state: dict):
    cleanup_images_after_zip(state.get("pending_cleanup", []))
    state["pending_cleanup"] = []


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Переглянути історію", callback_data="show_history")],
        [InlineKeyboardButton(text="🔎 Пошук в історії", callback_data="search_history")],
        [InlineKeyboardButton(text="🛠 Службове меню", callback_data="service_menu")]
    ])


def build_service_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬇️ Скачати JSON з історією", callback_data="export_history_json")],
        [InlineKeyboardButton(text="🧹 Очистити дублікати", callback_data="dedup_history")],
        [InlineKeyboardButton(text="📦 Перенести всі архіви", callback_data="archive_all")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_start")]
    ])


def build_redownload_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Скачати наново", callback_data=f"mode:new:{register_download_request(url)}")],
        [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
    ])


def build_resume_keyboard(index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏩ Докачати", callback_data=f"resume:{index}")],
        [InlineKeyboardButton(text="🗑 Видалити з історії", callback_data=f"delete_history_item:{index}")],
        [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
    ])


def build_history_actions_keyboard(index: int, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Переслати архів", callback_data=f"resend:{index}")],
        [InlineKeyboardButton(text="🖼 Переслати частину фото", callback_data=f"partial:{index}")],
        [InlineKeyboardButton(text="📁 Архів", callback_data=f"archive_item:{index}")],
        [InlineKeyboardButton(text="🔗 Отримати посилання", callback_data=f"get_url:{index}")],
        [InlineKeyboardButton(text="🗑 Видалити з історії", callback_data=f"delete_history_item:{index}")],
        [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
    ])


def search_history_entries(query: str) -> list[tuple[int, dict]]:
    normalized = query.strip().lower()
    if not normalized:
        return []
    history = db.load_history()
    results = [
        (i, entry) for i, entry in enumerate(history)
        if normalized in entry.get("gallery_name", "").lower()
        or normalized in entry.get("url", "").lower()
    ]
    return list(reversed(results))


async def send_search_results(message: types.Message, query: str):
    results = search_history_entries(query)
    if not results:
        await message.answer(
            f"🔎 За запитом <b>{query}</b> нічого не знайдено.",
            reply_markup=build_main_menu()
        )
        return

    buttons = []
    for index, entry in results[:10]:
        status = entry.get("status", "done")
        marker = "⏸" if status == "interrupted" else ("🟡" if status == "in_progress" else "✅")
        label = f"{marker} {entry.get('gallery_name', 'Без назви')[:40]} ({entry.get('image_count', 0)} шт.) — {entry.get('date', '')}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"history_item:{index}")])

    buttons.append([InlineKeyboardButton(text="🔎 Новий пошук", callback_data="search_history")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_start")])

    await message.answer(
        f"🔎 Знайдено записів: <b>{len(results)}</b>\nПоказую перші {min(10, len(results))}:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


def parse_photo_selection(text: str, max_count: int) -> list[int]:
    normalized = text.strip().replace(' ', '')
    if not normalized:
        raise ValueError("Порожній список.")
    indices = set()
    for part in normalized.split(','):
        if not part:
            continue
        if '-' in part:
            start_str, end_str = part.split('-', 1)
            if not start_str.isdigit() or not end_str.isdigit():
                raise ValueError("Некоректний діапазон.")
            start, end = int(start_str), int(end_str)
            if start > end:
                raise ValueError("У діапазоні початок більший за кінець.")
            for value in range(start, end + 1):
                if value < 1 or value > max_count:
                    raise ValueError(f"Номер {value} поза межами 1..{max_count}.")
                indices.add(value - 1)
        else:
            if not part.isdigit():
                raise ValueError("Некоректний номер.")
            value = int(part)
            if value < 1 or value > max_count:
                raise ValueError(f"Номер {value} поза межами 1..{max_count}.")
            indices.add(value - 1)
    result = sorted(indices)
    if not result:
        raise ValueError("Не вибрано жодного фото.")
    return result


async def forward_selected_images_from_history(
    image_messages: list[dict], selected_indexes: list[int], message: types.Message
) -> bool:
    if not image_messages:
        return False
    selected = [image_messages[i] for i in selected_indexes if i < len(image_messages)]
    if len(selected) != len(selected_indexes):
        return False

    forwarded = 0
    for im in selected:
        try:
            await bot.forward_message(
                chat_id=message.chat.id,
                from_chat_id=int(im.get("chat_id") or message.chat.id),
                message_id=int(im["message_id"])
            )
            forwarded += 1
        except Exception as e:
            logging.error(f"Не вдалося переслати фото з Telegram history: {e}")
            return False

    await message.answer(f"✅ Відправлено {forwarded} фото з історії Telegram.", reply_markup=build_main_menu())
    return True


def find_image_messages_for_refs(image_refs: list[dict], selected_indexes: list[int], image_messages: list[dict]) -> list[dict]:
    by_name = {im.get("file_name", ""): im for im in image_messages if im.get("file_name")}
    selected_messages = []

    for index in selected_indexes:
        if index >= len(image_refs):
            return []

        file_name = Path(image_refs[index]["image_name"]).name
        image_message = by_name.get(file_name)
        if not image_message:
            return []

        selected_messages.append(image_message)

    return selected_messages


async def send_selected_images_from_refs(
    image_refs: list[dict], selected_indexes: list[int], message: types.Message, history_index: Optional[int] = None
):
    selected_refs = [image_refs[i] for i in selected_indexes]
    batch, batch_refs, sent_count, opened_zips = [], [], 0, {}
    try:
        for idx, ref in enumerate(selected_refs, start=1):
            zp = ref["zip_path"]
            if zp not in opened_zips:
                opened_zips[zp] = zipfile.ZipFile(zp, 'r')
            data = opened_zips[zp].read(ref["image_name"])
            batch.append(types.InputMediaPhoto(
                media=types.BufferedInputFile(data, filename=Path(ref["image_name"]).name),
                caption=f"🖼 {Path(ref['image_name']).name}" if len(batch) == 0 else None
            ))
            batch_refs.append(ref)
            if len(batch) == 10 or idx == len(selected_refs):
                sent_messages = await message.answer_media_group(batch)
                sent_count += len(batch)
                if history_index is not None:
                    for sent_message, sent_ref in zip(sent_messages, batch_refs):
                        append_image_message_to_history(history_index, Path(sent_ref["image_name"]), sent_message)
                batch = []
                batch_refs = []
    finally:
        for zf in opened_zips.values():
            zf.close()
    await message.answer(f"✅ Відправлено {sent_count} фото.", reply_markup=build_main_menu())


async def send_archive_preview(
    message: types.Message, preview_image: Optional[Path],
    gallery_name: str, image_count: int, history_index: int
):
    # Якщо превʼю вже збережено в БД — не відправляємо повторно
    history = db.load_history()
    if 0 <= history_index < len(history):
        existing_preview = history[history_index].get("preview_message") or {}
        if existing_preview.get("message_id"):
            return

    # Якщо передана картинка не існує — шукаємо першу з уже збережених zip_parts
    if (not preview_image or not preview_image.exists()) and 0 <= history_index < len(history):
        existing_zip_parts = [p for p in get_zip_parts(history[history_index]) if p.exists()]
        if existing_zip_parts:
            try:
                refs = get_image_refs_from_zip_parts(existing_zip_parts)
                if refs:
                    preview_image = Path(refs[0]["zip_path"]) if False else None
                    # Читаємо безпосередньо з ZIP
                    with zipfile.ZipFile(refs[0]["zip_path"], 'r') as zf:
                        data = zf.read(refs[0]["image_name"])
                    sent = await message.answer_photo(
                        types.BufferedInputFile(data, filename=Path(refs[0]["image_name"]).name),
                        caption=(
                            f"🖼 Превʼю архіву\n"
                            f"Назва: {gallery_name}\n"
                            f"Зображень: {image_count if image_count else 'рахується...'}"
                        )
                    )
                    download_id = _get_download_id(history_index)
                    if download_id:
                        db.set_preview_message(download_id, str(sent.chat.id), sent.message_id)
                    db.update_history_entry(
                        history_index,
                        preview_chat_id=str(sent.chat.id),
                        preview_message_id=sent.message_id,
                        preview_message={"chat_id": str(sent.chat.id), "message_id": sent.message_id}
                    )
                    return
            except Exception as e:
                logging.error(f"Не вдалося відправити превʼю з ZIP: {e}")

    if not preview_image or not preview_image.exists():
        return

    try:
        data = preview_image.read_bytes()
        sent = await message.answer_photo(
            types.BufferedInputFile(data, filename=preview_image.name),
            caption=(
                f"🖼 Превʼю архіву\n"
                f"Назва: {gallery_name}\n"
                f"Зображень: {image_count if image_count else 'рахується...'}"
            )
        )
        download_id = _get_download_id(history_index)
        if download_id:
            db.set_preview_message(download_id, str(sent.chat.id), sent.message_id)
        # Зворотна сумісність
        db.update_history_entry(
            history_index,
            preview_chat_id=str(sent.chat.id),
            preview_message_id=sent.message_id,
            preview_message={"chat_id": str(sent.chat.id), "message_id": sent.message_id}
        )
    except Exception as e:
        logging.error(f"Не вдалося відправити превʼю: {e}")


async def send_history_item_preview(message: types.Message, index: int, entry: dict):
    preview_message = entry.get("preview_message") or {}
    if not preview_message and entry.get("preview_message_id"):
        preview_message = {
            "chat_id": entry.get("preview_chat_id") or message.chat.id,
            "message_id": entry.get("preview_message_id")
        }

    if preview_message:
        try:
            await bot.forward_message(
                chat_id=message.chat.id,
                from_chat_id=int(preview_message.get("chat_id") or message.chat.id),
                message_id=int(preview_message["message_id"])
            )
            return
        except Exception as e:
            logging.error(f"Не вдалося переслати превʼю з Telegram history: {e}")

    existing_zip_parts = [p for p in get_zip_parts(entry) if p.exists()]
    if not existing_zip_parts:
        return

    try:
        image_refs = get_image_refs_from_zip_parts(existing_zip_parts)
        if not image_refs:
            return

        first_ref = image_refs[0]
        with zipfile.ZipFile(first_ref["zip_path"], 'r') as zf:
            preview_data = zf.read(first_ref["image_name"])

        sent_preview = await message.answer_photo(
            types.BufferedInputFile(preview_data, filename=Path(first_ref["image_name"]).name),
            caption=(
                f"🖼 Превʼю архіву\n"
                f"Назва: {entry['gallery_name']}\n"
                f"Зображень: {entry['image_count']}"
            )
        )
        download_id = entry.get("_id")
        if download_id:
            db.set_preview_message(download_id, str(sent_preview.chat.id), sent_preview.message_id)
        db.update_history_entry(
            index,
            preview_chat_id=str(sent_preview.chat.id),
            preview_message_id=sent_preview.message_id,
            preview_message={"chat_id": str(sent_preview.chat.id), "message_id": sent_preview.message_id}
        )
    except Exception as e:
        logging.error(f"Не вдалося відправити превʼю з ZIP: {e}")


def _get_download_id(history_index: int) -> Optional[int]:
    history = db.load_history()
    if 0 <= history_index < len(history):
        return history[history_index].get("_id")
    return None


def build_archive_part_name(base_name: str, part_number: int, total_parts: int) -> str:
    if total_parts == 1:
        return f"{base_name}.zip"
    return f"{base_name}.part{part_number:03d}.zip"


def build_streaming_archive_part_name(base_name: str, part_number: int) -> str:
    return f"{base_name}.part{part_number:03d}.zip"


def create_streaming_zip_part(folder: Path, images: list[Path], safe_name: str, part_number: int) -> Path:
    zip_path = make_unique_path(folder / build_streaming_archive_part_name(safe_name, part_number))
    create_zip_file(zip_path, images)
    return zip_path


def append_zip_part_to_history(history_index: int, zip_path: Path):
    # Використовуємо тільки append_zip_part — він додає до існуючих частин,
    # не перезаписує весь список. update_history_entry тут не потрібен.
    download_id = _get_download_id(history_index)
    if download_id:
        db.append_zip_part(download_id, str(zip_path))


def append_archive_message_to_history(history_index: int, sent_message: types.Message):
    download_id = _get_download_id(history_index)
    if download_id:
        db.append_archive_message(download_id, str(sent_message.chat.id), sent_message.message_id)


def append_image_message_to_history(history_index: int, image_path: Path, sent_message: types.Message):
    download_id = _get_download_id(history_index)
    if download_id:
        db.append_image_message(
            download_id, image_path.name,
            str(sent_message.chat.id), sent_message.message_id
        )


async def send_single_archive_part(
    message: types.Message, zip_path: Path,
    gallery_name: str, part_number: int, image_count: int,
    history_index: int, is_final_part: bool = False
):
    if part_number == 1 and is_final_part:
        caption = f"📦 Готово!\nЗображень: {image_count}\nНазва: {gallery_name}"
    elif is_final_part:
        caption = f"📦 Фінальна частина {part_number}\nЗображень у частині: {image_count}\nНазва: {gallery_name}"
    else:
        caption = f"📦 Частина {part_number}\nЗображень у частині: {image_count}\nНазва: {gallery_name}"

    data = zip_path.read_bytes()
    sent = await message.answer_document(
        types.BufferedInputFile(data, filename=zip_path.name),
        caption=caption,
    )
    append_archive_message_to_history(history_index, sent)


def _get_existing_zip_parts_from_db(history_index: int) -> list[str]:
    """Повертає список zip_parts що вже збережені в БД для цього запису."""
    history = db.load_history()
    if 0 <= history_index < len(history):
        return history[history_index].get("zip_parts") or []
    return []


async def flush_archive_part(
    folder: Path, message: types.Message, history_index: int,
    state: dict, images: list[Path], is_final_part: bool = False
):
    if not images:
        return

    gallery_name = state.get("gallery_name")
    safe_name = state.get("safe_name")
    if not gallery_name or not safe_name:
        gallery_name, safe_name = get_gallery_folder_and_names(folder)
        state["gallery_name"] = gallery_name
        state["safe_name"] = safe_name

    if not state.get("preview_sent"):
        await send_archive_preview(message, get_first_preview_image(images), gallery_name, 0, history_index)
        state["preview_sent"] = True

    state["part_number"] += 1
    part_number = state["part_number"]
    zip_path = create_streaming_zip_part(folder, images, safe_name, part_number)

    # Додаємо нову частину через append (не перезаписуємо весь список)
    append_zip_part_to_history(history_index, zip_path)
    state["zip_paths"].append(str(zip_path))
    state["image_count"] += len(images)
    state.setdefault("pending_cleanup", []).extend(images)

    await send_single_archive_part(
        message=message, zip_path=zip_path, gallery_name=gallery_name,
        part_number=part_number, image_count=len(images),
        history_index=history_index, is_final_part=is_final_part
    )

    # Оновлюємо тільки скалярні поля — zip_parts не чіпаємо, вони вже в БД через append
    db.update_history_entry(
        history_index,
        gallery_name=gallery_name,
        image_count=state["image_count"] + state.get("prev_image_count", 0),
        status="in_progress"
    )


async def send_live_downloaded_image(
    message: types.Message, file_path: Path, state: dict, history_index: Optional[int] = None
):
    key = str(file_path)
    if key in state["live_sent_files"]:
        return
    if not file_path.exists() or file_path.suffix.lower() not in IMAGE_EXTENSIONS:
        return
    try:
        data = file_path.read_bytes()
        sent = await message.answer_photo(
            types.BufferedInputFile(data, filename=file_path.name),
            caption=f"📸 {file_path.name}"
        )
        state["live_sent_files"].add(key)
        if history_index is not None:
            append_image_message_to_history(history_index, file_path, sent)
    except Exception as e:
        logging.error(f"Помилка живої відправки {file_path.name}: {e}")


async def monitor_folder_and_send_archives(
    folder: Path, message: types.Message, history_index: int, state: dict
):
    while True:
        await asyncio.sleep(2)
        try:
            files = get_downloaded_images(folder)
            new_images = []

            for file_path in files:
                key = str(file_path)
                if key in state["processed_files"]:
                    continue
                await send_live_downloaded_image(message, file_path, state, history_index)
                state["pending_files"].append(file_path)
                state["processed_files"].add(key)
                state["last_image_at"] = asyncio.get_running_loop().time()
                new_images.append(file_path)

            if new_images and not state.get("preview_sent"):
                gallery_name, safe_name = get_gallery_folder_and_names(folder)
                state["gallery_name"] = gallery_name
                state["safe_name"] = safe_name
                await send_archive_preview(
                    message, get_first_preview_image(new_images), gallery_name, 0, history_index
                )
                state["preview_sent"] = True

            pending_size = sum(img.stat().st_size for img in state["pending_files"] if img.exists())
            if pending_size >= ARCHIVE_FLUSH_SIZE:
                images_to_flush = [img for img in state["pending_files"] if img.exists()]
                state["pending_files"] = []
                await flush_archive_part(folder, message, history_index, state, images_to_flush)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Помилка в monitor_folder_and_send_archives: {e}")


async def flush_remaining_downloaded_images(
    folder: Path, message: types.Message, history_index: int, state: dict
):
    for file_path in get_downloaded_images(folder):
        key = str(file_path)
        if key not in state["processed_files"]:
            await send_live_downloaded_image(message, file_path, state, history_index)
            state["pending_files"].append(file_path)
            state["processed_files"].add(key)
            state["last_image_at"] = asyncio.get_running_loop().time()

    remaining = [img for img in state["pending_files"] if img.exists()]
    state["pending_files"] = []
    if remaining:
        await flush_archive_part(folder, message, history_index, state, remaining, is_final_part=True)


async def finalize_streaming_archives(
    folder: Path, message: types.Message, history_index: int, state: dict
):
    await flush_remaining_downloaded_images(folder, message, history_index, state)

    if state["image_count"] == 0:
        await message.answer("Не знайдено зображень для архіву.", reply_markup=build_main_menu())
        return

    gallery_name = state.get("gallery_name") or folder.name
    # Рахуємо загальну кількість зображень: поточний сеанс + попередні сеанси
    total_image_count = state["image_count"] + state.get("prev_image_count", 0)
    db.update_history_entry(
        history_index,
        gallery_name=gallery_name,
        image_count=total_image_count,
        status="done",
        resume_url=""
    )
    cleanup_all_downloaded_images(state)

    await message.answer(
        f"✅ Скачування завершено.\n"
        f"📦 Архівних частин (цей сеанс): {len(state['zip_paths'])}\n"
        f"🖼 Зображень загалом: {total_image_count}",
        reply_markup=build_main_menu()
    )


def move_entry_archives_to_archive_dir(index: int) -> tuple[int, list[str]]:
    history = db.load_history()
    if index < 0 or index >= len(history):
        return 0, []

    entry = history[index]
    zip_parts = get_zip_parts(entry)
    moved_paths = []

    for zip_path in zip_parts:
        if not zip_path.exists() or not zip_path.is_file():
            continue
        try:
            target = make_unique_path(ARCHIVE_DIR / zip_path.name)
            shutil.move(str(zip_path), str(target))
            moved_paths.append(str(target))
        except Exception as e:
            logging.error(f"Не вдалося перенести архів {zip_path}: {e}")

    if moved_paths:
        db.update_history_entry(
            index,
            zip_parts=moved_paths,
            zip_path=moved_paths[0],
            archived_at=datetime.now().strftime("%Y-%m-%d %H:%M")
        )

    return len(moved_paths), moved_paths


def archive_all_entries() -> tuple[int, int]:
    history = db.load_history()
    processed, total_moved = 0, 0
    for idx, entry in enumerate(history):
        if entry.get("status") != "done":
            continue
        moved_count, _ = move_entry_archives_to_archive_dir(idx)
        if moved_count:
            processed  += 1
            total_moved += moved_count
    return processed, total_moved


def find_done_history_entry_by_url(url: str) -> Optional[int]:
    return db.find_done_entry_by_url(normalize_gallery_url(url))


def extract_resume_url(text: str) -> Optional[str]:
    match = re.search(r"Use ['\"]([^'\"]+)['\"] as input URL to continue", text)
    if match:
        return match.group(1)
    urls = re.findall(r'https?://[^\s<>"\']+', text)
    return urls[-1] if urls else None


async def send_start_menu(message: types.Message):
    await message.answer(
        "👏🏽 <b>PixGrabber Bot</b>\n\nНадішли посилання — для завантаження.",
        reply_markup=build_main_menu()
    )


async def send_history(message: types.Message, page: int = 0):
    history = db.load_history()
    if not history:
        await message.answer("Історія порожня.", reply_markup=build_main_menu())
        return

    page = db.normalize_history_page(page, len(history))
    page_count = db.get_history_page_count(len(history))
    start = page * HISTORY_PAGE_SIZE
    end = start + HISTORY_PAGE_SIZE
    reversed_items = list(enumerate(reversed(history)))
    page_items = reversed_items[start:end]

    buttons = []
    for i, entry in page_items:
        real_index = len(history) - 1 - i
        status = entry.get("status", "done")
        if status == "interrupted":
            marker = "⏸"
        elif Path(entry.get("zip_path", "")).exists() \
             or entry.get("archive_message_id") \
             or entry.get("archive_messages"):
            marker = "✅"
        elif status == "in_progress":
            marker = "🟡"
        else:
            marker = "❌"
        label = f"{marker} {entry['gallery_name'][:40]} ({entry['image_count']} шт.) — {entry['date']}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"history_item:{real_index}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"history_page:{page - 1}"))
    if page < page_count - 1:
        nav.append(InlineKeyboardButton(text="➡️ Далі", callback_data=f"history_page:{page + 1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="🛠 Службове меню", callback_data="service_menu")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_start")])

    await message.answer(
        f"📋 Історія завантажень ({len(history)} шт.)\n"
        f"Сторінка {page + 1}/{page_count}. Показано до {HISTORY_PAGE_SIZE} записів:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


async def run_download(message: types.Message, url: str, history_index: int, download_dir: Path):
    cfg = get_site_config(url)
    username = cfg.get("username")
    password = cfg.get("password")

    download_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "gallery-dl",
        "--dest", str(download_dir),
        "--filename", "{num:0>4}.{extension}",
        "--no-part",
        "--no-mtime",
        "--retries",      str(CONFIG.get("gallery_dl_retries", 5)),
        "--retry-wait",   str(CONFIG.get("gallery_dl_retry_wait", 5)),
        "--sleep",        str(CONFIG.get("gallery_dl_sleep", 1)),
        "--sleep-request", str(CONFIG.get("gallery_dl_sleep_request", 1)),
    ]
    if username and password:
        cmd.extend(["--username", username, "--password", password])
    cookies_browser = CONFIG.get("gallery_dl_cookies_browser", "")
    if cookies_browser:
        cmd.extend(["--cookies-from-browser", cookies_browser])
    if any(d in url.lower() for d in ["e-hentai.org", "exhentai.org"]):
        cookies_path = Path("cookies.txt")
        if cookies_path.exists():
            cmd.extend(["--cookies", str(cookies_path.resolve())])
    cmd.append(url)

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    loop = asyncio.get_running_loop()

    # Підтягуємо кількість зображень з попередніх сеансів докачки
    prev_entry = db.load_history()
    prev_image_count = prev_entry[history_index].get("image_count", 0) if history_index < len(prev_entry) else 0

    archive_state = {
        "processed_files": set(),
        "live_sent_files": set(),
        "pending_files": [],
        "pending_cleanup": [],
        "zip_paths": [],
        "archive_messages":[],
        "image_count": 0,
        "prev_image_count": prev_image_count,  # зображення з попередніх сеансів
        "part_number": 0,
        "preview_sent": False,
        "gallery_name": "",
        "safe_name": "",
        "last_image_at": loop.time()
    }

    output_lines: list[str] = []
    sent_error_lines: set[str] = set()
    stall_triggered = False

    def _is_gallery_error_line(line: str) -> bool:
        ll = line.lower()
        return any(kw in ll for kw in (
            "error", "warning", "exception", "failed", "forbidden",
            "unauthorized", "timeout", "timed out", "403", "404", "429", "ssl"
        ))

    async def _send_gallery_log(lines: list[str], title: str = "⚠️ <b>Лог gallery-dl:</b>"):
        if not lines:
            return
        text = "\n".join(lines[-20:])
        try:
            await message.answer(f"{title}\n<pre>{html.escape(text[:3000])}</pre>")
        except Exception as e:
            logging.error(f"Не вдалося відправити лог: {e}")

    async def _pipe_reader(stream: asyncio.StreamReader, stream_name: str):
        pending_errors: list[str] = []
        last_error_sent_at = 0.0
        while True:
            try:
                line = await stream.readline()
            except Exception:
                break
            if not line:
                break
            decoded = line.decode("utf-8", errors="ignore").rstrip("\n")
            if not decoded:
                continue
            tagged = f"[{stream_name}] {decoded}"
            output_lines.append(tagged)
            logging.info(f"[gallery-dl] {tagged}")
            if _is_gallery_error_line(decoded) and tagged not in sent_error_lines:
                sent_error_lines.add(tagged)
                pending_errors.append(tagged)
            now = loop.time()
            if pending_errors and now - last_error_sent_at >= 5:
                await _send_gallery_log(pending_errors)
                pending_errors = []
                last_error_sent_at = now
        if pending_errors:
            await _send_gallery_log(pending_errors)

    async def _stop_process_by_interrupt():
        if process.returncode is not None:
            return
        try:
            process.send_signal(signal.SIGINT)
        except ProcessLookupError:
            return
        except Exception as e:
            logging.error(f"[watchdog] Не вдалося надіслати SIGINT: {e}")
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=10)
        except asyncio.TimeoutError:
            logging.warning("[watchdog] kill після таймауту SIGINT")
            try:
                process.kill()
            except Exception:
                pass

    async def _watchdog():
        nonlocal stall_triggered
        while process.returncode is None:
            await asyncio.sleep(1)
            elapsed = loop.time() - archive_state.get("last_image_at", loop.time())
            if elapsed < STALL_TIMEOUT:
                continue
            logging.warning(f"[watchdog] Немає нових картинок {STALL_TIMEOUT}с — SIGINT")
            stall_triggered = True
            await message.answer(
                f"⏱ Нова картинка не зʼявлялась більше {STALL_TIMEOUT} сек — роблю Ctrl+C."
            )
            await _stop_process_by_interrupt()
            return

    monitor_task = asyncio.create_task(monitor_folder_and_send_archives(download_dir, message, history_index, archive_state))
    stdout_task = asyncio.create_task(_pipe_reader(process.stdout, "stdout"))
    stderr_task = asyncio.create_task(_pipe_reader(process.stderr, "stderr"))
    watchdog_task = asyncio.create_task(_watchdog())

    try:
        await asyncio.wait_for(process.wait(), timeout=1800)
    except asyncio.TimeoutError:
        logging.warning("gallery-dl: загальний таймаут 30 хв")
        stall_triggered = True
        await _stop_process_by_interrupt()
    finally:
        watchdog_task.cancel()
        monitor_task.cancel()
        for t in (watchdog_task, monitor_task):
            try:
                await t
            except asyncio.CancelledError:
                pass
        try:
            await asyncio.wait_for(asyncio.gather(stdout_task, stderr_task, return_exceptions=True), timeout=5)
        except asyncio.TimeoutError:
            stdout_task.cancel()
            stderr_task.cancel()

    full_output = "\n".join(output_lines)
    resume_url = extract_resume_url(full_output)
    error_lines = [ln for ln in output_lines if _is_gallery_error_line(ln)]
    unsent_errors = [ln for ln in error_lines if ln not in sent_error_lines]
    if unsent_errors:
        await _send_gallery_log(unsent_errors)
    if stall_triggered:
        await _send_gallery_log(output_lines, title="📋 <b>Останній лог gallery-dl перед зупинкою:</b>")

    interrupted = (process.returncode != 0) or resume_url or stall_triggered

    if interrupted:
        await flush_remaining_downloaded_images(download_dir, message, history_index, archive_state)
        total_image_count = archive_state["image_count"] + archive_state.get("prev_image_count", 0)
        db.update_history_entry(
            history_index,
            gallery_name=archive_state.get("gallery_name") or "in_progress",
            image_count=total_image_count,
            status="interrupted",
            resume_url=resume_url or "",
            download_dir=str(download_dir)
        )
        cleanup_all_downloaded_images(archive_state)

        resume_text = f"\n🔁 Resume URL: {resume_url}" if resume_url else "\n🔁 Resume URL не знайдено."
        await message.answer(
            f"⚠️ Скачування перервано або неповне.\n"
            f"📦 Архівних частин (цей сеанс): {len(archive_state['zip_paths'])}\n"
            f"🖼 Зображень загалом: {total_image_count}"
            f"{resume_text}"
        )
        if resume_url:
            await message.answer("🔄 Можна продовжити з цього місця.", reply_markup=build_resume_keyboard(history_index))
            if AUTO_RESUME_DELAY > 0:
                delay_min = AUTO_RESUME_DELAY // 60
                await message.answer(
                    f"⏳ Автоматична докачка почнеться через {delay_min} хв.\n"
                    f"Щоб скасувати — видали запис з історії."
                )
                asyncio.create_task(_auto_resume(message, history_index, resume_url, download_dir))
        await send_start_menu(message)
        return

    await finalize_streaming_archives(download_dir, message, history_index, archive_state)


async def _auto_resume(message: types.Message, history_index: int, resume_url: str, download_dir: Path):
    """Автоматично докачує після затримки AUTO_RESUME_DELAY секунд."""
    global queue_worker_started
    await asyncio.sleep(AUTO_RESUME_DELAY)

    # Перевіряємо чи запис ще існує і ще interrupted (юзер міг видалити або вже докачав вручну)
    history = db.load_history()
    if history_index >= len(history):
        return
    entry = history[history_index]
    if entry.get("status") != "interrupted":
        return
    if not entry.get("resume_url"):
        return

    logging.info(f"[auto_resume] Автодокачка history_index={history_index}")
    await message.answer("⏩ Починаю автоматичну докачку...")

    db.update_history_entry(history_index, status="in_progress")
    queue = get_download_queue()
    await queue.put((message, resume_url, history_index, download_dir))

    if not queue_worker_started:
        queue_worker_started = True
        asyncio.create_task(queue_worker())


async def queue_worker():
    global active_downloads
    queue = get_download_queue()
    while True:
        message, url, history_index, download_dir = await queue.get()
        active_downloads += 1
        try:
            await run_download(message, url, history_index, download_dir)
        except Exception as e:
            logging.exception(f"Помилка в черзі для {url}: {e}")
            await send_start_menu(message)
        finally:
            active_downloads -= 1
            queue.task_done()


def register_download_request(url: str) -> str:
    token = str(len(PENDING_DOWNLOAD_REQUESTS) + 1)
    PENDING_DOWNLOAD_REQUESTS[token] = url
    return token


def get_download_request(token: str) -> Optional[str]:
    return PENDING_DOWNLOAD_REQUESTS.get(token)


def _find_active_history_entry_by_url(url: str) -> Optional[int]:
    """Повертає індекс interrupted/in_progress запису для URL, якщо є."""
    normalized = normalize_gallery_url(url)
    history = db.load_history()
    for i, entry in enumerate(history):
        if normalize_gallery_url(entry.get("url", "")) != normalized:
            continue
        if entry.get("status") in ("interrupted", "in_progress"):
            return i
    return None


async def handle_url(message: types.Message, url: str):
    # Спочатку перевіряємо чи є незавершене завантаження
    active_index = _find_active_history_entry_by_url(url)
    if active_index is not None:
        history = db.load_history()
        entry = history[active_index]
        status = entry.get("status")
        if status == "interrupted" and entry.get("resume_url"):
            await message.answer(
                f"⏸ Це посилання вже є в історії — скачування було перерване.\n\n"
                f"📁 <b>{entry['gallery_name']}</b>\n"
                f"🖼 Збережено: {entry['image_count']} зображень\n"
                f"📅 {entry['date']}\n\n"
                f"Докачати з того самого місця?",
                reply_markup=build_resume_keyboard(active_index)
            )
        else:
            await message.answer(
                f"🟡 Це посилання вже завантажується.\n\n"
                f"📁 <b>{entry['gallery_name']}</b>\n"
                f"📅 {entry['date']}",
                reply_markup=build_main_menu()
            )
        return

    existing_index = find_done_history_entry_by_url(url)
    if existing_index is not None:
        history = db.load_history()
        entry = history[existing_index]
        await message.answer(
            f"♻️ Це посилання вже є в історії і архів уже скачано.\n\n"
            f"📁 <b>{entry['gallery_name']}</b>\n"
            f"🖼 Зображень: {entry['image_count']}\n"
            f"📅 {entry['date']}\n\n"
            f"Відправляю готовий архів з історії."
        )
        await send_existing_archive_from_history(message, existing_index)
        return

    await message.answer(f"🔄 Посилання прийнято: <b>{urlparse(url).netloc}</b>")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Грабуємо 👏🏽", callback_data=f"mode:new:{register_download_request(url)}")],
        [InlineKeyboardButton(text="📋 Історія", callback_data="show_history")]
    ])
    await message.answer("Натисни щоб почати завантаження:", reply_markup=keyboard)


async def send_existing_archive_from_history(message: types.Message, index: int):
    history = db.load_history()
    if index < 0 or index >= len(history):
        await message.answer("Запис не знайдено.", reply_markup=build_main_menu())
        return

    entry = history[index]
    zip_parts = get_zip_parts(entry)
    existing = [p for p in zip_parts if p.exists()]

    if entry.get("status") == "interrupted" and entry.get("resume_url"):
        await message.answer("⚠️ Це скачування ще не завершене. Можна докачати:", reply_markup=build_resume_keyboard(index))
        return

    preview_message = entry.get("preview_message") or {}
    if not preview_message and entry.get("preview_message_id"):
        preview_message = {
            "chat_id": entry.get("preview_chat_id") or message.chat.id,
            "message_id": entry.get("preview_message_id")
        }

    archive_messages = entry.get("archive_messages") or []
    if not archive_messages and entry.get("archive_message_id"):
        archive_messages = [{"chat_id": entry.get("archive_chat_id") or message.chat.id, "message_id": entry.get("archive_message_id")}]

    preview_ok = False
    if preview_message:
        try:
            await bot.forward_message(
                chat_id=message.chat.id,
                from_chat_id=int(preview_message.get("chat_id") or message.chat.id),
                message_id=int(preview_message["message_id"])
            )
            preview_ok = True
        except Exception as e:
            logging.error(f"Не вдалося переслати превʼю: {e}")

    if not preview_ok and existing:
        try:
            refs = get_image_refs_from_zip_parts(existing)
            if refs:
                with zipfile.ZipFile(refs[0]["zip_path"], 'r') as zf:
                    prev_data = zf.read(refs[0]["image_name"])
                sent_prev = await message.answer_photo(
                    types.BufferedInputFile(prev_data, filename=Path(refs[0]["image_name"]).name),
                    caption=f"🖼 Превʼю архіву\nНазва: {entry['gallery_name']}\nЗображень: {entry['image_count']}"
                )
                download_id = entry.get("_id")
                if download_id:
                    db.set_preview_message(download_id, str(sent_prev.chat.id), sent_prev.message_id)
                preview_ok = True
        except Exception as e:
            logging.error(f"Не вдалося відправити превʼю з ZIP: {e}")

    if archive_messages:
        try:
            forwarded = 0
            for am in archive_messages:
                await bot.forward_message(
                    chat_id=message.chat.id,
                    from_chat_id=int(am.get("chat_id") or message.chat.id),
                    message_id=int(am["message_id"])
                )
                forwarded += 1
            suffix = " + превʼю" if preview_ok else ""
            await message.answer(f"✅ Архів переслано з історії Telegram{suffix}. Частин: {forwarded}", reply_markup=build_main_menu())
            return
        except Exception as e:
            logging.error(f"Не вдалося переслати архів з Telegram history: {e}")

    if not existing:
        await message.answer(
            "⚠️ Архів недоступний або був видалений.\n\nМожна скачати його наново:",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    sent_messages = []
    total_parts = len(existing)
    for part_number, zip_path in enumerate(existing, start=1):
        caption = (
            f"📦 {entry['gallery_name']}\n🖼 {entry['image_count']} зображень\n📅 {entry['date']}\n🔗 {entry['url']}"
            if total_parts == 1 else
            f"📦 {entry['gallery_name']} — частина {part_number}/{total_parts}\n🖼 {entry['image_count']} зображень загалом\n📅 {entry['date']}\n🔗 {entry['url']}"
        )
        data = zip_path.read_bytes()
        sent = await message.answer_document(
            types.BufferedInputFile(data, filename=zip_path.name),
            caption=caption,
            reply_markup=build_main_menu() if part_number == total_parts else None
        )
        sent_messages.append({"chat_id": str(sent.chat.id), "message_id": sent.message_id})

    download_id = entry.get("_id")
    if download_id and sent_messages:
        db.update_history_entry(
            index,
            archive_chat_id=sent_messages[0]["chat_id"],
            archive_message_id=sent_messages[0]["message_id"],
            archive_messages=sent_messages
        )


@dp.callback_query(F.data == "show_history")
async def show_history_callback(callback: types.CallbackQuery):
    await callback.answer()
    await send_history(callback.message, page=0)


@dp.callback_query(F.data == "search_history")
async def search_history_callback(callback: types.CallbackQuery):
    await callback.answer()
    PENDING_SEARCH_REQUESTS.add(callback.from_user.id)
    PENDING_PARTIAL_REQUESTS.pop(callback.from_user.id, None)
    await callback.message.answer(
        "🔎 Введи частину назви або частину посилання для пошуку в історії.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Скасувати", callback_data="back_to_start")]
        ])
    )


@dp.callback_query(F.data.startswith("history_page:"))
async def history_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split(":", 1)[1])
    await callback.answer()
    await send_history(callback.message, page=page)


@dp.callback_query(F.data == "export_history_json")
async def export_history_json_callback(callback: types.CallbackQuery):
    await callback.answer()
    history = db.load_history()
    if not history:
        await callback.message.answer("Історія порожня.", reply_markup=build_service_menu())
        return
    data = json.dumps(history, ensure_ascii=False, indent=2).encode('utf-8')
    filename = f"history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    await callback.message.answer_document(
        types.BufferedInputFile(data, filename=filename),
        caption=f"📄 Історія завантажень ({len(history)} записів)",
        reply_markup=build_service_menu()
    )


@dp.callback_query(F.data == "service_menu")
async def service_menu_callback(callback: types.CallbackQuery):
    await callback.answer()
    await callback.message.answer("🛠 <b>Службове меню</b>", reply_markup=build_service_menu())


@dp.callback_query(F.data == "dedup_history")
async def dedup_history_callback(callback: types.CallbackQuery):
    await callback.answer()
    removed, files = db.dedup_history()
    if removed == 0:
        await callback.message.answer("✅ Дублікатів не знайдено.", reply_markup=build_service_menu())
    else:
        await callback.message.answer(
            f"🧹 Видалено дублікатів: <b>{removed}</b>\nФайлів видалено з диску: <b>{files}</b>",
            reply_markup=build_service_menu()
        )


@dp.callback_query(F.data == "archive_all")
async def archive_all_callback(callback: types.CallbackQuery):
    await callback.answer()
    processed, moved = archive_all_entries()
    if moved == 0:
        await callback.message.answer(
            f"📦 Нічого переносити — або архіви вже в <code>{ARCHIVE_DIR}</code>, або файлів немає.",
            reply_markup=build_service_menu()
        )
    else:
        await callback.message.answer(
            f"📦 Готово!\nЗаписів оброблено: <b>{processed}</b>\nФайлів перенесено: <b>{moved}</b>",
            reply_markup=build_service_menu()
        )


@dp.callback_query(F.data == "back_to_start")
async def back_to_start_callback(callback: types.CallbackQuery):
    PENDING_SEARCH_REQUESTS.discard(callback.from_user.id)
    PENDING_PARTIAL_REQUESTS.pop(callback.from_user.id, None)
    await callback.answer()
    await callback.message.answer(
        "👋 <b>PixGrabber Bot</b>\n\nНадішли посилання — для завантаження.",
        reply_markup=build_main_menu()
    )


@dp.callback_query(F.data.startswith("history_item:"))
async def history_item_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = db.load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    zip_parts = get_zip_parts(entry)
    status = entry.get("status", "done")

    await callback.answer()

    if status == "interrupted" and entry.get("resume_url"):
        partial_request = prepare_partial_request(entry, index)
        await send_history_item_preview(callback.message, index, entry)
        partial_text = ""
        if partial_request:
            PENDING_PARTIAL_REQUESTS[callback.from_user.id] = partial_request
            partial_text = (
                "\n\n🖼 Можеш одразу написати номери фото або діапазон.\n"
                "Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>"
            )
        await callback.message.answer(
            f"⏸ <b>{entry['gallery_name']}</b>\n"
            f"🖼 Збережено: {entry['image_count']} зображень\n"
            f"📅 {entry['date']}\n"
            f"🔗 {entry['url']}"
            f"{partial_text}",
            reply_markup=build_resume_keyboard(index)
        )
        return

    if status == "in_progress":
        partial_request = prepare_partial_request(entry, index)
        await send_history_item_preview(callback.message, index, entry)
        partial_text = ""
        if partial_request:
            PENDING_PARTIAL_REQUESTS[callback.from_user.id] = partial_request
            partial_text = (
                "\n\n🖼 Можеш одразу написати номери фото або діапазон.\n"
                "Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>"
            )
        await callback.message.answer(
            f"🟡 <b>{entry['gallery_name']}</b>\n"
            f"🖼 Завантажено: {entry['image_count']} зображень\n"
            f"📅 {entry['date']}\n"
            f"🔗 {entry['url']}"
            f"{partial_text}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🖼 Переслати частину фото", callback_data=f"partial:{index}")],
                [InlineKeyboardButton(text="🗑 Видалити з історії", callback_data=f"delete_history_item:{index}")],
                [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
            ])
        )
        return

    partial_request = prepare_partial_request(entry, index)

    if not any(p.exists() for p in zip_parts) \
       and not entry.get("archive_message_id") \
       and not entry.get("archive_messages") \
       and not partial_request:
        await callback.message.answer(
            f"⚠️ Архів для <b>{entry['gallery_name']}</b> недоступний або видалений.\n\n"
            f"Можна спробувати скачати наново:\n{entry['url']}",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    await send_history_item_preview(callback.message, index, entry)

    partial_text = ""
    if partial_request:
        PENDING_PARTIAL_REQUESTS[callback.from_user.id] = partial_request
        partial_text = (
            "\n\n🖼 Можеш одразу написати номери фото або діапазон.\n"
            "Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>"
        )

    await callback.message.answer(
        f"📁 <b>{entry['gallery_name']}</b>\n"
        f"🖼 Зображень: {entry['image_count']}\n"
        f"📅 {entry['date']}\n"
        f"🔗 {entry['url']}"
        f"{partial_text}",
        reply_markup=build_history_actions_keyboard(index, entry["url"])
    )


@dp.callback_query(F.data.startswith("get_url:"))
async def get_url_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = db.load_history()
    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return
    entry = history[index]
    await callback.answer()
    await callback.message.answer(
        f"🔗 Посилання для <b>{entry['gallery_name']}</b>:\n<a href=\"{entry['url']}\">{entry['url']}</a>",
        reply_markup=build_main_menu()
    )


@dp.callback_query(F.data.startswith("delete_history_item:"))
async def delete_history_item_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = db.load_history()
    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    PENDING_DELETE_REQUESTS[callback.from_user.id] = index
    await callback.answer()
    await callback.message.answer(
        f"🗑 Видалити <b>{entry.get('gallery_name', 'Без назви')}</b> з історії?\n\n"
        f"Напиши <code>DELETE</code> великими літерами для підтвердження, або /cancel для скасування.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Скасувати", callback_data="cancel_delete")]
        ])
    )


@dp.callback_query(F.data == "cancel_delete")
async def cancel_delete_callback(callback: types.CallbackQuery):
    PENDING_DELETE_REQUESTS.pop(callback.from_user.id, None)
    await callback.answer("Скасовано.")
    await callback.message.answer("Видалення скасовано.", reply_markup=build_main_menu())


@dp.callback_query(F.data.startswith("archive_item:"))
async def archive_item_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = db.load_history()
    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return
    entry = history[index]
    moved_count, moved_paths = move_entry_archives_to_archive_dir(index)
    await callback.answer()
    if moved_count == 0:
        await callback.message.answer(
            f"⚠️ Не знайшов ZIP-файлів для перенесення у папку <code>{ARCHIVE_DIR}</code>.",
            reply_markup=build_history_actions_keyboard(index, entry.get("url", ""))
        )
        return
    await callback.message.answer(
        f"📁 Переніс архів у папку <code>{ARCHIVE_DIR}</code>.\n"
        f"Файлів перенесено: {moved_count}\n"
        f"Перший файл: <code>{Path(moved_paths[0]).name}</code>",
        reply_markup=build_history_actions_keyboard(index, entry.get("url", ""))
    )


@dp.callback_query(F.data.startswith("partial:"))
async def partial_resend_request(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = db.load_history()
    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]

    if entry.get("status") == "interrupted" and entry.get("resume_url"):
        await callback.answer()
        await callback.message.answer(
            "⚠️ Це завантаження ще не завершене. Спочатку докачай його.",
            reply_markup=build_resume_keyboard(index)
        )
        return

    partial_request = prepare_partial_request(entry, index)
    if not partial_request:
        await callback.answer()
        await callback.message.answer(
            "⚠️ Архів недоступний або був видалений.\nМожна скачати його наново:",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    PENDING_PARTIAL_REQUESTS[callback.from_user.id] = partial_request

    image_count = partial_request["image_count"]
    preview_count = min(10, image_count)
    existing_zip_parts = [Path(p) for p in partial_request.get("zip_parts", []) if Path(p).exists()]
    image_messages = get_image_messages(entry)
    image_refs = []

    if existing_zip_parts:
        try:
            image_refs = get_image_refs_from_zip_parts(existing_zip_parts)
        except Exception as e:
            logging.error(f"Не вдалося прочитати ZIP parts: {e}")

    preview_lines = (
        [f"{i + 1}. {Path(image_refs[i]['image_name']).name}" for i in range(preview_count)]
        if image_refs else
        [f"{i + 1}. {image_messages[i].get('file_name', 'photo')}" for i in range(preview_count)]
    )

    await callback.answer()
    await callback.message.answer(
        f"🖼 <b>{entry['gallery_name']}</b>\n"
        f"В архіві <b>{image_count}</b> фото.\n\n"
        f"Надішли номери фото або діапазон.\n"
        f"Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>\n\n"
        f"Перші {preview_count} фото:\n" + "\n".join(preview_lines)
    )


@dp.callback_query(F.data.startswith("resume:"))
async def resume_download(callback: types.CallbackQuery):
    global queue_worker_started
    index = int(callback.data.split(":", 1)[1])
    history = db.load_history()
    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    resume_url = entry.get("resume_url")
    download_dir_value = entry.get("download_dir")

    if not resume_url:
        await callback.answer("Немає даних для докачки.", show_alert=True)
        return

    if not download_dir_value:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_dir_value = str(BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(resume_url).netloc}")
        db.update_history_entry(index, download_dir=download_dir_value)

    await callback.answer()
    queue = get_download_queue()
    position = queue.qsize() + active_downloads
    if position == 0:
        await callback.message.answer("⏩ Продовжую скачування з того самого місця...")
    else:
        await callback.message.answer(f"⏩ Докачку додано в чергу. Позиція: {position + 1}")

    db.update_history_entry(index, status="in_progress")
    await queue.put((callback.message, resume_url, index, Path(download_dir_value)))

    if not queue_worker_started:
        queue_worker_started = True
        asyncio.create_task(queue_worker())


@dp.callback_query(F.data.startswith("mode:"))
async def process_mode(callback: types.CallbackQuery):
    global queue_worker_started
    _, action, token = callback.data.split(":", 2)
    url = get_download_request(token)
    if not url:
        await callback.answer("Посилання для цієї кнопки вже недоступне. Надішли URL ще раз.", show_alert=True)
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_dir = BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(url).netloc}"
    history_index = db.add_history_entry(url, str(download_dir))

    queue = get_download_queue()
    position = queue.qsize() + active_downloads
    if position == 0:
        await callback.message.edit_text("✅ Починаю скачування...")
    else:
        await callback.message.edit_text(f"✅ Додано в чергу. Позиція: {position + 1}")

    await queue.put((callback.message, url, history_index, download_dir))

    if not queue_worker_started:
        queue_worker_started = True
        asyncio.create_task(queue_worker())


@dp.callback_query(F.data.startswith("resend:"))
async def resend_zip(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    await callback.answer()
    await send_existing_archive_from_history(callback.message, index)


@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    PENDING_SEARCH_REQUESTS.add(message.from_user.id)
    PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)
    await message.answer("🔎 Введи частину назви або частину посилання.", reply_markup=build_main_menu())


@dp.message(Command("history"))
async def cmd_history(message: types.Message):
    await send_history(message)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 <b>PixGrabber Bot</b>\n\nНадішли посилання — для завантаження.",
        reply_markup=build_main_menu()
    )


@dp.message(F.text.regexp(r'^https?://\S+'))
async def on_link(message: types.Message):
    asyncio.create_task(handle_url(message, message.text.strip()))


@dp.message(F.text)
async def handle_partial_selection(message: types.Message):
    if message.from_user.id in PENDING_SEARCH_REQUESTS:
        PENDING_SEARCH_REQUESTS.discard(message.from_user.id)
        await send_search_results(message, message.text.strip())
        return

    if message.from_user.id in PENDING_DELETE_REQUESTS:
        index = PENDING_DELETE_REQUESTS.pop(message.from_user.id)
        if message.text.strip() == "DELETE":
            deleted_entry = db.delete_history_entry(index)
            if not deleted_entry:
                await message.answer("Запис не знайдено.", reply_markup=build_main_menu())
                return
            pending = PENDING_PARTIAL_REQUESTS.get(message.from_user.id)
            if pending and pending.get("history_index") == index:
                PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)
            await message.answer(
                f"🗑 Видалено з історії: <b>{deleted_entry.get('gallery_name', 'Без назви')}</b>\n"
                "Файли архіву на диску не видаляв.",
                reply_markup=build_main_menu()
            )
        else:
            await message.answer("❌ Невірне слово. Видалення скасовано.", reply_markup=build_main_menu())
        return

    pending = PENDING_PARTIAL_REQUESTS.get(message.from_user.id)
    if not pending:
        await send_start_menu(message)
        return

    zip_parts = [Path(p) for p in pending.get("zip_parts", [])]
    existing_zips = [p for p in zip_parts if p.exists()]
    history = db.load_history()
    history_index = pending.get("history_index")
    entry = history[history_index] if history_index is not None and history_index < len(history) else {}
    image_messages = get_image_messages(entry)

    if not existing_zips and not image_messages:
        PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)
        if entry:
            await message.answer("Архів більше не знайдено. Можна скачати наново:", reply_markup=build_redownload_keyboard(entry["url"]))
        else:
            await message.answer("Архів більше не знайдено.", reply_markup=build_main_menu())
        return

    try:
        image_count = pending.get("image_count", 0)
        selected_indexes = parse_photo_selection(message.text, image_count)

        if existing_zips:
            image_refs = get_image_refs_from_zip_parts(existing_zips)
            selected_messages = find_image_messages_for_refs(image_refs, selected_indexes, image_messages)
            if selected_messages and await forward_selected_images_from_history(selected_messages, list(range(len(selected_messages))), message):
                return

            await send_selected_images_from_refs(image_refs, selected_indexes, message, history_index)
            return

        if image_messages and await forward_selected_images_from_history(image_messages, selected_indexes, message):
            return

        await message.answer("Архів більше не знайдено.", reply_markup=build_main_menu())
    except ValueError as e:
        await message.answer(f"⚠️ {e}\n\nСпробуй ще раз. Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>")
        return
    except Exception as e:
        logging.error(f"Помилка вибору частини фото: {e}")
        await message.answer("Не вдалося прочитати архів.", reply_markup=build_main_menu())
    finally:
        PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)


async def main():
    db.init_db()
    get_download_queue()
    logging.info("🚀 Бот запущено...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())