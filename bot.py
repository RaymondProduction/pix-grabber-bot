import asyncio
import json
import logging
import re
from typing import Optional
from pathlib import Path
import zipfile
import shutil
from datetime import datetime
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram import F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

bot = Bot(token=CONFIG["telegram_token"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

BASE_DOWNLOAD_DIR = Path("images")
BASE_DOWNLOAD_DIR.mkdir(exist_ok=True)

ARCHIVE_DIR = Path("arhive")
ARCHIVE_DIR.mkdir(exist_ok=True)

HISTORY_PAGE_SIZE = 5

HISTORY_FILE = Path("history.json")
PENDING_PARTIAL_REQUESTS = {}
PENDING_SEARCH_REQUESTS = set()
download_queue: Optional[asyncio.Queue] = None
queue_worker_started = False
active_downloads = 0

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
MAX_ARCHIVE_PART_SIZE = int(CONFIG.get("max_archive_part_size_mb", 45)) * 1024 * 1024


def get_download_queue() -> asyncio.Queue:
    global download_queue
    if download_queue is None:
        download_queue = asyncio.Queue()
    return download_queue



def load_history() -> list:
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def save_history(history: list):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def add_history_entry(url: str, download_dir: str) -> int:
    history = load_history()
    history.append({
        "url": url,
        "gallery_name": "in_progress",
        "zip_path": "",
        "image_count": 0,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "status": "in_progress",
        "resume_url": "",
        "download_dir": download_dir,
        "archive_chat_id": "",
        "archive_message_id": "",
        "archive_messages": [],
        "preview_chat_id": "",
        "preview_message_id": "",
        "preview_message": {},
        "zip_parts": []
    })
    save_history(history)
    return len(history) - 1


def update_history_entry(index: int, **kwargs):
    history = load_history()
    if index < 0 or index >= len(history):
        return

    for key, value in kwargs.items():
        history[index][key] = value

    save_history(history)


def delete_history_entry(index: int) -> Optional[dict]:
    history = load_history()
    if index < 0 or index >= len(history):
        return None

    deleted_entry = history.pop(index)
    save_history(history)
    return deleted_entry


def get_history_page_count(total_items: int) -> int:
    if total_items <= 0:
        return 1
    return (total_items + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE


def normalize_history_page(page: int, total_items: int) -> int:
    page_count = get_history_page_count(total_items)
    return max(0, min(page, page_count - 1))


def make_unique_path(target_path: Path) -> Path:
    if not target_path.exists():
        return target_path

    stem = target_path.stem
    suffix = target_path.suffix
    parent = target_path.parent
    counter = 1

    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def move_entry_archives_to_archive_dir(index: int) -> tuple[int, list[str]]:
    history = load_history()
    if index < 0 or index >= len(history):
        return 0, []

    entry = history[index]
    zip_parts = get_zip_parts(entry)
    moved_paths = []

    for zip_path in zip_parts:
        if not zip_path.exists() or not zip_path.is_file():
            continue

        try:
            target_path = make_unique_path(ARCHIVE_DIR / zip_path.name)
            shutil.move(str(zip_path), str(target_path))
            moved_paths.append(str(target_path))
        except Exception as e:
            logging.error(f"Не вдалося перенести архів {zip_path} в {ARCHIVE_DIR}: {e}")

    if moved_paths:
        entry["zip_parts"] = moved_paths
        entry["zip_path"] = moved_paths[0]
        entry["archived_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        save_history(history)

    return len(moved_paths), moved_paths


def get_site_config(url: str):
    url_lower = url.lower()
    for domain, cfg in CONFIG.get("sites", {}).items():
        if domain in url_lower:
            return cfg
    return CONFIG.get("default", {})




def normalize_gallery_url(url: str) -> str:
    parsed = urlparse(url.strip())
    return parsed._replace(query="", fragment="").geturl().rstrip("/")


def find_done_history_entry_by_url(url: str) -> Optional[int]:
    target_url = normalize_gallery_url(url)
    history = load_history()

    best_index = None
    best_score = -1

    for index, entry in enumerate(history):
        if normalize_gallery_url(entry.get("url", "")) != target_url:
            continue

        if entry.get("status", "done") != "done":
            continue

        if not entry.get("zip_path") and not entry.get("zip_parts") and not entry.get("archive_messages") and not entry.get("archive_message_id"):
            continue

        score = 0
        if entry.get("archive_messages") or entry.get("archive_message_id"):
            score += 100
        if entry.get("preview_message") or entry.get("preview_message_id"):
            score += 50
        if entry.get("zip_parts"):
            score += 20
        if entry.get("zip_path"):
            score += 10
        score += index

        if score > best_score:
            best_score = score
            best_index = index

    return best_index

def extract_resume_url(text: str) -> Optional[str]:
    # gallery-dl пише: Use 'URL' або "URL" as input URL to continue downloading
    match = re.search(r"Use ['\"]([^'\"]+)['\"] as input URL to continue", text)
    if match:
        return match.group(1)

    urls = re.findall(r'https?://[^\s<>"\']+', text)
    if urls:
        return urls[-1]
    return None


async def send_start_menu(message: types.Message):
    await message.answer(
        "👏🏽 <b>PixGrabber Bot</b>\n\n"
        "Надішли посилання — для завантаження.",
        reply_markup=build_main_menu()
    )


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Переглянути історію", callback_data="show_history")],
        [InlineKeyboardButton(text="🔎 Пошук в історії", callback_data="search_history")]
    ])


def build_redownload_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Скачати наново", callback_data=f"mode:new:{url}")],
        [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
    ])


def build_resume_keyboard(index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏩ Докачати", callback_data=f"resume:{index}")],
        [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
    ])


def get_image_names_from_zip(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path, 'r') as zf:
        return [
            name for name in zf.namelist()
            if Path(name).suffix.lower() in IMAGE_EXTENSIONS
            and not name.endswith('/')
        ]


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
    normalized_query = query.strip().lower()
    if not normalized_query:
        return []

    history = load_history()
    results = []

    for index, entry in enumerate(history):
        gallery_name = entry.get("gallery_name", "").lower()
        url = entry.get("url", "").lower()

        if normalized_query in gallery_name or normalized_query in url:
            results.append((index, entry))

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

        if status == "interrupted":
            marker = "⏸"
        elif status == "in_progress":
            marker = "🟡"
        else:
            marker = "✅"

        label = f"{marker} {entry.get('gallery_name', 'Без назви')[:40]} ({entry.get('image_count', 0)} шт.) — {entry.get('date', '')}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"history_item:{index}")])

    buttons.append([InlineKeyboardButton(text="🔎 Новий пошук", callback_data="search_history")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_start")])

    await message.answer(
        f"🔎 Знайдено записів: <b>{len(results)}</b>\n"
        f"Показую перші {min(10, len(results))}:",
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
            start = int(start_str)
            end = int(end_str)
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


def get_zip_parts(entry: dict) -> list[Path]:
    zip_parts = entry.get("zip_parts") or []
    if zip_parts:
        return [Path(part) for part in zip_parts]

    zip_path = entry.get("zip_path", "")
    if zip_path:
        return [Path(zip_path)]

    return []


def get_image_refs_from_zip_parts(zip_parts: list[Path]) -> list[dict]:
    image_refs = []
    for zip_path in zip_parts:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                if Path(name).suffix.lower() in IMAGE_EXTENSIONS and not name.endswith('/'):
                    image_refs.append({
                        "zip_path": str(zip_path),
                        "image_name": name
                    })
    return image_refs


def build_archive_part_name(base_name: str, part_number: int, total_parts: int) -> str:
    if total_parts == 1:
        return f"{base_name}.zip"
    return f"{base_name}.part{part_number:03d}.zip"


def split_images_by_size(images: list[Path], max_part_size: int) -> list[list[Path]]:
    parts = []
    current_part = []
    current_size = 0

    for img in images:
        img_size = img.stat().st_size
        if current_part and current_size + img_size > max_part_size:
            parts.append(current_part)
            current_part = []
            current_size = 0

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
    existing_images = [
        img for img in images
        if img.exists() and img.suffix.lower() in IMAGE_EXTENSIONS
    ]
    if not existing_images:
        return None

    return sorted(existing_images, key=lambda x: x.name.lower())[0]


async def send_archive_preview(message: types.Message, preview_image: Optional[Path], gallery_name: str, image_count: int, history_index: int):
    if not preview_image or not preview_image.exists():
        return

    try:
        data = preview_image.read_bytes()
        sent_message = await message.answer_photo(
            types.BufferedInputFile(data, filename=preview_image.name),
            caption=(
                f"🖼 Превʼю архіву\n"
                f"Назва: {gallery_name}\n"
                f"Зображень: {image_count}"
            )
        )
        preview_message = {
            "chat_id": str(sent_message.chat.id),
            "message_id": sent_message.message_id
        }
        update_history_entry(
            history_index,
            preview_chat_id=preview_message["chat_id"],
            preview_message_id=preview_message["message_id"],
            preview_message=preview_message
        )
    except Exception as e:
        logging.error(f"Не вдалося відправити превʼю архіву {preview_image}: {e}")


def create_archive_parts(folder: Path, images: list[Path], safe_name: str) -> list[Path]:
    image_parts = split_images_by_size(images, MAX_ARCHIVE_PART_SIZE)
    total_parts = len(image_parts)
    zip_paths = []

    for part_number, part_images in enumerate(image_parts, start=1):
        zip_filename = build_archive_part_name(safe_name, part_number, total_parts)
        zip_path = folder / zip_filename
        create_zip_file(zip_path, part_images)
        zip_paths.append(zip_path)

    return zip_paths


async def send_archive_parts(message: types.Message, zip_paths: list[Path], gallery_name: str, image_count: int, history_index: int):
    archive_messages = []
    total_parts = len(zip_paths)

    for part_number, zip_path in enumerate(zip_paths, start=1):
        if total_parts == 1:
            caption = f"📦 Готово!\nЗображень: {image_count}\nНазва: {gallery_name}"
        else:
            caption = (
                f"📦 Частина {part_number}/{total_parts}\n"
                f"Зображень загалом: {image_count}\n"
                f"Назва: {gallery_name}"
            )

        data = zip_path.read_bytes()
        sent_message = await message.answer_document(
            types.BufferedInputFile(data, filename=zip_path.name),
            caption=caption,
        )
        archive_messages.append({
            "chat_id": str(sent_message.chat.id),
            "message_id": sent_message.message_id
        })

    update_history_entry(
        history_index,
        archive_chat_id=str(archive_messages[0]["chat_id"]) if archive_messages else "",
        archive_message_id=archive_messages[0]["message_id"] if archive_messages else "",
        archive_messages=archive_messages
    )


def cleanup_images_after_zip(images: list[Path]):
    for img in images:
        try:
            if img.exists():
                img.unlink()
        except Exception as e:
            logging.error(f"Не вдалося видалити файл {img}: {e}")


async def send_selected_images_from_refs(image_refs: list[dict], selected_indexes: list[int], message: types.Message):
    selected_refs = [image_refs[i] for i in selected_indexes]

    batch = []
    sent_count = 0
    opened_zips = {}

    try:
        for idx, image_ref in enumerate(selected_refs, start=1):
            zip_path = image_ref["zip_path"]
            image_name = image_ref["image_name"]

            if zip_path not in opened_zips:
                opened_zips[zip_path] = zipfile.ZipFile(zip_path, 'r')

            data = opened_zips[zip_path].read(image_name)
            batch.append(
                types.InputMediaPhoto(
                    media=types.BufferedInputFile(data, filename=Path(image_name).name),
                    caption=f"🖼 {Path(image_name).name}" if len(batch) == 0 else None
                )
            )

            if len(batch) == 10 or idx == len(selected_refs):
                await message.answer_media_group(batch)
                sent_count += len(batch)
                batch = []
    finally:
        for zf in opened_zips.values():
            zf.close()

    await message.answer(f"✅ Відправлено {sent_count} фото.", reply_markup=build_main_menu())


async def send_selected_images(zip_path: Path, image_names: list[str], selected_indexes: list[int], message: types.Message):
    image_refs = [{"zip_path": str(zip_path), "image_name": image_name} for image_name in image_names]
    await send_selected_images_from_refs(image_refs, selected_indexes, message)


async def send_history(message: types.Message, page: int = 0):
    history = load_history()
    if not history:
        await message.answer("Історія порожня.", reply_markup=build_main_menu())
        return

    page = normalize_history_page(page, len(history))
    page_count = get_history_page_count(len(history))
    start_index = page * HISTORY_PAGE_SIZE
    end_index = start_index + HISTORY_PAGE_SIZE
    reversed_items = list(enumerate(reversed(history)))
    page_items = reversed_items[start_index:end_index]

    buttons = []
    for i, entry in page_items:
        real_index = len(history) - 1 - i
        status = entry.get("status", "done")

        if status == "interrupted":
            marker = "⏸"
        elif Path(entry.get("zip_path", "")).exists() or entry.get("archive_message_id") or entry.get("archive_messages"):
            marker = "✅"
        elif status == "in_progress":
            marker = "🟡"
        else:
            marker = "❌"

        label = f"{marker} {entry['gallery_name'][:40]} ({entry['image_count']} шт.) — {entry['date']}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"history_item:{real_index}")])

    navigation_buttons = []
    if page > 0:
        navigation_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"history_page:{page - 1}"))
    if page < page_count - 1:
        navigation_buttons.append(InlineKeyboardButton(text="➡️ Далі", callback_data=f"history_page:{page + 1}"))
    if navigation_buttons:
        buttons.append(navigation_buttons)

    buttons.append([InlineKeyboardButton(text="⬇️ Скачати JSON з історією", callback_data="export_history_json")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_start")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer(
        f"📋 Історія завантажень ({len(history)} шт.)\n"
        f"Сторінка {page + 1}/{page_count}. Показано до {HISTORY_PAGE_SIZE} записів:",
        reply_markup=keyboard
    )

async def monitor_folder(folder: Path, message: types.Message, sent_files: set):
    while True:
        await asyncio.sleep(2)
        try:
            files = sorted(
                [f for f in folder.rglob("*") if f.is_file()],
                key=lambda x: x.stat().st_mtime
            )
            for file_path in files:
                if file_path.suffix.lower() not in IMAGE_EXTENSIONS:
                    continue
                key = str(file_path)
                if key not in sent_files and file_path.exists():
                    try:
                        data = file_path.read_bytes()
                        await message.answer_photo(
                            types.BufferedInputFile(data, filename=file_path.name),
                            caption=f"📸 {file_path.name}"
                        )
                        sent_files.add(key)
                    except Exception as e:
                        logging.error(f"Помилка відправки {file_path.name}: {e}")
        except Exception as e:
            logging.error(f"Помилка в monitor_folder: {e}")


async def create_and_send_zip(folder: Path, message: types.Message, history_index: int):
    images = [f for f in folder.rglob("*.*")
              if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS]

    if not images:
        await message.answer("Не знайдено зображень для архіву.", reply_markup=build_main_menu())
        return

    gallery_folder = None
    for p in folder.rglob("*"):
        if p.is_dir():
            try:
                if any(img.is_file() and img.suffix.lower() in IMAGE_EXTENSIONS for img in p.iterdir()):
                    gallery_folder = p
                    break
            except Exception:
                continue

    if gallery_folder:
        gallery_name = gallery_folder.name
        safe_name = re.sub(r'[\\/*?:"<>|]', '_', gallery_name)[:120]
    else:
        gallery_name = folder.name
        safe_name = re.sub(r'[\\/*?:"<>|]', '_', folder.name)[:120]

    preview_image = get_first_preview_image(images)
    zip_paths = create_archive_parts(folder, images, safe_name)

    update_history_entry(
        history_index,
        gallery_name=gallery_name,
        zip_path=str(zip_paths[0]) if zip_paths else "",
        zip_parts=[str(zip_path) for zip_path in zip_paths],
        image_count=len(images),
        status="done",
        resume_url=""
    )

    try:
        await send_archive_preview(message, preview_image, gallery_name, len(images), history_index)

        if len(zip_paths) > 1:
            await message.answer(f"📦 Архів великий, тому ділю його на {len(zip_paths)} частини.")

        await send_archive_parts(message, zip_paths, gallery_name, len(images), history_index)
        cleanup_images_after_zip(images)
        await send_start_menu(message)
    except Exception as e:
        logging.error(f"Помилка відправки ZIP: {e}")
        await message.answer(f"Не вдалося відправити архів: {e}")
        await send_start_menu(message)


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
        "--no-mtime"
    ]

    if username and password:
        cmd.extend(["--username", username, "--password", password])

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

    sent_files = set()
    monitor_task = asyncio.create_task(monitor_folder(download_dir, message, sent_files))

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=1800)
    finally:
        monitor_task.cancel()

    error_text = (stderr or b"").decode('utf-8', errors='ignore') + (stdout or b"").decode('utf-8', errors='ignore')
    resume_url = extract_resume_url(error_text)

    if process.returncode != 0 or resume_url:
        update_history_entry(
            history_index,
            status="interrupted",
            resume_url=resume_url or "",
            download_dir=str(download_dir)
        )

        await message.answer("⚠️ Скачування перервано або неповне.\nЗбережено те, що встигло.")

        if resume_url:
            await message.answer(
                "🔄 Можна продовжити з цього місця.",
                reply_markup=build_resume_keyboard(history_index)
            )
        await send_start_menu(message)
        return

    await create_and_send_zip(download_dir, message, history_index)


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


async def handle_url(message: types.Message, url: str):
    existing_index = find_done_history_entry_by_url(url)
    if existing_index is not None:
        history = load_history()
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
        [InlineKeyboardButton(text="📸 Грабуємо 👏🏽", callback_data=f"mode:new:{url}")],
        [InlineKeyboardButton(text="📋 Історія", callback_data="show_history")]
    ])
    await message.answer("Натисни щоб почати завантаження:", reply_markup=keyboard)


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
async def export_history_json(callback: types.CallbackQuery):
    await callback.answer()
    history = load_history()
    if not history:
        await callback.message.answer("Історія порожня.", reply_markup=build_main_menu())
        return

    data = json.dumps(history, ensure_ascii=False, indent=2).encode('utf-8')
    filename = f"history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    await callback.message.answer_document(
        types.BufferedInputFile(data, filename=filename),
        caption=f"📄 Історія завантажень ({len(history)} записів)",
        reply_markup=build_main_menu()
    )


@dp.callback_query(F.data == "back_to_start")
async def back_to_start_callback(callback: types.CallbackQuery):
    PENDING_SEARCH_REQUESTS.discard(callback.from_user.id)
    PENDING_PARTIAL_REQUESTS.pop(callback.from_user.id, None)
    await callback.answer()
    await callback.message.answer(
        "👋 <b>PixGrabber Bot</b>\n\n"
        "Надішли посилання — для завантаження.",
        reply_markup=build_main_menu()
    )


@dp.callback_query(F.data.startswith("history_item:"))
async def history_item_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    zip_parts = get_zip_parts(entry)
    status = entry.get("status", "done")

    await callback.answer()

    if status == "interrupted" and entry.get("resume_url"):
        await callback.message.answer(
            f"⏸ <b>{entry['gallery_name']}</b>\n"
            f"Скачування було перерване.\n"
            f"Можна докачати з того самого місця.",
            reply_markup=build_resume_keyboard(index)
        )
        return

    if not any(zip_path.exists() for zip_path in zip_parts) and not entry.get("archive_message_id") and not entry.get("archive_messages"):
        await callback.message.answer(
            f"⚠️ Архів для <b>{entry['gallery_name']}</b> недоступний або був видалений.\n\n"
            f"Можна спробувати скачати його наново з цього посилання:\n{entry['url']}",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    await callback.message.answer(
        f"📁 <b>{entry['gallery_name']}</b>\n"
        f"🖼 Зображень: {entry['image_count']}\n"
        f"📅 {entry['date']}\n"
        f"🔗 {entry['url']}",
        reply_markup=build_history_actions_keyboard(index, entry["url"])
    )


@dp.callback_query(F.data.startswith("get_url:"))
async def get_url_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

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
    deleted_entry = delete_history_entry(index)

    if not deleted_entry:
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    pending = PENDING_PARTIAL_REQUESTS.get(callback.from_user.id)
    if pending and pending.get("history_index") == index:
        PENDING_PARTIAL_REQUESTS.pop(callback.from_user.id, None)

    await callback.answer("Видалено з історії.")
    await callback.message.answer(
        f"🗑 Видалено з історії: <b>{deleted_entry.get('gallery_name', 'Без назви')}</b>\n"
        "Файли архіву на диску не видаляв.",
        reply_markup=build_main_menu()
    )


@dp.callback_query(F.data.startswith("archive_item:"))
async def archive_item_callback(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    moved_count, moved_paths = move_entry_archives_to_archive_dir(index)

    await callback.answer()

    if moved_count == 0:
        await callback.message.answer(
            f"⚠️ Не знайшов ZIP-файлів для перенесення у папку <code>{ARCHIVE_DIR}</code>.\n"
            "Можливо, архів уже перенесений, видалений або доступний тільки через Telegram history.",
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
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    zip_parts = get_zip_parts(entry)

    if entry.get("status") == "interrupted" and entry.get("resume_url"):
        await callback.answer()
        await callback.message.answer(
            "⚠️ Це завантаження ще не завершене. Спочатку докачай його.",
            reply_markup=build_resume_keyboard(index)
        )
        return

    existing_zip_parts = [zip_path for zip_path in zip_parts if zip_path.exists()]
    if not existing_zip_parts:
        await callback.answer()
        await callback.message.answer(
            "⚠️ Архів недоступний або був видалений.\n"
            "Можна скачати його наново з цього посилання:",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    try:
        image_refs = get_image_refs_from_zip_parts(existing_zip_parts)
    except Exception as e:
        logging.error(f"Не вдалося прочитати ZIP parts {existing_zip_parts}: {e}")
        await callback.answer("Не вдалося відкрити архів.", show_alert=True)
        return

    if not image_refs:
        await callback.answer("У архіві немає зображень.", show_alert=True)
        return

    PENDING_PARTIAL_REQUESTS[callback.from_user.id] = {
        "history_index": index,
        "zip_parts": [str(zip_path) for zip_path in existing_zip_parts],
        "image_count": len(image_refs)
    }

    preview_count = min(10, len(image_refs))
    preview_lines = [f"{i + 1}. {Path(image_refs[i]['image_name']).name}" for i in range(preview_count)]
    preview_text = "\n".join(preview_lines)

    await callback.answer()
    await callback.message.answer(
        f"🖼 <b>{entry['gallery_name']}</b>\n"
        f"В архіві <b>{len(image_refs)}</b> фото.\n\n"
        f"Надішли номери фото або діапазон.\n"
        f"Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>\n\n"
        f"Перші {preview_count} фото:\n{preview_text}"
    )


@dp.callback_query(F.data.startswith("resume:"))
async def resume_download(callback: types.CallbackQuery):
    global queue_worker_started
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    resume_url = entry.get("resume_url")
    download_dir_value = entry.get("download_dir")

    if not resume_url:
        await callback.answer("Немає даних для докачки.", show_alert=True)
        return

    # Якщо download_dir з якоїсь причини відсутній — створюємо новий
    if not download_dir_value:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_dir_value = str(BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(resume_url).netloc}")
        update_history_entry(index, download_dir=download_dir_value)

    await callback.answer()
    queue = get_download_queue()
    position = queue.qsize() + active_downloads
    if position == 0:
        await callback.message.answer("⏩ Продовжую скачування з того самого місця...")
    else:
        await callback.message.answer(f"⏩ Докачку додано в чергу. Позиція: {position + 1}")

    update_history_entry(index, status="in_progress")
    await queue.put((callback.message, resume_url, index, Path(download_dir_value)))

    if not queue_worker_started:
        queue_worker_started = True
        asyncio.create_task(queue_worker())


@dp.callback_query(F.data.startswith("mode:"))
async def process_mode(callback: types.CallbackQuery):
    global queue_worker_started
    _, action, url = callback.data.split(":", 2)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_dir = BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(url).netloc}"
    history_index = add_history_entry(url, str(download_dir))

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


async def send_existing_archive_from_history(message: types.Message, index: int):
    history = load_history()

    if index < 0 or index >= len(history):
        await message.answer("Запис не знайдено.", reply_markup=build_main_menu())
        return

    entry = history[index]
    zip_parts = get_zip_parts(entry)
    existing_zip_parts = [zip_path for zip_path in zip_parts if zip_path.exists()]

    if entry.get("status") == "interrupted" and entry.get("resume_url"):
        await message.answer(
            "⚠️ Це скачування ще не завершене. Можна докачати:",
            reply_markup=build_resume_keyboard(index)
        )
        return

    preview_message = entry.get("preview_message") or {}
    if not preview_message and entry.get("preview_message_id"):
        preview_message = {
            "chat_id": entry.get("preview_chat_id") or message.chat.id,
            "message_id": entry.get("preview_message_id")
        }

    archive_messages = entry.get("archive_messages") or []
    if not archive_messages and entry.get("archive_message_id"):
        archive_messages = [{
            "chat_id": entry.get("archive_chat_id") or message.chat.id,
            "message_id": entry.get("archive_message_id")
        }]

    preview_forwarded_or_sent = False

    # 1) Спочатку пробуємо показати превʼю з Telegram history.
    if preview_message:
        try:
            await bot.forward_message(
                chat_id=message.chat.id,
                from_chat_id=int(preview_message.get("chat_id") or message.chat.id),
                message_id=int(preview_message["message_id"])
            )
            preview_forwarded_or_sent = True
        except Exception as e:
            logging.error(f"Не вдалося переслати превʼю з Telegram history: {e}")

    # 2) Якщо preview_message ще немає або він не переслався, але ZIP є на диску —
    #    самовідновлюємо превʼю і записуємо його message_id в history.json.
    if not preview_forwarded_or_sent and existing_zip_parts:
        try:
            image_refs = get_image_refs_from_zip_parts(existing_zip_parts)
            if image_refs:
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
                preview_message = {
                    "chat_id": str(sent_preview.chat.id),
                    "message_id": sent_preview.message_id
                }
                update_history_entry(
                    index,
                    preview_chat_id=preview_message["chat_id"],
                    preview_message_id=preview_message["message_id"],
                    preview_message=preview_message
                )
                preview_forwarded_or_sent = True
        except Exception as e:
            logging.error(f"Не вдалося відправити превʼю з ZIP: {e}")

    # 3) Потім пробуємо переслати архів/частини з Telegram history.
    #    Це має працювати навіть якщо локальні ZIP-файли вже видалені з сервера.
    forwarded_count = 0
    if archive_messages:
        try:
            for archive_message in archive_messages:
                await bot.forward_message(
                    chat_id=message.chat.id,
                    from_chat_id=int(archive_message.get("chat_id") or message.chat.id),
                    message_id=int(archive_message["message_id"])
                )
                forwarded_count += 1

            preview_text = " + превʼю" if preview_forwarded_or_sent else ""
            await message.answer(
                f"✅ Архів переслано з історії Telegram{preview_text}. Частин: {forwarded_count}",
                reply_markup=build_main_menu()
            )
            return
        except Exception as e:
            logging.error(f"Не вдалося переслати архів з Telegram history: {e}")

    # 4) Якщо Telegram history не спрацювала або її ще немає — fallback на локальні ZIP-файли.
    #    Після успішної відправки оновлюємо archive_messages, щоб наступного разу можна було
    #    пересилати вже напряму з Telegram навіть без файлів на диску.
    if not existing_zip_parts:
        await message.answer(
            "⚠️ Архів недоступний або був видалений.\n"
            "У Telegram history теж немає робочого повідомлення з архівом.\n\n"
            "Можна скачати його наново з цього посилання:",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    sent_messages = []
    total_parts = len(existing_zip_parts)
    for part_number, zip_path in enumerate(existing_zip_parts, start=1):
        if total_parts == 1:
            caption = f"📦 {entry['gallery_name']}\n🖼 {entry['image_count']} зображень\n📅 {entry['date']}\n🔗 {entry['url']}"
        else:
            caption = (
                f"📦 {entry['gallery_name']} — частина {part_number}/{total_parts}\n"
                f"🖼 {entry['image_count']} зображень загалом\n"
                f"📅 {entry['date']}\n"
                f"🔗 {entry['url']}"
            )

        data = zip_path.read_bytes()
        sent_message = await message.answer_document(
            types.BufferedInputFile(data, filename=zip_path.name),
            caption=caption,
            reply_markup=build_main_menu() if part_number == total_parts else None
        )
        sent_messages.append({
            "chat_id": str(sent_message.chat.id),
            "message_id": sent_message.message_id
        })

    update_history_entry(
        index,
        archive_chat_id=str(sent_messages[0]["chat_id"]) if sent_messages else "",
        archive_message_id=sent_messages[0]["message_id"] if sent_messages else "",
        archive_messages=sent_messages
    )


@dp.callback_query(F.data.startswith("resend:"))
async def resend_zip(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    await callback.answer()
    await send_existing_archive_from_history(callback.message, index)


@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    PENDING_SEARCH_REQUESTS.add(message.from_user.id)
    PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)
    await message.answer(
        "🔎 Введи частину назви або частину посилання для пошуку в історії.",
        reply_markup=build_main_menu()
    )


@dp.message(Command("history"))
async def cmd_history(message: types.Message):
    await send_history(message)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 <b>PixGrabber Bot</b>\n\n"
        "Надішли посилання — для завантаження.",
        reply_markup=build_main_menu()
    )


@dp.message(F.text.regexp(r'^https?://\S+'))
async def on_link(message: types.Message):
    url = message.text.strip()
    asyncio.create_task(handle_url(message, url))


@dp.message(F.text)
async def handle_partial_selection(message: types.Message):
    if message.from_user.id in PENDING_SEARCH_REQUESTS:
        PENDING_SEARCH_REQUESTS.discard(message.from_user.id)
        await send_search_results(message, message.text.strip())
        return

    pending = PENDING_PARTIAL_REQUESTS.get(message.from_user.id)
    if not pending:
        await send_start_menu(message)
        return

    zip_parts = [Path(zip_path) for zip_path in pending.get("zip_parts", [])]
    if not zip_parts and pending.get("zip_path"):
        zip_parts = [Path(pending["zip_path"])]

    existing_zip_parts = [zip_path for zip_path in zip_parts if zip_path.exists()]
    if not existing_zip_parts:
        history = load_history()
        history_index = pending.get("history_index")
        entry = history[history_index] if history_index is not None and history_index < len(history) else None
        PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)

        if entry:
            await message.answer(
                "Архів більше не знайдено. Можна скачати його наново:",
                reply_markup=build_redownload_keyboard(entry["url"])
            )
        else:
            await message.answer("Архів більше не знайдено.", reply_markup=build_main_menu())
        return

    try:
        image_refs = get_image_refs_from_zip_parts(existing_zip_parts)
        selected_indexes = parse_photo_selection(message.text, len(image_refs))
    except ValueError as e:
        await message.answer(
            f"⚠️ {e}\n\nСпробуй ще раз. Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>"
        )
        return
    except Exception as e:
        logging.error(f"Помилка вибору частини фото: {e}")
        PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)
        await message.answer("Не вдалося прочитати архів.", reply_markup=build_main_menu())
        return

    PENDING_PARTIAL_REQUESTS.pop(message.from_user.id, None)
    await send_selected_images_from_refs(image_refs, selected_indexes, message)


async def main():
    get_download_queue()
    logging.info("🚀 Бот запущено...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())