import asyncio
import json
import logging
import re
from typing import Optional
from pathlib import Path
import zipfile
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

HISTORY_FILE = Path("history.json")
PENDING_PARTIAL_REQUESTS = {}


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
        "download_dir": download_dir
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


def get_site_config(url: str):
    url_lower = url.lower()
    for domain, cfg in CONFIG.get("sites", {}).items():
        if domain in url_lower:
            return cfg
    return CONFIG.get("default", {})


def extract_resume_url(text: str) -> Optional[str]:
    match = re.search(r"Use '([^']+)' as input URL to continue downloading", text)
    if match:
        return match.group(1)

    urls = re.findall(r'https?://[^\s<>"\']+', text)
    if urls:
        return urls[-1]
    return None


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Переглянути історію", callback_data="show_history")]
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
            if Path(name).suffix.lower() in {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
            and not name.endswith('/')
        ]


def build_history_actions_keyboard(index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Переслати архів", callback_data=f"resend:{index}")],
        [InlineKeyboardButton(text="🖼 Переслати частину фото", callback_data=f"partial:{index}")],
        [InlineKeyboardButton(text="⬅️ До історії", callback_data="show_history")]
    ])


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


def cleanup_images_after_zip(images: list[Path], zip_path: Path):
    for img in images:
        try:
            if img.exists() and img != zip_path:
                img.unlink()
        except Exception as e:
            logging.error(f"Не вдалося видалити файл {img}: {e}")


async def send_selected_images(zip_path: Path, image_names: list[str], selected_indexes: list[int], message: types.Message):
    selected_names = [image_names[i] for i in selected_indexes]

    with zipfile.ZipFile(zip_path, 'r') as zf:
        batch = []
        sent_count = 0

        for idx, image_name in enumerate(selected_names, start=1):
            data = zf.read(image_name)
            batch.append(
                types.InputMediaPhoto(
                    media=types.BufferedInputFile(data, filename=Path(image_name).name),
                    caption=f"🖼 {Path(image_name).name}" if len(batch) == 0 else None
                )
            )

            if len(batch) == 10 or idx == len(selected_names):
                await message.answer_media_group(batch)
                sent_count += len(batch)
                batch = []

        await message.answer(f"✅ Відправлено {sent_count} фото.", reply_markup=build_main_menu())


async def send_history(message: types.Message):
    history = load_history()
    if not history:
        await message.answer("Історія порожня.", reply_markup=build_main_menu())
        return

    buttons = []
    for i, entry in enumerate(reversed(history)):
        real_index = len(history) - 1 - i
        status = entry.get("status", "done")

        if status == "interrupted":
            marker = "⏸"
        elif Path(entry.get("zip_path", "")).exists():
            marker = "✅"
        elif status == "in_progress":
            marker = "🟡"
        else:
            marker = "❌"

        label = f"{marker} {entry['gallery_name'][:40]} ({entry['image_count']} шт.) — {entry['date']}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"history_item:{real_index}")])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_start")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer(f"📋 Історія завантажень ({len(history)} шт.):", reply_markup=keyboard)


async def monitor_folder(folder: Path, message: types.Message, sent_files: set):
    while True:
        await asyncio.sleep(2)
        try:
            files = sorted(
                [f for f in folder.rglob("*") if f.is_file()],
                key=lambda x: x.stat().st_mtime
            )
            for file_path in files:
                if file_path.suffix.lower() not in {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}:
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
              if f.is_file() and f.suffix.lower() in {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}]

    if not images:
        await message.answer("Не знайдено зображень для архіву.", reply_markup=build_main_menu())
        return

    gallery_folder = None
    for p in folder.rglob("*"):
        if p.is_dir():
            try:
                if any(img.is_file() and img.suffix.lower() in {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'} for img in p.iterdir()):
                    gallery_folder = p
                    break
            except Exception:
                continue

    if gallery_folder:
        gallery_name = gallery_folder.name
        safe_name = re.sub(r'[\\/*?:"<>|]', '_', gallery_name)[:120]
        zip_filename = f"{safe_name}.zip"
    else:
        gallery_name = folder.name
        zip_filename = f"{folder.name}.zip"

    zip_path = folder / zip_filename

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for img in images:
            zf.write(img, img.name)

    cleanup_images_after_zip(images, zip_path)

    update_history_entry(
        history_index,
        gallery_name=gallery_name,
        zip_path=str(zip_path),
        image_count=len(images),
        status="done",
        resume_url=""
    )

    try:
        data = zip_path.read_bytes()
        await message.answer_document(
            types.BufferedInputFile(data, filename=zip_filename),
            caption=f"📦 Готово!\nЗображень: {len(images)}\nНазва: {gallery_name}",
            reply_markup=build_main_menu()
        )
    except Exception as e:
        logging.error(f"Помилка відправки ZIP: {e}")
        await message.answer(f"Не вдалося відправити архів: {e}", reply_markup=build_main_menu())


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
        return

    await create_and_send_zip(download_dir, message, history_index)


async def handle_url(message: types.Message, url: str):
    await message.answer(f"🔄 Посилання прийнято: <b>{urlparse(url).netloc}</b>")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Грабуємо 👏🏽", callback_data=f"mode:new:{url}")],
        [InlineKeyboardButton(text="📋 Історія", callback_data="show_history")]
    ])
    await message.answer("Натисни щоб почати завантаження:", reply_markup=keyboard)


@dp.callback_query(F.data == "show_history")
async def show_history_callback(callback: types.CallbackQuery):
    await callback.answer()
    await send_history(callback.message)


@dp.callback_query(F.data == "back_to_start")
async def back_to_start_callback(callback: types.CallbackQuery):
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
    zip_path = Path(entry.get("zip_path", ""))
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

    if not zip_path.exists():
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
        reply_markup=build_history_actions_keyboard(index)
    )


@dp.callback_query(F.data.startswith("partial:"))
async def partial_resend_request(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    zip_path = Path(entry.get("zip_path", ""))

    if entry.get("status") == "interrupted" and entry.get("resume_url"):
        await callback.answer()
        await callback.message.answer(
            "⚠️ Це завантаження ще не завершене. Спочатку докачай його.",
            reply_markup=build_resume_keyboard(index)
        )
        return

    if not zip_path.exists():
        await callback.answer()
        await callback.message.answer(
            "⚠️ Архів недоступний або був видалений.\n"
            "Можна скачати його наново з цього посилання:",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    try:
        image_names = get_image_names_from_zip(zip_path)
    except Exception as e:
        logging.error(f"Не вдалося прочитати ZIP {zip_path}: {e}")
        await callback.answer("Не вдалося відкрити архів.", show_alert=True)
        return

    if not image_names:
        await callback.answer("У архіві немає зображень.", show_alert=True)
        return

    PENDING_PARTIAL_REQUESTS[callback.from_user.id] = {
        "history_index": index,
        "zip_path": str(zip_path),
        "image_count": len(image_names)
    }

    preview_count = min(10, len(image_names))
    preview_lines = [f"{i + 1}. {Path(image_names[i]).name}" for i in range(preview_count)]
    preview_text = "\n".join(preview_lines)

    await callback.answer()
    await callback.message.answer(
        f"🖼 <b>{entry['gallery_name']}</b>\n"
        f"В архіві <b>{len(image_names)}</b> фото.\n\n"
        f"Надішли номери фото або діапазон.\n"
        f"Приклади: <code>1-5</code>, <code>1,3,7</code>, <code>2-4,8</code>\n\n"
        f"Перші {preview_count} фото:\n{preview_text}"
    )


@dp.callback_query(F.data.startswith("resume:"))
async def resume_download(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    resume_url = entry.get("resume_url")
    download_dir_value = entry.get("download_dir")

    if not resume_url or not download_dir_value:
        await callback.answer("Немає даних для докачки.", show_alert=True)
        return

    await callback.answer()
    await callback.message.answer("⏩ Продовжую скачування з того самого місця...")

    update_history_entry(index, status="in_progress")

    await run_download(
        callback.message,
        resume_url,
        index,
        Path(download_dir_value)
    )


@dp.callback_query(F.data.startswith("mode:"))
async def process_mode(callback: types.CallbackQuery):
    _, action, url = callback.data.split(":", 2)
    await callback.message.edit_text("✅ Починаю скачування...")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_dir = BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(url).netloc}"
    history_index = add_history_entry(url, str(download_dir))

    await run_download(callback.message, url, history_index, download_dir)


@dp.callback_query(F.data.startswith("resend:"))
async def resend_zip(callback: types.CallbackQuery):
    index = int(callback.data.split(":", 1)[1])
    history = load_history()

    if index >= len(history):
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    entry = history[index]
    zip_path = Path(entry.get("zip_path", ""))

    await callback.answer()

    if entry.get("status") == "interrupted" and entry.get("resume_url"):
        await callback.message.answer(
            "⚠️ Це скачування ще не завершене. Можна докачати:",
            reply_markup=build_resume_keyboard(index)
        )
        return

    if not zip_path.exists():
        await callback.message.answer(
            "⚠️ Архів недоступний або був видалений.\n"
            "Можна скачати його наново з цього посилання:",
            reply_markup=build_redownload_keyboard(entry["url"])
        )
        return

    data = zip_path.read_bytes()
    await callback.message.answer_document(
        types.BufferedInputFile(data, filename=zip_path.name),
        caption=f"📦 {entry['gallery_name']}\n🖼 {entry['image_count']} зображень\n📅 {entry['date']}\n🔗 {entry['url']}",
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
    pending = PENDING_PARTIAL_REQUESTS.get(message.from_user.id)
    if not pending:
        return

    zip_path = Path(pending["zip_path"])
    if not zip_path.exists():
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
        image_names = get_image_names_from_zip(zip_path)
        selected_indexes = parse_photo_selection(message.text, len(image_names))
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
    await send_selected_images(zip_path, image_names, selected_indexes, message)


async def main():
    logging.info("🚀 Бот запущено...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
