import asyncio
import json
import logging
import re
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ====================== КОНФІГ ======================
with open('config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

bot = Bot(
    token=CONFIG["telegram_token"],
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

BASE_DOWNLOAD_DIR = Path("images")
BASE_DOWNLOAD_DIR.mkdir(exist_ok=True)


def get_site_config(url: str):
    url_lower = url.lower()
    for domain, cfg in CONFIG.get("sites", {}).items():
        if domain in url_lower:
            return cfg
    return CONFIG.get("default", {})


def extract_resume_url(text: str) -> str | None:
    """Шукає посилання для продовження скачування"""
    # Шукаємо будь-яке http/https посилання в логах
    urls = re.findall(r'https?://[^\s<>"\']+', text)
    if urls:
        return urls[-1]  # беремо останнє (найімовірніше — для resume)
    return None


async def monitor_folder(folder: Path, message: types.Message, sent_files: set):
    """Моніторинг для режиму 'По одному'"""
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
                        logging.info(f"Відправлено: {file_path.name}")
                    except Exception as e:
                        logging.error(f"Помилка відправки {file_path.name}: {e}")
        except Exception as e:
            logging.error(f"Помилка в monitor_folder: {e}")


async def send_all_images_at_once(folder: Path, message: types.Message):
    """Відправляє всі картинки одразу"""
    images = sorted(
        [f for f in folder.rglob("*") if f.is_file() and f.suffix.lower() in {'.jpg','.jpeg','.png','.gif','.webp','.bmp'}],
        key=lambda x: x.stat().st_mtime
    )

    if not images:
        await message.answer("Не знайдено зображень.")
        return

    await message.answer(f"📤 Відправляю {len(images)} зображень...")

    for img in images:
        try:
            data = img.read_bytes()
            await message.answer_photo(
                types.BufferedInputFile(data, filename=img.name),
                caption=img.name
            )
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"Не вдалося відправити {img.name}: {e}")


async def create_and_send_zip(folder: Path, message: types.Message, url: str):
    """Виправлена функція створення та відправки ZIP"""
    images = [f for f in folder.rglob("*") if f.is_file() and f.suffix.lower() in {'.jpg','.jpeg','.png','.gif','.webp','.bmp'}]
    
    if not images:
        await message.answer("Не знайдено зображень для архіву.")
        return

    zip_path = folder / "gallery.zip"

    # Створюємо архів
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for img in images:
            zf.write(img, img.name)

    # Відправляємо архів правильно
    try:
        data = zip_path.read_bytes()
        await message.answer_document(
            types.BufferedInputFile(data, filename="gallery.zip"),
            caption=f"📦 Готово!\nЗображень: {len(images)}\n🔗 {url}"
        )
        logging.info(f"Архів відправлено: {len(images)} файлів")
    except Exception as e:
        logging.error(f"Помилка відправки ZIP: {e}")
        await message.answer(f"Не вдалося відправити архів: {e}")


async def handle_url(message: types.Message, url: str):
    cfg = get_site_config(url)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_dir = BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(url).netloc}"
    download_dir.mkdir(parents=True, exist_ok=True)

    await message.answer(f"🔄 Посилання прийнято: <b>{urlparse(url).netloc}</b>")

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 По одному (в процесі)", callback_data=f"mode:slow:{url}")],
        [InlineKeyboardButton(text="⚡ Швидко (всі в кінці)", callback_data=f"mode:fast:{url}")]
    ])

    await message.answer("Обери режим завантаження:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("mode:"))
async def process_mode(callback: types.CallbackQuery):
    _, mode, url = callback.data.split(":", 2)
    await callback.message.edit_text(f"✅ Обрано: {'По одному' if mode == 'slow' else 'Швидко'}\n\nПочинаю скачування...")

    cfg = get_site_config(url)
    username = cfg.get("username")
    password = cfg.get("password")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    download_dir = BASE_DOWNLOAD_DIR / f"{timestamp}_{urlparse(url).netloc}"
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

    if mode == "slow":
        sent_files = set()
        monitor_task = asyncio.create_task(monitor_folder(download_dir, callback.message, sent_files))

    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=1800)

    if mode == "slow":
        monitor_task.cancel()

    # === НОВА ЛОГІКА ОБРОБКИ ПОМИЛОК ===
    error_text = (stderr or b"").decode('utf-8', errors='ignore') + (stdout or b"").decode('utf-8', errors='ignore')

    resume_url = extract_resume_url(error_text)

    if process.returncode != 0 or resume_url:
        await callback.message.answer(
            f"⚠️ Скачування перервано або неповне.\n"
            f"Збережено те, що встигло."
        )
        if resume_url:
            await callback.message.answer(
                f"🔄 Можна продовжити з цього посилання:\n"
                f"<code>{resume_url}</code>\n\n"
                f"Просто надішли його мені ще раз."
            )

    # Завжди створюємо архів з тим, що є
    await create_and_send_zip(download_dir, callback.message, url)

    if mode == "fast":
        await send_all_images_at_once(download_dir, callback.message)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 <b>PixGrabber Bot</b>\n\n"
        "Надішли посилання — я запитаю режим скачування.\n"
        "Тепер при помилках показую лінк для продовження."
    )


@dp.message(F.text.regexp(r'^https?://\S+'))
async def on_link(message: types.Message):
    url = message.text.strip()
    asyncio.create_task(handle_url(message, url))


async def main():
    logging.info("🚀 Бот запущено...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())