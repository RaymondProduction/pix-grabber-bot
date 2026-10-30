# PixGrabber Bot

Telegram bot for downloading galleries using **gallery-dl** with automatic archiving and history.

## Features
- Download images from web sites
- Convert Pixiv Ugoira animations to MP4 and send them as Telegram animations
- Two modes: real-time (one by one) and fast (all at once)
- Automatic ZIP archive creation with meaningful filename
- Download history with easy re-send
- Resume support when download is interrupted
- Runs as systemd service

## Installation

```bash
git clone <your-repo-url>
cd pix-grabber-bot

# Run installer
./install.sh
```

## Management Commands

```bash
# Check status
sudo systemctl status pixgrabber-bot

# Restart bot
sudo systemctl restart pixgrabber-bot

# View logs in real time
tail -f /var/log/pix-grabber-bot.log

# Stop bot
sudo systemctl stop pixgrabber-bot
```

## Usage

Send any gallery URL to the bot
Choose mode:
**📸 По одному**— images sent as they are downloaded
**⚡ Швидко** — all images sent at the end

Use **/history** to see previous downloads and re-send archives

### Вхід у Pixiv через Telegram

В особистому чаті надішліть `/pixiv_login` або відкрийте **Службове меню → Підключити Pixiv**.
Якщо надіслати посилання Pixiv без збереженої авторизації, бот сам запропонує вхід.

1. Відкрийте нове посилання від бота у браузері на комп’ютері.
2. До входу відкрийте інструменти розробника (F12), вкладку **Мережа / Network**.
3. Увійдіть у Pixiv та знайдіть останній запит `callback?state=…`.
4. Протягом приблизно **30 секунд після входу** надішліть боту значення `code` або повну адресу callback.

Бот підтвердить вхід українською й поверне до початкового посилання. Для скасування є `/cancel`.
Якщо код прострочився або бот перезапустився, почніть вхід знову: код має належати саме поточному посиланню від бота.

Браузер і сервер бота можуть бути на різних IP: PKCE використовує ключ сесії, збережений ботом.
Мережеві обмеження Pixiv для IP сервера все одно можуть перешкодити запиту.
Див. [PKCE](https://www.rfc-editor.org/rfc/rfc7636) та [авторизацію gallery-dl](https://gdl-org.github.io/docs/configuration.html#extractor-pixiv-refresh-token).

Авторизація зберігається окремо для кожного особистого чату у `.pixiv-auth/`, з правами `700` на каталог і `600` на файли.
Каталог виключений із Git. Бот намагається видалити отримане повідомлення з кодом; токен у чат не надсилається.
Завантаження Pixiv отримують відповідний файл через `--config`; шаблонні логін і пароль із `sites.pixiv.net` для них не використовуються.
Кеш авторизації `gallery-dl` для цих запусків працює в пам’яті, щоб не змішувати облікові записи.

Для цієї функції `gallery-dl` має бути встановлений у тому самому Python-середовищі, що й бот: він включений у `requirements.txt`.

Перевірки без звернення до Telegram або Pixiv:

```bash
python -m unittest discover -s tests -v
```

### Анімації Pixiv (Ugoira)

Бот завантажує ZIP із кадрами та перетворює його на MP4 (H.264 без звуку) через FFmpeg.
Готова анімація надсилається в Telegram, потрапляє в ZIP-архів та доступна для повторного надсилання з історії.
Одна анімація рахується як один файл. Незавершені MP4 не надсилаються; при помилці конвертації джерело залишається на сервері.

На сервері потрібні `ffmpeg` і `ffprobe` у `PATH` (Debian/Ubuntu: `sudo apt install ffmpeg`).
Для Pixiv таймаут без активності за замовчуванням становить 300 секунд; його можна змінити через `pixiv_stall_timeout_sec` у `config.json`.
Перевірка враховує зміну розміру тимчасових файлів і активність конвертації. Загальний ліміт однієї спроби — 30 хвилин.

### Configuration
Edit config.json:
```json
JSON{
  "telegram_token": "YOUR_BOT_TOKEN_HERE",
  "sites": {
    "e-hentai.org": {
      "username": "your_login",
      "password": "your_password"
    }
  }
}
```

**Note: Add config.json to .gitignore for security.**

### Logs
All output is written to:
/var/log/pix-grabber-bot.log
