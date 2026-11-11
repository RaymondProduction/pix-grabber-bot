# PixGrabber Bot

Telegram bot for downloading galleries using **gallery-dl** and social videos using **yt-dlp**, with download history.

## Features
- Download images from web sites
- Download Facebook, Instagram and TikTok videos with sound, with optional per-chat browser sessions
- Keep original videos separately on the same storage as images, without ZIP archives
- Convert Pixiv Ugoira animations to MP4 and send them as Telegram animations
- Two modes: real-time (one by one) and fast (all at once)
- Automatic ZIP archive creation with meaningful filename
- Download history with easy re-send
- Save unstarted links and download them together with `/grab_pending`
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

### Відкладені посилання

Посилання зберігаються одразу після отримання, ще до відповіді бота. Можна надсилати
кілька URL в одному повідомленні, розділяючи їх пробілами або новими рядками.

Якщо немає часу натискати **📸 Грабуємо**, пізніше надішліть **`/grab_pending`**
або натисніть **📥 Грабувати всі відкладені** в головному меню чи під новим посиланням.
Бот додасть відкладені посилання з поточного чату в чергу та завантажить їх по одному.
Повторне натискання не додає ті самі запити ще раз; для кожної галереї створюється окрема папка.

Посилання без натиснутої кнопки зберігаються в SQLite й переживають перезапуск.
Для Pixiv без авторизації вони залишаються відкладеними: після входу повторіть `/grab_pending`.
Окремі кнопки «Скачати наново» з історії не запускаються масово.

Збереження діє для посилань, отриманих після цього оновлення. Старі посилання,
які попередня версія не записала в базу, потрібно надіслати боту ще раз.

### Відео Facebook, Instagram та TikTok

Надішліть посилання на відео або Reel, зокрема коротке `fb.watch`, `vm.tiktok.com`
чи `vt.tiktok.com`, та натисніть **Грабуємо** або запустіть `/grab_pending`.
Бот завантажує відео зі звуком через `yt-dlp`, надсилає його окремо й залишає оригінал
на диску. **ZIP для цих відео не створюється.** Історія підтримує повторне надсилання
з Telegram або з диска, а перерване завантаження можна докачати.
Профілі Instagram і явні посилання на фото залишаються у `gallery-dl`;
якщо Instagram повідомляє, що в дописі `/p/` немає відео, бот завантажує його як галерею.
Анімації Pixiv і далі обробляються та архівуються разом із галереями.

Публічні відео бот спочатку пробує завантажити без входу. Якщо сайт вимагає сесію,
в особистому чаті відкрийте **Службове меню → Facebook / Instagram / TikTok**
або використайте `/facebook_login`, `/instagram_login`, `/tiktok_login`:

1. Увійдіть на потрібний сайт у браузері.
2. Експортуйте cookies цього сайту в текстовому форматі **Netscape**.
3. Після команди підключення надішліть файл боту як документ, до 256 КіБ.
4. Повторіть завантаження: **Докачати** в історії або `/grab_pending` для ще не запущених посилань.

Механізм відповідає імпорту браузерної сесії Facebook у наданому `ai-chat-main.zip`.
Пароль не потрібен. Для Facebook потрібні cookies `c_user` і `xs`, для Instagram —
`sessionid`, для TikTok — `sid_tt`, `sessionid` або `sessionid_ss`.
Бот відкидає cookies інших сайтів і прострочені записи; зберігає сесії окремо
для кожного особистого чату у `.video-auth/` (каталог `700`, файли `600`, виключено з Git).
Бот намагається видалити повідомлення з файлом. Cookies дають доступ до акаунта —
не публікуйте їх і не додавайте до репозиторію. Скасування імпорту: `/cancel`;
відключення акаунта — у тому самому службовому меню.

Формат і строк дії перевіряються локально; сайт може відхилити навіть коректний файл
через завершену сесію, перевірку входу, обмеження IP чи недоступність допису.
У такому разі увійдіть у браузері й імпортуйте свіжі cookies. Групові чати завантажують
анонімно. Відео та їхня історія доступні лише чату, який запустив завантаження.
Документація: [cookies у yt-dlp](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp).

#### Зберігання на SSD

Типовий шлях: `images/videos/<сервіс>/<chat_id>/<завантаження>/`.
Якщо `images` — символьне посилання на SSD, **відео та проміжні медіафайли автоматично
записуються на той самий SSD**. Окремого символьного посилання не потрібно.
Очищення робочих папок галерей не видаляє цей каталог; видалення запису історії також
залишає оригінали на диску. Файл `videos.json` у папці містить перелік завершених відео
для повторного надсилання після перезапуску; незавершені файли не надсилаються.

Шляхи та вільне місце видно в **Службове меню → Вільне місце на диску**.
Якщо символьне посилання не працює, завантажувач повідомить про недоступність сховища.
На сервері можна перевірити підключення командами:

```bash
readlink -f images
df -h images
```

За потреби параметр `video_download_dir` у `config.json` задає іншу папку;
для неї адміністратор сам обирає шлях на SSD. Решта необов’язкових налаштувань:

```json
{
  "video_download_dir": "images/videos",
  "video_timeout_sec": 1800,
  "video_stall_timeout_sec": 300,
  "max_video_download_size_mb": 2048,
  "max_video_upload_size_mb": 49
}
```

Завантажувач обмежує кількість елементів одного запиту до 10, розмір завантажуваного
потоку — `max_video_download_size_mb`, а сумарні робочі файли — утричі більшим значенням.
Типовий ліміт надсилання — 49 МіБ, із запасом до
[ліміту стандартного Telegram Bot API](https://core.telegram.org/bots/api#sendvideo).
Більше відео зберігається на SSD: бот повідомляє шлях і розмір, не архівує й не видаляє файл.

На сервері встановіть залежності й FFmpeg, потім перезапустіть службу:

```bash
source venv/bin/activate
pip install -r requirements.txt
sudo apt install ffmpeg
sudo systemctl restart pixgrabber-bot.service
```

`curl-cffi` встановлюється разом із `yt-dlp` для сумісності з HTTP-запитами соцмереж.
Якщо сайт змінився, оновіть завантажувач: `pip install -U 'yt-dlp[curl-cffi]'`.
Перевірки `python -m unittest discover -s tests -v` охоплюють сесії, чергу, історію,
символьне посилання, відсутність ZIP, помилки й таймаути. За наявності FFmpeg тест також
завантажує локальне MP4 через справжній `yt-dlp` і перевіряє збереження звуку.
Живі приватні відео потребують власної сесії користувача; автоматичні тести її не використовують.

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
