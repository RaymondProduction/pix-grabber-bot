# PixGrabber Bot

Telegram bot for downloading galleries using **gallery-dl** with automatic archiving and history.

## Features
- Download images from web sites
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