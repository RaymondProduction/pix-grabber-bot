#!/bin/bash
# ================================================
# PixGrabber Bot - Automatic Installer
# ================================================

set -e  # Stop script on error

echo "========================================"
echo "   PixGrabber Bot - Installer"
echo "========================================"

# ====================== CONFIGURATION ======================
BOT_DIR="$(pwd)"
BOT_NAME="pixgrabber-bot"
SERVICE_NAME="${BOT_NAME}.service"
LOG_FILE="/var/log/${BOT_NAME}.log"

echo "Bot directory: $BOT_DIR"

# Check if venv exists, create if not
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating venv and updating packages..."
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade aiogram gallery-dl

# Check gallery-dl version
echo "gallery-dl version:"
gallery-dl --version

# ====================== CREATE SYSTEMD SERVICE ======================
echo "Creating systemd service..."

cat > /tmp/${SERVICE_NAME} << EOF
[Unit]
Description=PixGrabber Telegram Bot (gallery-dl downloader)
After=network.target
Wants=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=${BOT_DIR}
ExecStart=/bin/bash -c 'source ${BOT_DIR}/venv/bin/activate && ${BOT_DIR}/venv/bin/python ${BOT_DIR}/bot.py'
Restart=always
RestartSec=5
StandardOutput=append:${LOG_FILE}
StandardError=append:${LOG_FILE}
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

sudo mv /tmp/${SERVICE_NAME} /etc/systemd/system/${SERVICE_NAME}
sudo chown root:root /etc/systemd/system/${SERVICE_NAME}
sudo chmod 644 /etc/systemd/system/${SERVICE_NAME}

# ====================== ENABLE AND START SERVICE ======================
echo "Enabling and starting the service..."

sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}
sudo systemctl start ${SERVICE_NAME}

# Create log file with correct permissions
sudo touch ${LOG_FILE}
sudo chown $(whoami):$(whoami) ${LOG_FILE}
sudo chmod 644 ${LOG_FILE}

echo "========================================"
echo "✅ Installation completed successfully!"
echo ""
echo "Service:      ${SERVICE_NAME}"
echo "Bot directory: ${BOT_DIR}"
echo "Log file:     ${LOG_FILE}"
echo ""
echo "Useful commands:"
echo "   sudo systemctl status ${SERVICE_NAME}     # check status"
echo "   sudo systemctl restart ${SERVICE_NAME}    # restart bot"
echo "   sudo systemctl stop ${SERVICE_NAME}       # stop bot"
echo "   tail -f ${LOG_FILE}                       # view live logs"
echo ""
echo "The bot will start automatically on system boot."
echo "========================================"