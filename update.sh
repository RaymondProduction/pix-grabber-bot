#!/bin/bash
git pull origin main
source venv/bin/activate && pip install -r requirements.txt
sudo systemctl restart pixgrabber-bot.service
tail -f /var/log/pix-grabber-bot.log
