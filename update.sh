#!/bin/bash
git pull origin main
sudo systemctl restart pixgrabber-bot.service
tail -f /var/log/pix-grabber-bot.log
