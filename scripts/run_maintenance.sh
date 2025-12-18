#!/bin/bash

# Останавливать скрипт при любой ошибке (глобально)
set -e

# Переходим в папку проекта
cd /home/niki/projects/torrent

# Активируем virtualenv
source venv/bin/activate

echo "=========================================="
echo "STARTING MAINTENANCE: $(date)"
echo "=========================================="

# 1. Python (если он упадет, скрипт остановится тут и не тронет сервис - это ХОРОШО)
echo ">> [1/2] Running Python Updater (2025 movies)..."
python3 scripts/auto_update_2025.py

echo ">> Stopping cinetorrent service..."
sudo systemctl stop cinetorrent

# 2. Оптимизация БД
echo ">> [2/2] Running DB Optimization..."

# --- БЕЗОПАСНЫЙ БЛОК ---
set +e  # Временно разрешаем ошибки
/usr/bin/node scripts/optimize_all.js
NODE_EXIT_CODE=$? # Запоминаем код возврата, если нужно логировать
set -e  # Включаем строгий режим обратно
# -----------------------

echo ">> Starting cinetorrent service..."
# Запускаем сервис обратно в любом случае
sudo systemctl start cinetorrent

if [ $NODE_EXIT_CODE -ne 0 ]; then
    echo "!! WARNING: DB Optimization finished with errors, but service was restarted."
else
    echo "=========================================="
    echo "SUCCESS: $(date)"
    echo "=========================================="
fi
