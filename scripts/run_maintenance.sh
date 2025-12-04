#!/bin/bash

# Останавливать скрипт при любой ошибке
set -e

# Переходим в папку проекта
cd /home/niki/projects/torrent

# Активируем virtualenv (если используете)
# source venv/bin/activate

echo "=========================================="
echo "STARTING MAINTENANCE: $(date)"
echo "=========================================="

# 1. Сначала полностью отрабатывает Python (скачивает и парсит)
echo ">> [1/2] Running Python Updater (2025 movies)..."
/usr/bin/python3 scripts/auto_update_2025.py

# 2. Только когда Python завершился, запускается Node (чистит WAL файлы)
echo ">> [2/2] Running DB Optimization..."
/usr/bin/node scripts/optimize_all.js

echo "=========================================="
echo "SUCCESS: $(date)"
echo "=========================================="
