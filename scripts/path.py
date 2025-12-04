#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_poster_paths.py

Скрипт для массового обновления путей к постерам в БД.
Меняет путь на: /home/niki/projects/torrent/posters/<имя_файла>
"""

import sqlite3
import os
import logging
from pathlib import Path

# --- НАСТРОЙКИ ---
DB_PATH = Path("tmdb_data/tmdb_minimal_no_original.db")
# Новый путь, где теперь лежат постеры
NEW_DIR = Path("/home/niki/projects/torrent/posters")
TABLE_NAME = "items_minimal"
COLUMN_NAME = "local_poster_path"  # Название колонки в БД

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_paths():
    if not DB_PATH.exists():
        logging.error(f"База данных не найдена: {DB_PATH}")
        return

    # Создаем папку, если вдруг скрипт запускается, а папки еще нет (опционально)
    if not NEW_DIR.exists():
        logging.warning(f"Внимание! Новая папка {NEW_DIR} физически не существует.")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        logging.info("Чтение текущих путей из БД...")
        cursor.execute(f"SELECT id, {COLUMN_NAME} FROM {TABLE_NAME} WHERE {COLUMN_NAME} IS NOT NULL AND {COLUMN_NAME} != ''")
        rows = cursor.fetchall()
        
        updates = []
        
        for item_id, old_path_str in rows:
            # Получаем чистое имя файла (например, из "/files/posters/100.jpg" получаем "100.jpg")
            filename = os.path.basename(old_path_str)
            
            # Формируем новый полный путь
            # new_path будет: /home/niki/projects/torrent/posters/100.jpg
            new_path_str = str(NEW_DIR / filename)
            
            # Если путь реально изменился, добавляем в список на обновление
            if new_path_str != old_path_str:
                updates.append((new_path_str, item_id))

        if not updates:
            logging.info("Нет записей, требующих обновления (пути уже совпадают).")
            return

        logging.info(f"Подготовлено {len(updates)} записей к обновлению.")
        
        # Массовое обновление (быстро)
        logging.info("Применение изменений в БД...")
        cursor.executemany(f"UPDATE {TABLE_NAME} SET {COLUMN_NAME} = ? WHERE id = ?", updates)
        
        conn.commit()
        logging.info("Успешно обновлено!")

        # Проверка (выведем первый обновленный путь для примера)
        cursor.execute(f"SELECT {COLUMN_NAME} FROM {TABLE_NAME} WHERE id = ?", (updates[0][1],))
        example = cursor.fetchone()[0]
        logging.info(f"Пример нового пути: {example}")

    except sqlite3.OperationalError as e:
        logging.error(f"Ошибка БД. Возможно, неверное имя таблицы или колонки. Детали: {e}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    update_paths()
