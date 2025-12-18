import sqlite3
import os
from pathlib import Path

# Настройки путей (как в твоем основном скрипте)
BASE_DIR = Path(os.getcwd())
TMDB_DB_PATH = BASE_DIR / "tmdb_data" / "tmdb_minimal_no_original.db"

def init_database_column():
    print(f"Подключение к БД: {TMDB_DB_PATH}")
    
    try:
        with sqlite3.connect(TMDB_DB_PATH) as conn:
            cursor = conn.cursor()
            
            # 1. Пытаемся добавить колонку. Если она есть — игнорируем ошибку.
            try:
                print("Создание колонки updated_at...")
                cursor.execute("ALTER TABLE items_minimal ADD COLUMN updated_at TEXT")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    print("Колонка updated_at уже существует.")
                else:
                    raise e

            # 2. Обновляем ВСЕ записи на дату 2025-12-04
            print("Установка даты '2025-12-04' для всех записей...")
            cursor.execute("UPDATE items_minimal SET updated_at = '2025-12-04'")
            
            conn.commit()
            print(f"Успешно обновлено {cursor.rowcount} строк.")
            
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    init_database_column()
