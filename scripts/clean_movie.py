import sqlite3
import os
import re
from pathlib import Path

# --- КОНФИГУРАЦИЯ ---
BASE_DIR = Path(os.getcwd())
TORRENTS_DB_PATH = BASE_DIR / "tmdb_data" / "torrents.db"
DATA_DB_PATH = BASE_DIR / "tmdb_data" / "torrents_data.db"

def extract_hash(magnet_link):
    """Извлекает хеш из магнет-ссылки"""
    if not magnet_link:
        return None
    match = re.search(r'btih:([a-zA-Z0-9]{40})', magnet_link, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None

def clean_movie_data(tmdb_id):
    if not os.path.exists(TORRENTS_DB_PATH) or not os.path.exists(DATA_DB_PATH):
        print("❌ Ошибка: Файлы баз данных не найдены.")
        return

    print(f"🔍 Поиск данных для TMDB ID: {tmdb_id}...")

    # 1. Подключаемся к базе торрентов
    conn_torrents = sqlite3.connect(TORRENTS_DB_PATH)
    cursor_torrents = conn_torrents.cursor()

    # 2. Находим все магнет-ссылки для этого фильма
    cursor_torrents.execute("SELECT magnet FROM torrents WHERE tmdb_id = ?", (tmdb_id,))
    rows = cursor_torrents.fetchall()

    if not rows:
        print(f"⚠️ Торренты для TMDB ID {tmdb_id} не найдены в torrents.db.")
        conn_torrents.close()
        return

    # 3. Извлекаем хеши
    hashes_to_delete = []
    for row in rows:
        magnet = row[0]
        info_hash = extract_hash(magnet)
        if info_hash:
            hashes_to_delete.append(info_hash)

    print(f"   ∟ Найдено {len(rows)} торрентов (хешей: {len(hashes_to_delete)})")

    # 4. Удаляем детали из torrents_data.db (по хешам)
    deleted_details_count = 0
    if hashes_to_delete:
        try:
            conn_data = sqlite3.connect(DATA_DB_PATH)
            # Включаем WAL для надежности
            conn_data.execute("PRAGMA journal_mode = WAL;")
            
            # Формируем SQL запрос с IN (?, ?, ?)
            placeholders = ','.join('?' * len(hashes_to_delete))
            sql = f"DELETE FROM torrent_details WHERE info_hash IN ({placeholders})"
            
            cursor_data = conn_data.execute(sql, tuple(hashes_to_delete))
            deleted_details_count = cursor_data.rowcount
            conn_data.commit()
            conn_data.close()
        except Exception as e:
            print(f"❌ Ошибка при удалении из torrents_data.db: {e}")

    # 5. Удаляем записи из torrents.db (по ID)
    cursor_torrents.execute("DELETE FROM torrents WHERE tmdb_id = ?", (tmdb_id,))
    deleted_torrents_count = cursor_torrents.rowcount
    conn_torrents.commit()
    conn_torrents.close()

    print(f"✅ Успешно удалено:")
    print(f"   - Из списка торрентов (torrents.db): {deleted_torrents_count} записей")
    print(f"   - Из метаданных (torrents_data.db): {deleted_details_count} записей")

def main():
    print("--- ОЧИСТКА ДАННЫХ О ФИЛЬМЕ ---")
    while True:
        user_input = input("\nВведите TMDB ID фильма (или 'q' для выхода): ").strip()
        
        if user_input.lower() in ['q', 'exit', 'quit']:
            break
        
        if not user_input.isdigit():
            print("❌ Пожалуйста, введите числовой ID.")
            continue
            
        tmdb_id = int(user_input)
        clean_movie_data(tmdb_id)

if __name__ == "__main__":
    main()
