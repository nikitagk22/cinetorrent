import sqlite3
import requests
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# --- НАСТРОЙКИ ---
TMDB_API_KEY = "ba43a97bbcb31fb56b46b2966249ab8d" 
DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"
MAX_WORKERS = 30  # Количество потоков

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(items_minimal)")
    columns = [info[1] for info in cursor.fetchall()]
    
    if "trailer_key" not in columns:
        print("🛠 Добавляем колонку trailer_key...")
        cursor.execute("ALTER TABLE items_minimal ADD COLUMN trailer_key TEXT")
        conn.commit()
    
    conn.close()

def get_movies_without_trailer():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Берем те, где трейлера нет (NULL), исключая те, где мы уже искали и не нашли (например, пометим как 'none')
    cursor.execute("SELECT id FROM items_minimal WHERE trailer_key IS NULL")
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]

def fetch_trailer(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/videos"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "ru-RU" # Ищем строго на русском
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            
            # ЛОГИКА ПОИСКА:
            # 1. Ищем видео с type="Trailer" и site="YouTube"
            # 2. Если нет трейлера, можно взять "Teaser" (по желанию)
            
            trailer = next((v for v in results if v["site"] == "YouTube" and v["type"] == "Trailer"), None)
            
            if not trailer:
                # Если нет трейлера, ищем тизер
                trailer = next((v for v in results if v["site"] == "YouTube" and v["type"] == "Teaser"), None)
            
            if trailer:
                return tmdb_id, trailer["key"]
            else:
                return tmdb_id, "none" # Не нашли на русском
        
        elif resp.status_code == 404:
            return tmdb_id, "none" # Фильма нет
            
    except Exception:
        return None
    
    return None

def update_db_batch(updates):
    if not updates: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany("UPDATE items_minimal SET trailer_key = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()

def main():
    if TMDB_API_KEY == "ВАШ_API_KEY_ЗДЕСЬ":
        print("❌ Вставьте API Key!")
        return

    print("🚀 Поиск русских трейлеров...")
    init_db()
    
    movie_ids = get_movies_without_trailer()
    total = len(movie_ids)
    print(f"📥 Очередь на проверку: {total}")
    
    if total == 0: return

    batch_updates = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(fetch_trailer, mid): mid for mid in movie_ids}
        
        with tqdm(total=total) as pbar:
            for future in as_completed(future_to_id):
                result = future.result()
                if result:
                    tmdb_id, key = result
                    batch_updates.append((key, tmdb_id))
                
                if len(batch_updates) >= 100:
                    update_db_batch(batch_updates)
                    batch_updates = []
                
                pbar.update(1)

    if batch_updates:
        update_db_batch(batch_updates)

    print("\n🎉 Готово! Трейлеры добавлены.")

if __name__ == "__main__":
    main()
