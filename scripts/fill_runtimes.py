import sqlite3
import requests
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

# --- НАСТРОЙКИ ---
# Вставьте сюда ваш ключ API TMDB
TMDB_API_KEY = "ba43a97bbcb31fb56b46b2966249ab8d" 

# Путь к базе данных
DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"

# Количество потоков (можно ставить 20-30, TMDB держит)
MAX_WORKERS = 25

# -------------------------------------------------------------------

def init_db():
    """Создает колонку runtime, если её нет."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Проверяем колонки
    cursor.execute("PRAGMA table_info(items_minimal)")
    columns = [info[1] for info in cursor.fetchall()]
    
    if "runtime" not in columns:
        print("🛠 Добавляем колонку runtime...")
        cursor.execute("ALTER TABLE items_minimal ADD COLUMN runtime INTEGER DEFAULT 0")
        conn.commit()
    
    conn.close()

def get_movies_without_runtime():
    """Получает список ID фильмов, где runtime пустой или 0."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM items_minimal WHERE runtime IS NULL OR runtime = 0")
    rows = cursor.fetchall()
    conn.close()
    
    return [row[0] for row in rows]

def fetch_runtime(tmdb_id):
    """Запрашивает длительность фильма у TMDB."""
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "ru-RU" # Нам нужен только runtime, язык не важен, но пусть будет RU
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            runtime = data.get("runtime", 0)
            return tmdb_id, runtime
        
        elif resp.status_code == 404:
            # Фильма нет - возвращаем -1, чтобы пометить как несуществующий
            return tmdb_id, -1
            
        elif resp.status_code == 429:
            # Лимит запросов - спим и пробуем (хотя в тредах это сложно, просто вернем None)
            time.sleep(2)
            return None
            
    except Exception:
        return None
        
    return None

def update_db_batch(updates):
    """Обновляет базу данных пачкой (транзакция)."""
    if not updates:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.executemany("UPDATE items_minimal SET runtime = ? WHERE id = ?", updates)
    
    conn.commit()
    conn.close()

def main():
    if TMDB_API_KEY == "ВАШ_API_KEY_ЗДЕСЬ":
        print("❌ ОШИБКА: Вставьте API Key в начале файла!")
        return

    if not DB_PATH.exists():
        print(f"❌ ОШИБКА: База {DB_PATH} не найдена.")
        return

    print("🚀 Запуск скрипта обновления длительности (Python Multithreaded)...")
    
    init_db()
    
    movie_ids = get_movies_without_runtime()
    total = len(movie_ids)
    print(f"📥 Найдено фильмов для обновления: {total}")
    
    if total == 0:
        print("✨ Все данные уже заполнены.")
        return

    # Пакет для записи в БД (будем писать каждые 100 фильмов)
    batch_updates = []
    BATCH_SIZE = 100
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Создаем задачи
        future_to_id = {executor.submit(fetch_runtime, mid): mid for mid in movie_ids}
        
        # Прогресс-бар
        with tqdm(total=total, unit="movie") as pbar:
            for future in as_completed(future_to_id):
                result = future.result()
                
                if result:
                    tmdb_id, runtime = result
                    # Добавляем в пакет (runtime, id) - порядок для SQL UPDATE
                    batch_updates.append((runtime, tmdb_id))
                
                # Если набрали пакет - пишем в базу
                if len(batch_updates) >= BATCH_SIZE:
                    update_db_batch(batch_updates)
                    batch_updates = []
                
                pbar.update(1)

    # Дописываем остатки
    if batch_updates:
        update_db_batch(batch_updates)

    print("\n🎉 Готово! База данных обновлена.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Остановлено пользователем.")
