import requests
import sqlite3
from pathlib import Path
import time
import math

# --- НАСТРОЙКИ ---
HOST = "cinetorrent.ru"
KEY = "E987654321CINEKEY"
KEY_LOCATION = f"https://{HOST}/{KEY}.txt"
DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"

# Размер пачки (IndexNow позволяет до 10k, но 2k безопаснее и стабильнее)
BATCH_SIZE = 2000

def get_all_slugs():
    print("📂 Читаем всю базу данных...")
    if not DB_PATH.exists():
        print("❌ База данных не найдена")
        return []

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Берем ВСЕ фильмы
    query = "SELECT id_slug FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0"
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    return [row[0] for row in rows]

def submit_batch(urls, batch_num, total_batches):
    endpoint = "https://yandex.com/indexnow"
    
    payload = {
        "host": HOST,
        "key": KEY,
        "keyLocation": KEY_LOCATION,
        "urlList": urls
    }

    print(f"🚀 Пачка {batch_num}/{total_batches}: Отправка {len(urls)} ссылок...")
    
    try:
        response = requests.post(endpoint, json=payload, timeout=30)
        
        if response.status_code == 200:
            print(f"   ✅ Успешно (200 OK)")
        elif response.status_code == 202:
            print(f"   ✅ Принято в обработку (202 Accepted)")
        else:
            print(f"   ❌ Ошибка: {response.status_code} - {response.text[:100]}")
            
    except Exception as e:
        print(f"   ❌ Ошибка соединения: {e}")

if __name__ == "__main__":
    print(f"--- MASS IndexNow Submitter для {HOST} ---")
    
    # 1. Получаем все слаги
    slugs = get_all_slugs()
    total_items = len(slugs)
    print(f"Всего фильмов в базе: {total_items}")
    
    if total_items == 0:
        exit()

    # 2. Добавляем главную страницу
    all_urls = [f"https://{HOST}/"] + [f"https://{HOST}/movies/{slug}" for slug in slugs]
    
    # 3. Разбиваем на пачки и отправляем
    total_batches = math.ceil(len(all_urls) / BATCH_SIZE)
    
    for i in range(total_batches):
        start = i * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_urls = all_urls[start:end]
        
        submit_batch(batch_urls, i + 1, total_batches)
        
        # Небольшая пауза между запросами, чтобы не спамить сервер Яндекса
        if i < total_batches - 1:
            time.sleep(2)

    print("\n🏁 Готово! Все ссылки отправлены в Яндекс.")
