import sqlite3
import requests
import time
import sys
import os
from datetime import datetime
from pathlib import Path

# --- НАСТРОЙКИ ---
DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"

# API Ключи Кинопоиска
API_KEYS = [
    '1e727ee9-e29d-4188-9a80-230acb1938d2',
    '44a8186b-7220-4a99-93a8-37542881e847',
    '2ffed1fe-a3d8-4bf2-ac40-92f490467425',
    '67a7ed45-bbe2-4db8-80ad-8c6f21a8fcd5'
]

BATCH_LIMIT = 500  # Сколько фильмов перепроверить за раз
DELAY = 0.1        # Задержка между запросами

class KeyManager:
    def __init__(self, keys):
        self.keys = keys
        self.current_index = 0

    def get_header(self):
        return {
            'X-API-KEY': self.keys[self.current_index],
            'Content-Type': 'application/json',
        }

    def switch(self):
        old = self.current_index
        self.current_index += 1
        if self.current_index >= len(self.keys):
            return False
        print(f"\n⚠️ Ключ №{old + 1} исчерпан. Переключаюсь на ключ №{self.current_index + 1}...")
        return True

def search_kp(title, year, key_manager):
    url = 'https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword'
    params = {'keyword': title, 'page': 1}
    
    while True:
        try:
            response = requests.get(url, headers=key_manager.get_header(), params=params, timeout=10)
            
            # Обработка лимитов
            if response.status_code in [402, 429]:
                if key_manager.switch():
                    time.sleep(1)
                    continue 
                else:
                    print("\n❌ ВСЕ ключи исчерпаны! Завершение работы.")
                    sys.exit(0)

            if response.status_code != 200:
                return None

            data = response.json()
            films = data.get('films', [])
            
            if not films:
                return None

            target_year = int(year) if year else 0
            
            # Перебор результатов поиска
            for film in films:
                # Получаем год фильма
                film_year_str = str(film.get('year', '')).split('-')[0]
                if not film_year_str.isdigit():
                    continue
                film_year = int(film_year_str)
                
                # Допускаем разницу в 1 год (релиз в мире vs релиз в РФ)
                if abs(film_year - target_year) <= 1:
                    kp_id = film.get('filmId')
                    
                    # Сразу забираем рейтинг, раз уж нашли
                    rating_raw = film.get('rating')
                    votes = film.get('ratingVoteCount')
                    
                    rating = 0.0
                    if rating_raw:
                        if '%' in str(rating_raw):
                            rating = 0.0
                        else:
                            try:
                                rating = float(rating_raw)
                            except ValueError:
                                rating = 0.0
                    
                    # Возвращаем кортеж с данными
                    return (kp_id, rating, votes)
            
            return None 

        except requests.exceptions.RequestException:
            return None
        except Exception as e:
            print(f" Ошибка запроса: {e}")
            return None

def main():
    if not os.path.exists(os.path.dirname(DB_PATH)):
        print(f"Папка не найдена: {os.path.dirname(DB_PATH)}")
        return

    current_year = datetime.now().year
    print(f"🔎 Поиск пропущенных фильмов за {current_year} год...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ВЫБОРКА:
    # 1. Фильмы (media_type='movie')
    # 2. Текущий год
    # 3. kp_id равен NULL (никогда не искали) ИЛИ -1 (искали, но не нашли)
    # Сортируем по updated_at, чтобы сначала перепроверять те, что давно не трогали
    cursor.execute("""
        SELECT id, title, year, kp_id 
        FROM items_minimal 
        WHERE year = ? 
          AND media_type = 'movie'
          AND (kp_id IS NULL OR kp_id = -1)
        ORDER BY updated_at ASC
        LIMIT ?
    """, (current_year, BATCH_LIMIT))
    
    movies = cursor.fetchall()
    total = len(movies)
    
    if total == 0:
        print("✅ Нет фильмов для перепроверки (все либо найдены, либо база пуста).")
        return

    print(f"В очереди на перепроверку: {total}")
    key_manager = KeyManager(API_KEYS)

    found_count = 0
    still_missing_count = 0

    for i, (tmdb_id, title, year, old_kp_id) in enumerate(movies):
        status_prefix = "RETRY" if old_kp_id == -1 else "NEW"
        sys.stdout.write(f"\r[{i+1}/{total}] [{status_prefix}] {title} ({year}) -> ")
        sys.stdout.flush()

        result = search_kp(title, year, key_manager)
        
        # Обновляем поле updated_at в любом случае, чтобы этот фильм ушел в конец очереди
        # и мы не проверяли его снова через 5 минут
        current_time = datetime.now().isoformat()

        if result:
            kp_id, rating, votes = result
            votes = votes if votes else 0
            
            cursor.execute("""
                UPDATE items_minimal 
                SET kp_id = ?, kp_rating = ?, kp_vote_count = ?, updated_at = ?
                WHERE id = ?
            """, (kp_id, rating, votes, current_time, tmdb_id))
            
            found_count += 1
            sys.stdout.write(f"✅ НАЙДЕН! ID: {kp_id}")
        else:
            # Если не нашли - ставим (или обновляем) -1 и время
            cursor.execute("""
                UPDATE items_minimal 
                SET kp_id = -1, updated_at = ?
                WHERE id = ?
            """, (current_time, tmdb_id))
            
            still_missing_count += 1
            sys.stdout.write("❌ Не найден")

        conn.commit()
        time.sleep(DELAY)

    conn.close()
    print("\n" + "-" * 50)
    print(f"Итог перепроверки:")
    print(f"🎉 Найдено (восстановлено): {found_count}")
    print(f"💨 Всё ещё не найдено: {still_missing_count}")

if __name__ == '__main__':
    main()
