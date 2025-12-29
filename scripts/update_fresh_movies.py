import sqlite3
import requests
import time
import sys
import os
from datetime import datetime
from pathlib import Path

# --- КОНФИГУРАЦИЯ ---

# Пути
DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"

# TMDB Настройки
TMDB_PROXY_BASE = 'https://tmdb.golik-niki.workers.dev/3'
TMDB_API_KEY = 'ba43a97bbcb31fb56b46b2966249ab8d'

# Кинопоиск API Ключи (из твоего списка)
KP_API_KEYS = [
    '1e727ee9-e29d-4188-9a80-230acb1938d2',
    '44a8186b-7220-4a99-93a8-37542881e847',
    '2ffed1fe-a3d8-4bf2-ac40-92f490467425',
    '67a7ed45-bbe2-4db8-80ad-8c6f21a8fcd5'
]

# Настройки парсинга
CURRENT_KP_KEY_INDEX = 0
DELAY_TMDB = 0.01
DELAY_KP = 0.01
BATCH_LIMIT = 2000  # Сколько фильмов обработать за один запуск (чтобы не убить ключи)

# --- КЛАСС ДЛЯ РАБОТЫ С КИНОПОИСКОМ ---
class KpUpdater:
    def __init__(self):
        self.key_index = 0
    
    def get_headers(self):
        return {
            'X-API-KEY': KP_API_KEYS[self.key_index],
            'Content-Type': 'application/json',
        }

    def switch_key(self):
        old = self.key_index
        self.key_index += 1
        if self.key_index >= len(KP_API_KEYS):
            return False
        print(f"\n⚠️ KP: Ключ №{old + 1} исчерпан. Переход на №{self.key_index + 1}...")
        return True

    def _request(self, url, params=None):
        while True:
            try:
                response = requests.get(url, headers=self.get_headers(), params=params, timeout=10)
                
                # Лимиты (402 - Payment Required, 429 - Too Many Requests)
                if response.status_code in [402, 429]:
                    if self.switch_key():
                        time.sleep(1)
                        continue
                    else:
                        print("\n❌ KP: Все ключи исчерпаны!")
                        raise Exception("All KP keys exhausted")
                
                if response.status_code == 404:
                    return None
                    
                if response.status_code != 200:
                    # Другие ошибки
                    return None

                return response.json()
            except requests.exceptions.RequestException:
                return None

    def get_details_by_id(self, kp_id):
        """Получает свежие данные, если ID уже известен"""
        url = f'https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}'
        data = self._request(url)
        if data:
            rating = data.get('ratingKinopoisk')
            votes = data.get('ratingKinopoiskVoteCount')
            return (kp_id, rating, votes)
        return None

    def search_by_title(self, title, year):
        """Ищет фильм, если ID нет"""
        url = 'https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword'
        params = {'keyword': title, 'page': 1}
        data = self._request(url, params)
        
        if not data or 'films' not in data:
            return None

        target_year = int(year) if year else 0
        
        for film in data['films']:
            # Проверка года
            f_year_str = str(film.get('year', '')).split('-')[0]
            if not f_year_str.isdigit():
                continue
            f_year = int(f_year_str)
            
            # Допускаем погрешность в 1 год
            if abs(f_year - target_year) <= 1:
                kp_id = film.get('filmId')
                rating_raw = film.get('rating')
                votes = film.get('ratingVoteCount')
                
                # Чистим рейтинг (бывает в %, бывает null)
                rating = 0.0
                if rating_raw:
                    if '%' in str(rating_raw):
                        rating = 0.0
                    else:
                        try:
                            rating = float(rating_raw)
                        except ValueError:
                            rating = 0.0
                            
                return (kp_id, rating, votes)
        return None

# --- ФУНКЦИИ TMDB ---
def get_tmdb_data(tmdb_id, media_type='movie'):
    """Получает данные с TMDB через прокси"""
    # Если вдруг в базе есть сериалы, меняем эндпоинт
    endpoint = 'movie' if media_type == 'movie' else 'tv'
    url = f"{TMDB_PROXY_BASE}/{endpoint}/{tmdb_id}"
    params = {
        'api_key': TMDB_API_KEY,
        'language': 'ru-RU' # Получаем данные, актуальные для ру-региона (но голоса общие)
    }
    
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return {
                'vote_average': data.get('vote_average', 0),
                'vote_count': data.get('vote_count', 0)
            }
    except Exception as e:
        print(f"TMDB Error: {e}")
    return None

# --- MAIN ---
def main():
    if not os.path.exists(os.path.dirname(DB_PATH)):
        print(f"❌ Ошибка: Папка БД не найдена: {os.path.dirname(DB_PATH)}")
        return

    current_year = datetime.now().year
    print(f"📅 Обновляем фильмы за {current_year} год...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Выбираем фильмы текущего года
    # Исключаем те, где kp_id = -1 (значит уже искали и не нашли)
    # media_type='movie' чтобы не ломать логику сериалами, если они есть
    cursor.execute("""
        SELECT id, title, year, kp_id, media_type
        FROM items_minimal 
        WHERE year = ? 
          AND media_type = 'movie'
          AND (kp_id != -1 OR kp_id IS NULL)
        ORDER BY updated_at ASC
        LIMIT ?
    """, (current_year, BATCH_LIMIT))
    
    movies = cursor.fetchall()
    total = len(movies)
    print(f"🔍 Найдено {total} фильмов для обновления.")

    kp_updater = KpUpdater()
    stats = {'tmdb_ok': 0, 'kp_ok': 0, 'kp_not_found': 0, 'errors': 0}

    for i, row in enumerate(movies):
        tmdb_id = row['id']
        title = row['title']
        kp_id = row['kp_id']
        
        print(f"\n[{i+1}/{total}] {title} (ID: {tmdb_id})")

        # --- ШАГ 1: TMDB Update ---
        tmdb_res = get_tmdb_data(tmdb_id)
        if tmdb_res:
            cursor.execute("""
                UPDATE items_minimal 
                SET vote_average = ?, vote_count = ?, updated_at = ?
                WHERE id = ?
            """, (tmdb_res['vote_average'], tmdb_res['vote_count'], datetime.now().isoformat(), tmdb_id))
            print(f"   ✅ TMDB: {tmdb_res['vote_average']} ({tmdb_res['vote_count']} голосов)")
            stats['tmdb_ok'] += 1
        else:
            print("   ⚠️ TMDB: Ошибка получения")
            stats['errors'] += 1
        
        time.sleep(DELAY_TMDB)

        # --- ШАГ 2: Кинопоиск Update ---
        try:
            kp_result = None
            
            # Если KP_ID уже есть -> обновляем конкретный фильм (дешево и точно)
            if kp_id and kp_id > 0:
                kp_result = kp_updater.get_details_by_id(kp_id)
            # Если KP_ID нет -> ищем (дорого)
            elif kp_id is None:
                kp_result = kp_updater.search_by_title(title, current_year)

            if kp_result:
                found_kp_id, rating, votes = kp_result
                # Рейтинг с КП часто бывает None, если голосов мало
                rating = rating if rating else 0
                votes = votes if votes else 0
                
                cursor.execute("""
                    UPDATE items_minimal 
                    SET kp_id = ?, kp_rating = ?, kp_vote_count = ?
                    WHERE id = ?
                """, (found_kp_id, rating, votes, tmdb_id))
                print(f"   ✅ KP:   {rating} ({votes} голосов) [ID: {found_kp_id}]")
                stats['kp_ok'] += 1
            else:
                # Если не нашли - ставим -1, чтобы больше не мучать поиск для этого фильма
                # Если у фильма был ID, но он перестал отдаваться (404), тоже помечаем как ошибку или оставляем старое
                if kp_id is None: # Только если мы ИСКАЛИ и не нашли
                    cursor.execute("UPDATE items_minimal SET kp_id = -1 WHERE id = ?", (tmdb_id,))
                    print("   ❌ KP:   Не найдено (отмечен -1)")
                    stats['kp_not_found'] += 1
                else:
                    print(f"   ⚠️ KP:   Данные для ID {kp_id} не получены (возможно сбой)")

        except Exception as e:
            if "All KP keys exhausted" in str(e):
                conn.commit()
                print("⛔ Завершение работы: кончились ключи КП.")
                break
            print(f"   Ошибка KP: {e}")

        conn.commit() # Сохраняем после каждого фильма на случай сбоя
        time.sleep(DELAY_KP)

    conn.close()
    print("\n" + "="*30)
    print("ИТОГИ:")
    print(f"TMDB обновлено: {stats['tmdb_ok']}")
    print(f"KP обновлено:   {stats['kp_ok']}")
    print(f"KP не найдено:  {stats['kp_not_found']}")

if __name__ == '__main__':
    main()
