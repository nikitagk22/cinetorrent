import sqlite3
import re
import json
import sys
import os
from pathlib import Path

# --- КОНФИГУРАЦИЯ ПУТЕЙ ---
BASE_DIR = Path(os.getcwd())
TMDB_DB_PATH = BASE_DIR / "tmdb_data" / "tmdb_minimal_no_original.db"
TORRENTS_DB_PATH = BASE_DIR / "tmdb_data" / "torrents.db"
DATA_DB_PATH = BASE_DIR / "tmdb_data" / "torrents_data.db"

# --- ТВОИ НАСТРОЙКИ REGEX ---
REGEX_CONFIG = {
    'resolution': {'pattern': re.compile(r'\b(3840x2160|4K|2160p|1920x1080|1080p|1280x720|720p)\b', re.IGNORECASE), 'type': 'resolution'},
    'audio_channels': {'pattern': re.compile(r'\b(5\.1|7\.1)\b', re.IGNORECASE), 'type': 'audio_channels'},
    'quality': {'pattern': re.compile(r'\b(HEVC|HDR10\+|HDR10|HDR|Dolby Vision|DV|BDRemux|BluRay|Web-DL|Hybrid|IMAX)\b', re.IGNORECASE), 'type': 'quality'},
    'audio_track': {'pattern': re.compile(r'\b(Red Head Sound|RHS|Bluebird|HDRezka|Jaskier|TVShows|NewStudio|BaibaKo|AlexFilm|LostFilm|Кубик в [Кк]убе|Octopus|LineFilm|Cold Film|AlphaProject|TVG|Good People|Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|Дубляж|Dub|MVO|DVO|AVO|Original|ENG|RUS|UKR)\b', re.IGNORECASE), 'type': 'audio_lang'},
    'subtitles': {'pattern': re.compile(r'Sub\s*[:(]\s*([^)]+)\)?', re.IGNORECASE), 'type': 'subtitles'}
}

# --- ТВОИ ФУНКЦИИ ---
def parse_size_to_bytes(size_str):
    if not size_str: return 0
    match = re.search(r'(\d+(\.\d+)?)\s*(GB|MB|KB|TB|ГБ|МБ|КБ|ТБ)', str(size_str), re.IGNORECASE)
    if not match: return 0
    val = float(match.group(1))
    unit = match.group(3).upper().replace('ГБ','GB').replace('МБ','MB').replace('ТБ','TB').replace('КБ','KB')
    if unit == 'TB': val *= 1024**4
    elif unit == 'GB': val *= 1024**3
    elif unit == 'MB': val *= 1024**2
    elif unit == 'KB': val *= 1024
    return int(val)

def calculate_bitrate(size_bytes, runtime_minutes):
    if not size_bytes or not runtime_minutes or runtime_minutes <= 0: return None
    size_bits = size_bytes * 8
    seconds = runtime_minutes * 60
    mbps = (size_bits / seconds) / 1_000_000
    return round(mbps, 2)

def analyze_title(title):
    if not title: return {}
    found_tags = set()
    result = {'resolution': 'N/A', 'audio_tags': [], 'quality_tags': [], 'hdr_type': 'SDR', 'codec': None}
    for key, config in REGEX_CONFIG.items():
        matches = config['pattern'].finditer(title)
        for match in matches:
            content = match.group(0)
            if key == 'subtitles':
                inner = match.group(1)
                subs = re.split(r'[,+]', inner)
                for s in subs:
                    clean_tag = f"Sub: {s.strip()}"
                    if 'rus' in s.lower(): clean_tag = "Sub: Rus"
                    elif 'eng' in s.lower(): clean_tag = "Sub: Eng"
                    if clean_tag.lower() not in found_tags:
                        found_tags.add(clean_tag.lower())
                        result['audio_tags'].append(clean_tag)
                continue
            clean_content = content.strip()
            if clean_content.lower() in found_tags: continue
            found_tags.add(clean_content.lower())
            if config['type'] == 'resolution': result['resolution'] = clean_content
            elif config['type'] == 'quality': result['quality_tags'].append(clean_content)
            elif config['type'] in ['audio_lang', 'audio_channels']: result['audio_tags'].append(clean_content)
    res = result['resolution']
    if res and res.lower() == '4k': result['resolution'] = '4K'
    elif not res: result['resolution'] = 'N/A'
    quality_combined = " ".join(result['quality_tags'])
    if re.search(r'Dolby|DV', quality_combined, re.IGNORECASE): result['hdr_type'] = 'Dolby Vision'
    elif re.search(r'HDR', quality_combined, re.IGNORECASE): result['hdr_type'] = 'HDR'
    if re.search(r'x265|h265|hevc', title, re.IGNORECASE): result['codec'] = 'HEVC'
    elif re.search(r'x264|h264|avc', title, re.IGNORECASE): result['codec'] = 'H.264'
    return result

# --- ОСНОВНАЯ ЛОГИКА ---
def reparse_movie(tmdb_id):
    print(f"🔄 Запуск обработки метаданных для ID: {tmdb_id}")

    # 1. Получаем Runtime из основной базы
    runtime = 0
    if os.path.exists(TMDB_DB_PATH):
        with sqlite3.connect(TMDB_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT runtime, title FROM items_minimal WHERE id = ?", (tmdb_id,))
            row = cursor.fetchone()
            if row:
                runtime = row[0] if row[0] else 0
                print(f"🎬 Фильм: {row[1]} (Длительность: {runtime} мин.)")
            else:
                print(f"⚠️ Фильм с ID {tmdb_id} не найден в {TMDB_DB_PATH}")
                # Продолжаем, просто битрейт будет 0
    else:
        print(f"❌ База {TMDB_DB_PATH} не найдена!")
        return

    # 2. Получаем список торрентов
    torrents = []
    if os.path.exists(TORRENTS_DB_PATH):
        with sqlite3.connect(TORRENTS_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT magnet, torrent_title, size FROM torrents WHERE tmdb_id = ?", (tmdb_id,))
            torrents = cursor.fetchall()
    else:
        print(f"❌ База {TORRENTS_DB_PATH} не найдена!")
        return

    if not torrents:
        print("❌ Торренты для этого ID не найдены.")
        return

    print(f"🔍 Найдено раздач: {len(torrents)}")

    # 3. Подготовка данных для DATA DB
    to_insert = []
    
    for magnet, title, size_str in torrents:
        # Извлекаем Info Hash из магнита
        hm = re.search(r'btih:([a-zA-Z0-9]{40})', magnet)
        if not hm:
            continue
        info_hash = hm.group(1).upper()
        
        # Анализ
        meta = analyze_title(title or "")
        size_bytes = parse_size_to_bytes(size_str)
        bitrate = calculate_bitrate(size_bytes, runtime)
        
        # Формирование строки аудио
        audio_str = " | ".join(meta['audio_tags'])
        
        # Данные для вставки
        row_data = (
            info_hash,
            meta['resolution'],
            size_bytes,
            json.dumps(['(title_parse)']), # Заглушка для файлов
            meta['hdr_type'],
            'mkv', # Предполагаем mkv, так как парсим только заголовок
            meta['codec'],
            bitrate,
            audio_str
        )
        to_insert.append(row_data)

    # 4. Запись в DATA DB
    if to_insert:
        try:
            conn_data = sqlite3.connect(DATA_DB_PATH)
            # Включаем WAL для быстродействия, если нужно
            conn_data.execute("PRAGMA journal_mode = WAL;") 
            
            # Создаем таблицу, если её нет
            conn_data.execute("""CREATE TABLE IF NOT EXISTS torrent_details (
                info_hash TEXT PRIMARY KEY, 
                resolution TEXT, 
                size INTEGER, 
                files TEXT, 
                hdr_type TEXT, 
                file_type TEXT, 
                codec TEXT, 
                bitrate REAL, 
                audio TEXT
            )""")
            
            # Вставляем данные (REPLACE, чтобы обновить старые данные)
            conn_data.executemany("""
                INSERT OR REPLACE INTO torrent_details 
                (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, to_insert)
            
            conn_data.commit()
            conn_data.close()
            print(f"✅ Успешно обновлено записей: {len(to_insert)}")
        except sqlite3.Error as e:
            print(f"❌ Ошибка записи в БД: {e}")
    else:
        print("⚠️ Нечего записывать (возможно, битые магниты).")

if __name__ == "__main__":
    print("--- Reparse Metadata Tool ---")
    if len(sys.argv) > 1:
        try:
            t_id = int(sys.argv[1])
            reparse_movie(t_id)
        except ValueError:
            print("ID должен быть числом.")
    else:
        try:
            user_input = input("Введите TMDB ID: ").strip()
            if user_input:
                reparse_movie(int(user_input))
        except ValueError:
            print("Ошибка: ID должен быть целым числом.")
        except KeyboardInterrupt:
            print("\nОтменено.")
