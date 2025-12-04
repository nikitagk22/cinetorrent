import sqlite3
import os
import re
import time
import json
import math

# --- НАСТРОЙКИ ---
BASE_DIR = os.getcwd()
DB_TMDB = os.path.join(BASE_DIR, 'tmdb_data', 'tmdb_minimal_no_original.db')
DB_TORRENTS = os.path.join(BASE_DIR, 'tmdb_data', 'torrents.db')
DB_DATA = os.path.join(BASE_DIR, 'tmdb_data', 'torrents_data.db')

BATCH_SIZE = 10000  # Писать в базу пачками по 10к (для скорости)

# --- РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ (Порт с вашего JS) ---
REGEX_CONFIG = {
    'resolution': {
        'pattern': re.compile(r'\b(3840x2160|4K|2160p|1920x1080|1080p|1280x720|720p)\b', re.IGNORECASE),
        'type': 'resolution'
    },
    'audio_channels': {
        'pattern': re.compile(r'\b(5\.1|7\.1)\b', re.IGNORECASE),
        'type': 'audio_channels'
    },
    'quality': {
        'pattern': re.compile(r'\b(HEVC|HDR10\+|HDR10|HDR|Dolby Vision|DV|BDRemux|BluRay|Web-DL|Hybrid|IMAX)\b', re.IGNORECASE),
        'type': 'quality'
    },
    'audio_track': {
        # Длинный список студий
        'pattern': re.compile(r'\b(Red Head Sound|RHS|Bluebird|HDRezka|Jaskier|TVShows|NewStudio|BaibaKo|AlexFilm|LostFilm|Кубик в [Кк]убе|Octopus|LineFilm|Cold Film|AlphaProject|TVG|Good People|Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|Дубляж|Dub|MVO|DVO|AVO|Original|ENG|RUS|UKR)\b', re.IGNORECASE),
        'type': 'audio_lang'
    },
    'subtitles': {
        'pattern': re.compile(r'Sub\s*[:(]\s*([^)]+)\)?', re.IGNORECASE),
        'type': 'subtitles'
    }
}

def get_db_connection(path, readonly=True):
    """Создает подключение к SQLite"""
    if not os.path.exists(path):
        if readonly:
            print(f"❌ Ошибка: Файл БД не найден: {path}")
            return None
    
    conn = sqlite3.connect(path)
    # Включаем WAL для скорости
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    if readonly:
        conn.execute("PRAGMA query_only = 1;")
    return conn

def parse_size_to_bytes(size_str):
    """Конвертирует строку '10.5 GB' в байты"""
    if not size_str:
        return 0
    if isinstance(size_str, (int, float)):
        return int(size_str)
    
    match = re.search(r'(\d+(\.\d+)?)\s*(GB|MB|KB|TB|ГБ|МБ|КБ|ТБ)', size_str, re.IGNORECASE)
    if not match:
        return 0
    
    val = float(match.group(1))
    unit = match.group(3).upper()
    
    unit = unit.replace('ГБ', 'GB').replace('МБ', 'MB').replace('ТБ', 'TB').replace('КБ', 'KB')
    
    if unit == 'TB': val *= 1024**4
    elif unit == 'GB': val *= 1024**3
    elif unit == 'MB': val *= 1024**2
    elif unit == 'KB': val *= 1024
    
    return int(val)

def calculate_bitrate(size_bytes, runtime_minutes):
    """Расчет битрейта (Mbps)"""
    if not size_bytes or not runtime_minutes or runtime_minutes <= 0:
        return None
    size_bits = size_bytes * 8
    seconds = runtime_minutes * 60
    mbps = (size_bits / seconds) / 1_000_000
    return round(mbps, 2)

def analyze_title(title):
    """Парсинг заголовка (Порт логики JS)"""
    if not title:
        return {}

    found_tags = set()
    result = {
        'resolution': None,
        'audio_tags': [],
        'quality_tags': [],
        'hdr_type': 'SDR',
        'codec': None
    }

    # 1. Проход по регуляркам
    for key, config in REGEX_CONFIG.items():
        matches = config['pattern'].finditer(title)
        for match in matches:
            content = match.group(0) # Полное совпадение
            
            # Особая логика для субтитров
            if key == 'subtitles':
                # Вытаскиваем только то, что внутри скобок (группа 1)
                inner = match.group(1)
                subs = re.split(r'[,+]', inner)
                for s in subs:
                    s = s.strip()
                    s_lower = s.lower()
                    clean_tag = f"Sub: {s}"
                    if 'rus' in s_lower: clean_tag = "Sub: Rus"
                    elif 'eng' in s_lower: clean_tag = "Sub: Eng"
                    
                    if clean_tag.lower() not in found_tags:
                        found_tags.add(clean_tag.lower())
                        result['audio_tags'].append(clean_tag)
                continue

            # Логика для аудио и остальных тегов
            clean_content = content.strip()
            # Убираем дубли (например, два раза MVO)
            if clean_content.lower() in found_tags:
                continue
            
            found_tags.add(clean_content.lower())
            
            if config['type'] == 'resolution':
                result['resolution'] = clean_content
            elif config['type'] == 'quality':
                result['quality_tags'].append(clean_content)
            elif config['type'] in ['audio_lang', 'audio_channels']:
                result['audio_tags'].append(clean_content)

    # 2. Постобработка
    # Разрешение
    res = result['resolution']
    if res and res.lower() == '4k':
        result['resolution'] = '4K'
    elif not res:
        result['resolution'] = 'N/A'

    # HDR / Dolby Vision
    quality_combined = " ".join(result['quality_tags'])
    if re.search(r'Dolby|DV', quality_combined, re.IGNORECASE):
        result['hdr_type'] = 'Dolby Vision'
    elif re.search(r'HDR', quality_combined, re.IGNORECASE):
        result['hdr_type'] = 'HDR'

    # Кодек (простой поиск, если нет ptt библиотеки)
    if re.search(r'x265|h265|hevc', title, re.IGNORECASE):
        result['codec'] = 'HEVC'
    elif re.search(r'x264|h264|avc', title, re.IGNORECASE):
        result['codec'] = 'H.264'

    return result

def main():
    print("🚀 Запуск Python парсера заголовков...")
    
    # 1. Подключаемся к базам
    conn_tmdb = get_db_connection(DB_TMDB)
    conn_torrents = get_db_connection(DB_TORRENTS)
    
    # Создаем/Подключаем базу результатов
    conn_data = sqlite3.connect(DB_DATA)
    conn_data.execute("PRAGMA journal_mode = WAL;")
    conn_data.execute("PRAGMA synchronous = NORMAL;")
    
    # Создаем таблицу, если нет
    conn_data.execute("""
        CREATE TABLE IF NOT EXISTS torrent_details (
            info_hash TEXT PRIMARY KEY,
            resolution TEXT,
            size INTEGER,
            files TEXT,
            hdr_type TEXT,
            file_type TEXT,
            codec TEXT,
            bitrate REAL,
            audio TEXT
        )
    """)
    # Добавляем колонки, если старая база
    try:
        cur = conn_data.cursor()
        cols = [info[1] for info in cur.execute("PRAGMA table_info(torrent_details)")]
        if 'codec' not in cols: cur.execute("ALTER TABLE torrent_details ADD COLUMN codec TEXT")
        if 'bitrate' not in cols: cur.execute("ALTER TABLE torrent_details ADD COLUMN bitrate REAL")
        if 'audio' not in cols: cur.execute("ALTER TABLE torrent_details ADD COLUMN audio TEXT")
        conn_data.commit()
    except:
        pass

    # 2. Загружаем Runtime (Длительность) в память для скорости
    print("⏳ Загрузка длительности фильмов (Runtime)...")
    runtime_map = {}
    try:
        cursor = conn_tmdb.cursor()
        cursor.execute("SELECT id, runtime FROM items_minimal WHERE runtime IS NOT NULL")
        for row in cursor:
            runtime_map[row[0]] = row[1]
    except Exception as e:
        print(f"⚠️ Ошибка чтения runtime: {e}")
    conn_tmdb.close()
    print(f"   ∟ Загружено {len(runtime_map)} записей.")

    # 3. Загружаем уже обработанные хеши (чтобы не дублировать)
    print("⏳ Проверка существующей базы...")
    existing_hashes = set()
    try:
        cursor = conn_data.cursor()
        cursor.execute("SELECT info_hash FROM torrent_details")
        for row in cursor:
            existing_hashes.add(row[0])
    except:
        pass
    print(f"   ∟ В базе уже есть: {len(existing_hashes)} записей.")

    # 4. Читаем исходные торренты
    print("⏳ Чтение списка торрентов...")
    cursor = conn_torrents.cursor()
    # Берем сразу все, SQLite справится, это быстро
    cursor.execute("SELECT magnet, torrent_title, size, tmdb_id FROM torrents")
    
    to_insert = []
    processed_count = 0
    skipped_count = 0
    
    start_time = time.time()

    for row in cursor:
        magnet, title, size_str, tmdb_id = row
        
        # Парсим хеш из магнета
        hash_match = re.search(r'btih:([a-zA-Z0-9]{40})', magnet)
        if not hash_match:
            continue
        
        info_hash = hash_match.group(1).upper() # Приводим к верхнему регистру как в JS

        # Пропускаем, если уже есть
        if info_hash in existing_hashes:
            skipped_count += 1
            continue

        # --- АНАЛИЗ ---
        if not title: title = ""
        
        # Анализ текста
        meta = analyze_title(title)
        
        # Размер и битрейт
        size_bytes = parse_size_to_bytes(size_str)
        runtime = runtime_map.get(tmdb_id, 0)
        bitrate = calculate_bitrate(size_bytes, runtime)
        
        # Формируем строку аудио
        audio_str = " | ".join(meta['audio_tags'])
        
        # Данные для вставки
        row_data = (
            info_hash,
            meta['resolution'],
            size_bytes,
            json.dumps(['(title_parse)']), # Файлы неизвестны, ставим заглушку
            meta['hdr_type'],
            'mkv', # Предполагаем mkv, так как парсим title
            meta['codec'],
            bitrate,
            audio_str
        )
        
        to_insert.append(row_data)
        processed_count += 1

        # Пакетная вставка
        if len(to_insert) >= BATCH_SIZE:
            conn_data.executemany("""
                INSERT OR REPLACE INTO torrent_details 
                (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, to_insert)
            conn_data.commit()
            to_insert = []
            
            elapsed = time.time() - start_time
            speed = processed_count / elapsed
            print(f"\r⚡ Обработано: {processed_count} (Sk: {skipped_count}) | Скорость: {int(speed)} шт/сек", end="")

    # Вставляем остаток
    if to_insert:
        conn_data.executemany("""
            INSERT OR REPLACE INTO torrent_details 
            (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, to_insert)
        conn_data.commit()

    conn_torrents.close()
    conn_data.close()
    
    print(f"\n\n✅ ГОТОВО! Новых записей: {processed_count}. Пропущено: {skipped_count}")

if __name__ == "__main__":
    main()
