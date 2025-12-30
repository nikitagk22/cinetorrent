import sqlite3
import re
import json
import os
import sys
from pathlib import Path
from tqdm import tqdm

# --- НАСТРОЙКИ ---
# Если True: скрипт проверит ВСЕ торренты заново (нужно, чтобы найти новые озвучки в старых раздачах).
# Если False: скрипт пропустит те, у которых метаданные уже заполнены.
RESCAN_ALL = True 

BASE_DIR = Path(os.getcwd())
TMDB_DB_PATH = BASE_DIR / "tmdb_data" / "tmdb_minimal_no_original.db"
TORRENTS_DB_PATH = BASE_DIR / "tmdb_data" / "torrents.db"
DATA_DB_PATH = BASE_DIR / "tmdb_data" / "torrents_data.db"

# --- РАСШИРЕННЫЙ REGEX CONFIG ---
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
        'pattern': re.compile(r'\b('
                              # --- СОВРЕМЕННЫЕ СТУДИИ / РЕЛИЗ ГРУППЫ ---
                              r'Red Head Sound|RHS|Bluebird|HDRezka|Rezka|Jaskier|'
                              r'TVShows|NewStudio|BaibaKo|AlexFilm|LostFilm|Кубик в [Кк]убе|'
                              r'Octopus|LineFilm|Cold Film|AlphaProject|TVG|Good People|'
                              r'Pazl Voice|Ultradox|RuDub|Sound Film|ViruseProject|IdeaFilm|Novamedia|Кириллица|'
                              r'Kerob|Sunshine Studio|NewComers|LakeFilms|HamsterStudio|Paramount Comedy|'
                              r'Кураж-Бамбей|Kuraj-Bambey|Сыендук|Syenduk|'
                              # --- АНИМЕ ---
                              r'AniLibria|AniDUB|AnimeVost|SHIZA Project|Jam Club|Studio Band|Студийная Банда|'
                              r'SovetRomantica|Kansai|AniStar|AniFilm|Dream Cast|AniMaunt|AniRise|Amazing Dubbing|'
                              # --- АВТОРСКИЕ / VHS (ЛЕГЕНДЫ) ---
                              r'Гаврилов|Михалев|Володарский|Сербин|Живов|Пучков|Гоблин|Goblin|'
                              r'Дохалов|Визгунов|Карцев|Иванов|Санаев|Есарев|Штейн|Либерти|Вартан|Горчаков|'
                              r'Котов|Яковлев|Гланц|Glanz|'
                              # --- ОФИЦИАЛЬНЫЕ / ПРОФЕССИОНАЛЬНЫЕ ---
                              r'Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|SDI Media|ДБ|'
                              r'Киномания|Tycoon|CPIG|Позитив|Видеосервис|Varus Video|West Video|'
                              r'iTunes|Amedia|Netflix|'
                              # --- ОБЩИЕ МЕТКИ ---
                              r'Дубляж|Dub|MVO|DVO|AVO|Original|ENG|RUS|UKR'
                              r')\b', re.IGNORECASE), 
        'type': 'audio_lang'
    },
    'subtitles': {
        'pattern': re.compile(r'Sub\s*[:(]\s*([^)]+)\)?', re.IGNORECASE), 
        'type': 'subtitles'
    }
}

# --- ФУНКЦИИ ---
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
def main():
    print(f"🚀 Запуск (Режим полного пересканирования: {RESCAN_ALL})...")
    
    if not os.path.exists(TMDB_DB_PATH) or not os.path.exists(TORRENTS_DB_PATH):
        print("❌ Ошибка: Базы данных не найдены.")
        return

    # 1. Загружаем Runtime
    print("📦 Загрузка длительности фильмов (Runtime)...")
    runtime_map = {}
    with sqlite3.connect(TMDB_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, runtime FROM items_minimal WHERE runtime IS NOT NULL")
        for r in cursor:
            runtime_map[r[0]] = r[1]
    
    # 2. Проверяем, что уже есть (только если RESCAN_ALL = False)
    valid_hashes = set()
    
    with sqlite3.connect(DATA_DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS torrent_details (
            info_hash TEXT PRIMARY KEY, resolution TEXT, size INTEGER, files TEXT, 
            hdr_type TEXT, file_type TEXT, codec TEXT, bitrate REAL, audio TEXT
        )""")
        
        if not RESCAN_ALL:
            print("📦 Проверка существующих метаданных...")
            cursor = conn.execute("SELECT info_hash, resolution FROM torrent_details")
            for row in cursor:
                h, res = row
                if res and res != 'N/A':
                    valid_hashes.add(h)
            print(f"✅ Будет пропущено {len(valid_hashes)} записей.")
        else:
            print("⚠️ RESCAN_ALL включен. Существующие записи будут обновлены новыми тегами.")

    # 3. Загружаем торренты
    print("📦 Загрузка списка торрентов...")
    torrents_to_process = []
    
    with sqlite3.connect(TORRENTS_DB_PATH) as conn:
        cursor = conn.execute("SELECT tmdb_id, torrent_title, magnet, size FROM torrents")
        rows = cursor.fetchall()
        
        for row in rows:
            magnet = row[2]
            hm = re.search(r'btih:([a-zA-Z0-9]{40})', magnet)
            if not hm: continue
            
            info_hash = hm.group(1).upper()
            
            # Если RESCAN_ALL = True, то valid_hashes пустой, и мы берем всё.
            if info_hash not in valid_hashes:
                torrents_to_process.append({
                    'tmdb_id': row[0],
                    'title': row[1],
                    'size_str': row[3],
                    'info_hash': info_hash
                })

    total_count = len(torrents_to_process)
    if total_count == 0:
        print("🎉 Нет торрентов для обработки.")
        return

    print(f"⚡ Обработка {total_count} торрентов...")

    # 4. Обработка
    batch_size = 1000
    current_batch = []
    
    conn_data = sqlite3.connect(DATA_DB_PATH)
    conn_data.execute("PRAGMA journal_mode = WAL;") 
    
    for item in tqdm(torrents_to_process, desc="Processing"):
        tmdb_id = item['tmdb_id']
        title = item['title']
        size_str = item['size_str']
        info_hash = item['info_hash']
        
        meta = analyze_title(title or "")
        size_bytes = parse_size_to_bytes(size_str)
        runtime = runtime_map.get(tmdb_id, 0)
        bitrate = calculate_bitrate(size_bytes, runtime)
        audio_str = " | ".join(meta['audio_tags'])
        
        row_data = (
            info_hash,
            meta['resolution'],
            size_bytes,
            json.dumps(['(title_parse)']),
            meta['hdr_type'],
            'mkv',
            meta['codec'],
            bitrate,
            audio_str
        )
        current_batch.append(row_data)
        
        if len(current_batch) >= batch_size:
            conn_data.executemany("""
                INSERT OR REPLACE INTO torrent_details 
                (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, current_batch)
            conn_data.commit()
            current_batch = []

    if current_batch:
        conn_data.executemany("""
            INSERT OR REPLACE INTO torrent_details 
            (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, current_batch)
        conn_data.commit()

    conn_data.close()
    print("\n🏁 Готово!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nПрервано.")
