import asyncio
import logging
import aiosqlite
import sqlite3
import os
import re
import json
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright
from tqdm import tqdm

# --- КОНФИГУРАЦИЯ ---
BASE_DIR = Path(os.getcwd())
TMDB_DB_PATH = BASE_DIR / "tmdb_data" / "tmdb_minimal_no_original.db"
TORRENTS_DB_PATH = BASE_DIR / "tmdb_data" / "torrents.db"
DATA_DB_PATH = BASE_DIR / "tmdb_data" / "torrents_data.db"

LOG_FILE = "updater_2025.log"
MAX_CONCURRENT_TABS = 10 
BATCH_SIZE = 20
TARGET_YEAR = datetime.now().year # 2025

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger("aiosqlite").setLevel(logging.WARNING)

# --- ТОЧНАЯ КОПИЯ REGEX ИЗ PARSE_LOCAL.PY ---
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
        'pattern': re.compile(r'\b(Red Head Sound|RHS|Bluebird|HDRezka|Jaskier|TVShows|NewStudio|BaibaKo|AlexFilm|LostFilm|Кубик в [Кк]убе|Octopus|LineFilm|Cold Film|AlphaProject|TVG|Good People|Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|Дубляж|Dub|MVO|DVO|AVO|Original|ENG|RUS|UKR)\b', re.IGNORECASE),
        'type': 'audio_lang'
    },
    'subtitles': {
        'pattern': re.compile(r'Sub\s*[:(]\s*([^)]+)\)?', re.IGNORECASE),
        'type': 'subtitles'
    }
}

# --- КЛАСС ПАРСЕРА JACRED (PLAYWRIGHT) ---
class JacredParser:
    def __init__(self, max_concurrent: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.browser = None
        self.context = None
        self.playwright = None

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True, args=['--no-sandbox'])
        self.context = await self.browser.new_context(viewport={'width': 1400, 'height': 900}, user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
        await self.context.route("**/*.{png,jpg,jpeg,gif,webp,mp4,svg,woff,woff2}", lambda route: route.abort())

    async def stop(self):
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()

    async def parse_movie(self, tmdb_id: int, title_query: str, year: int) -> dict:
        async with self.semaphore:
            page = await self.context.new_page()
            try:
                page.set_default_timeout(30000)
                await page.goto('https://jacred.xyz', wait_until='domcontentloaded')
                
                try:
                    search_input = await page.wait_for_selector('input[type="text"]', state='visible')
                    await search_input.fill('') 
                    await search_input.type(title_query, delay=5) 
                    await search_input.press('Enter')
                    await page.wait_for_timeout(1500)
                except Exception:
                    return {'tmdb_id': tmdb_id, 'torrents': []}

                torrents = await page.evaluate(f'''([yearStr]) => {{
                    const rows = Array.from(document.querySelectorAll('div')).filter(div => {{
                        const text = div.innerText || "";
                        return text.includes("GB") || text.includes("MB") || text.includes("МБ");
                    }});
                    const data = [];
                    const processedMagnets = new Set();
                    for (const el of rows) {{
                        const magnetEl = el.querySelector('a[href^="magnet:"]');
                        if (!magnetEl) continue;
                        const magnet = magnetEl.href;
                        if (processedMagnets.has(magnet)) continue;
                        processedMagnets.add(magnet);

                        let title = el.innerText.split('\\n')[0];
                        if (yearStr && !title.includes(yearStr)) continue;

                        let size = "0 MB";
                        const sizeMatch = el.innerText.match(/(\\d+(\\.\\d+)?)\\s*(GB|MB|ГБ|МБ|TB|ТБ)/i);
                        if (sizeMatch) size = sizeMatch[0];

                        let seeders = 0, leechers = 0;
                        const sM = el.innerText.match(/(?:↑|⬆)\\s*(\\d+)/);
                        const lM = el.innerText.match(/(?:↓|⬇)\\s*(\\d+)/);
                        if (sM) seeders = parseInt(sM[1]);
                        if (lM) leechers = parseInt(lM[1]);

                        data.push({{ torrent_title: title, magnet: magnet, seeders: seeders, leechers: leechers, size: size }});
                    }}
                    return data;
                }}''', [str(year)])
                return {'tmdb_id': tmdb_id, 'torrents': torrents}
            except Exception:
                return {'tmdb_id': tmdb_id, 'torrents': []}
            finally:
                await page.close()

# --- ФУНКЦИИ ЛОКАЛЬНОГО ПАРСИНГА (Точная копия логики parse_local.py) ---

def parse_size_to_bytes(size_str):
    """Конвертация размера (Копия из parse_local.py)"""
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
    """Расчет битрейта (Копия из parse_local.py)"""
    if not size_bytes or not runtime_minutes or runtime_minutes <= 0: return None
    size_bits = size_bytes * 8
    seconds = runtime_minutes * 60
    mbps = (size_bits / seconds) / 1_000_000
    return round(mbps, 2)

def analyze_title(title):
    """Анализ заголовка (Копия из parse_local.py)"""
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
                    s = s.strip()
                    s_lower = s.lower()
                    clean_tag = f"Sub: {s}"
                    if 'rus' in s_lower: clean_tag = "Sub: Rus"
                    elif 'eng' in s_lower: clean_tag = "Sub: Eng"
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

# --- ФУНКЦИЯ ОБНОВЛЕНИЯ МЕТАДАННЫХ ---
def run_local_parsing(target_tmdb_ids):
    """Парсит данные только для указанных ID"""
    if not target_tmdb_ids:
        logger.info("Нет новых фильмов для парсинга метаданных.")
        return

    logger.info(f"⚡ Запуск парсинга метаданных для {len(target_tmdb_ids)} новых фильмов...")
    
    # 1. Загружаем Runtime
    runtime_map = {}
    with sqlite3.connect(TMDB_DB_PATH) as conn:
        for r in conn.execute("SELECT id, runtime FROM items_minimal WHERE runtime IS NOT NULL"):
            runtime_map[r[0]] = r[1]
            
    conn_torrents = sqlite3.connect(TORRENTS_DB_PATH)
    conn_data = sqlite3.connect(DATA_DB_PATH)
    conn_data.execute("PRAGMA journal_mode = WAL;") # Для скорости
    
    conn_data.execute("""CREATE TABLE IF NOT EXISTS torrent_details (
        info_hash TEXT PRIMARY KEY, resolution TEXT, size INTEGER, files TEXT, 
        hdr_type TEXT, file_type TEXT, codec TEXT, bitrate REAL, audio TEXT
    )""")
    
    # Выбираем торренты ТОЛЬКО для обновленных ID
    placeholders = ','.join('?' * len(target_tmdb_ids))
    cursor = conn_torrents.execute(f"SELECT magnet, torrent_title, size, tmdb_id FROM torrents WHERE tmdb_id IN ({placeholders})", tuple(target_tmdb_ids))
    
    to_insert = []
    
    for row in cursor:
        magnet, title, size_str, tmdb_id = row
        hm = re.search(r'btih:([a-zA-Z0-9]{40})', magnet)
        if not hm: continue
        info_hash = hm.group(1).upper()
        
        meta = analyze_title(title or "")
        size_bytes = parse_size_to_bytes(size_str)
        bitrate = calculate_bitrate(size_bytes, runtime_map.get(tmdb_id, 0))
        audio_str = " | ".join(meta['audio_tags'])
        
        # Точная структура данных как в parse_local.py
        to_insert.append((
            info_hash, meta['resolution'], size_bytes, json.dumps(['(title_parse)']),
            meta['hdr_type'], 'mkv', meta['codec'], bitrate, audio_str
        ))
        
    if to_insert:
        conn_data.executemany("""
            INSERT OR REPLACE INTO torrent_details 
            (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, to_insert)
        conn_data.commit()
        logger.info(f"✅ Успешно обновлены метаданные для {len(to_insert)} торрентов.")
    
    conn_torrents.close()
    conn_data.close()

# --- ОСНОВНОЙ ЦИКЛ ---
async def main():
    logger.info(f"🚀 Запуск авто-обновления для {TARGET_YEAR} года...")
    
    async with aiosqlite.connect(TMDB_DB_PATH) as db:
        async with db.execute("SELECT id, title FROM items_minimal WHERE year = ?", (TARGET_YEAR,)) as cursor:
            movies = await cursor.fetchall()
            
    if not movies:
        logger.info(f"Фильмы {TARGET_YEAR} года не найдены в базе TMDB.")
        return

    queue = [{'id': m[0], 'title': m[1]} for m in movies]
    logger.info(f"Найдено {len(queue)} фильмов для проверки.")
    
    parser = JacredParser(max_concurrent=MAX_CONCURRENT_TABS)
    await parser.start()
    
    processed_tmdb_ids = set() # ID фильмов, для которых нашли торренты

    async with aiosqlite.connect(TORRENTS_DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS torrents (id INTEGER PRIMARY KEY, tmdb_id INTEGER, torrent_title TEXT, magnet TEXT, seeders INTEGER, leechers INTEGER, size TEXT, url TEXT, parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_id ON torrents(tmdb_id)")
        await db.commit()

        for i in tqdm(range(0, len(queue), BATCH_SIZE), desc="Scraping"):
            batch = queue[i : i + BATCH_SIZE]
            tasks = [parser.parse_movie(m['id'], m['title'], TARGET_YEAR) for m in batch]
            results = await asyncio.gather(*tasks)
            
            insert_batch = []
            delete_ids = []
            
            for res in results:
                if res['torrents']:
                    processed_tmdb_ids.add(res['tmdb_id']) # Запоминаем, что этот фильм обновился
                    delete_ids.append(res['tmdb_id'])
                    for t in res['torrents']:
                        insert_batch.append((res['tmdb_id'], t['torrent_title'], t['magnet'], t['seeders'], t['leechers'], t['size']))
            
            if insert_batch:
                # Удаляем старые записи ТОЛЬКО для этих фильмов и вставляем новые
                placeholders = ','.join('?' * len(delete_ids))
                await db.execute(f"DELETE FROM torrents WHERE tmdb_id IN ({placeholders})", tuple(delete_ids))
                await db.executemany("INSERT INTO torrents (tmdb_id, torrent_title, magnet, seeders, leechers, size) VALUES (?, ?, ?, ?, ?, ?)", insert_batch)
                await db.commit()

    await parser.stop()
    
    # Сразу запускаем локальный парсинг для этих фильмов
    if processed_tmdb_ids:
        run_local_parsing(list(processed_tmdb_ids))
    else:
        logger.info("Новых торрентов не найдено, парсинг пропущен.")

if __name__ == "__main__":
    asyncio.run(main())
