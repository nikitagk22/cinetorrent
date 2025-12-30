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

LOG_FILE = "global_updater.log"
MAX_CONCURRENT_TABS = 40   # Максимальная производительность
BATCH_SIZE = 50            # Размер пачки
RESTART_BROWSER_EVERY = 500 # Перезапуск браузера каждые N фильмов (очистка RAM)

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("aiosqlite").setLevel(logging.WARNING)

# --- REGEX CONFIG ---
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
                              r'Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|SDI Media|'
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

# --- КЛАСС ПАРСЕРА ---
class JacredParser:
    def __init__(self, max_concurrent: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.browser = None
        self.context = None
        self.playwright = None
        self.processed_count = 0

    async def start(self):
        """Запуск браузера"""
        if self.playwright is None:
            self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox', 
                '--disable-setuid-sandbox', 
                '--disable-dev-shm-usage',
                '--disable-gpu', 
                '--disable-extensions', 
                '--mute-audio',
                '--dns-server=1.1.1.1'
            ]
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    async def stop(self):
        """Полная остановка"""
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
        self.playwright = None

    async def restart(self):
        """Перезапуск для очистки памяти"""
        # logger.info("♻️ Перезапуск браузера для очистки RAM...")
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        # Playwright не стопаем, только браузер
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-gpu', '--mute-audio', '--dns-server=1.1.1.1']
        )
        self.context = await self.browser.new_context(viewport={'width': 1920, 'height': 1080})
        await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def filter_by_year(self, torrents, target_year):
        if not target_year: return torrents
        valid_items = []
        year_pattern = re.compile(r'\b((?:19|20)\d{2})\b')
        allowed_years = {target_year - 1, target_year, target_year + 1}
        
        for t in torrents:
            title = t['torrent_title']
            found_years = [int(y) for y in year_pattern.findall(title)]
            if not found_years:
                valid_items.append(t)
            else:
                if any(y in allowed_years for y in found_years):
                    valid_items.append(t)
        return valid_items

    async def parse_movie(self, tmdb_id: int, search_query: str, target_year: int) -> dict:
        async with self.semaphore:
            page = await self.context.new_page()
            try:
                try:
                    await page.goto('https://jacred.xyz', wait_until='domcontentloaded', timeout=15000)
                except: pass 

                try:
                    search_input = await page.wait_for_selector('input[type="text"]', state='visible', timeout=15000)
                    await search_input.click()
                    await search_input.fill("") 
                    await search_input.type(search_query, delay=10) 
                    
                    search_button = page.locator('button', has_text="НАЙТИ").first
                    if await search_button.is_visible():
                        await search_button.click()
                    else:
                        await search_input.press('Enter')

                    try:
                        await page.wait_for_selector('a[href^="magnet:"]', timeout=20000)
                        await page.wait_for_timeout(2000)
                    except: 
                        pass
                except Exception:
                    return {'tmdb_id': tmdb_id, 'torrents': []}

                # JS Parsing
                raw_torrents = await page.evaluate('''() => {
                    const results = [];
                    // 1. Находим ВСЕ магниты
                    const allMagnets = Array.from(document.querySelectorAll('a[href^="magnet:"]'));
                    
                    // 2. Берем только первые 50 (или меньше, если их нет столько)
                    const top50 = allMagnets.slice(0, 50);

                    top50.forEach(magnetLink => {
                        let container = magnetLink.parentElement;
                        let found = false;
                        for (let i = 0; i < 5; i++) {
                            if (!container) break;
                            const titleLink = container.querySelector('a:not([href^="magnet:"])');
                            if (titleLink && titleLink.innerText.length > 2) {
                                found = true;
                                break;
                            }
                            container = container.parentElement;
                        }
                        if (!found || !container) return;

                        const magnet = magnetLink.href;
                        const linkEls = Array.from(container.querySelectorAll('a'));
                        const titleLink = linkEls.find(a => !a.href.startsWith('magnet:') && a.innerText.trim().length > 1);
                        const title = titleLink ? titleLink.innerText.trim() : container.innerText.split('\\n')[0];

                        let size = "0 MB";
                        const sizeMatch = container.innerText.match(/(\\d+(\\.\\d+)?)\\s*(GB|MB|ГБ|МБ|TB|ТБ)/i);
                        if (sizeMatch) size = sizeMatch[0];

                        let seeders = 0, leechers = 0;
                        const text = container.innerText;
                        const sM = text.match(/(?:↑|⬆)\\s*(\\d+)/);
                        const lM = text.match(/(?:↓|⬇)\\s*(\\d+)/);
                        if (sM) seeders = parseInt(sM[1]);
                        if (lM) leechers = parseInt(lM[1]);

                        results.push({ 
                            torrent_title: title, 
                            magnet: magnet, 
                            seeders: seeders, 
                            leechers: leechers, 
                            size: size 
                        });
                    });
                    return results;
                }''')
                
                filtered_torrents = self.filter_by_year(raw_torrents, target_year)
                return {'tmdb_id': tmdb_id, 'torrents': filtered_torrents}

            except Exception:
                return {'tmdb_id': tmdb_id, 'torrents': []}
            finally:
                await page.close()
                self.processed_count += 1

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
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

def run_local_parsing(target_tmdb_ids):
    if not target_tmdb_ids: return
    # logger.info(f"⚡ Метаданные ({len(target_tmdb_ids)} шт)...")
    runtime_map = {}
    with sqlite3.connect(TMDB_DB_PATH) as conn:
        for r in conn.execute("SELECT id, runtime FROM items_minimal WHERE runtime IS NOT NULL"):
            runtime_map[r[0]] = r[1]
    conn_torrents = sqlite3.connect(TORRENTS_DB_PATH)
    conn_data = sqlite3.connect(DATA_DB_PATH)
    conn_data.execute("PRAGMA journal_mode = WAL;") 
    conn_data.execute("""CREATE TABLE IF NOT EXISTS torrent_details (
        info_hash TEXT PRIMARY KEY, resolution TEXT, size INTEGER, files TEXT, 
        hdr_type TEXT, file_type TEXT, codec TEXT, bitrate REAL, audio TEXT
    )""")
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
        to_insert.append((info_hash, meta['resolution'], size_bytes, json.dumps(['(title_parse)']), meta['hdr_type'], 'mkv', meta['codec'], bitrate, audio_str))
    if to_insert:
        conn_data.executemany("INSERT OR REPLACE INTO torrent_details (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", to_insert)
        conn_data.commit()
    conn_torrents.close()
    conn_data.close()

# --- MAIN ---
async def main():
    logger.info(f"🌍 ЗАПУСК ГЛОБАЛЬНОГО ОБНОВЛЕНИЯ БАЗЫ (ВСЕ ФИЛЬМЫ)")
    
    async with aiosqlite.connect(TMDB_DB_PATH) as db:
        # Сортировка по updated_at ASC (старые первыми)
        # NULL идет первым (никогда не обновлялись)
        logger.info("Загрузка списка фильмов (это может занять время)...")
        async with db.execute("""
            SELECT id, title, kp_id, year 
            FROM items_minimal 
            ORDER BY updated_at ASC
        """) as cursor:
            movies = await cursor.fetchall()
            
    if not movies:
        logger.info("База пуста.")
        return

    logger.info(f"Всего фильмов в базе: {len(movies)}")
    
    # Очередь
    queue = []
    for m in movies:
        tmdb_id = m[0]
        title = m[1]
        kp_id = m[2]
        year = m[3]
        
        if kp_id: search_query = f"kp{kp_id}"
        elif title: search_query = title
        else: continue 
        
        queue.append({'id': tmdb_id, 'query': search_query, 'year': year})

    parser = JacredParser(max_concurrent=MAX_CONCURRENT_TABS)
    await parser.start()
    
    processed_tmdb_ids = set() 
    total_new = 0

    async with aiosqlite.connect(TORRENTS_DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS torrents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tmdb_id INTEGER,
                torrent_title TEXT,
                magnet TEXT,
                seeders INTEGER,
                leechers INTEGER,
                size TEXT,
                url TEXT, 
                parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_id ON torrents(tmdb_id)")
        await db.commit()

        with tqdm(total=len(queue), desc="Global Update", unit="mov") as pbar:
            for i in range(0, len(queue), BATCH_SIZE):
                batch = queue[i : i + BATCH_SIZE]
                
                # Проверка на перезапуск браузера
                if parser.processed_count > RESTART_BROWSER_EVERY:
                    await parser.restart()
                    parser.processed_count = 0

                tasks = [parser.parse_movie(m['id'], m['query'], m['year']) for m in batch]
                results = await asyncio.gather(*tasks)
                
                insert_batch = []
                delete_ids = []
                
                # Дата обновления для всей пачки
                current_date = datetime.now().strftime('%Y-%m-%d')
                all_batch_ids = [r['tmdb_id'] for r in results]

                for res in results:
                    t_id = res['tmdb_id']
                    
                    if res['torrents']:
                        # Если нашли новые - удаляем старые и пишем новые
                        delete_ids.append(t_id)
                        processed_tmdb_ids.add(t_id)
                        total_new += len(res['torrents'])
                        
                        for t in res['torrents']:
                            insert_batch.append((
                                t_id, 
                                t['torrent_title'], 
                                t['magnet'], 
                                t['seeders'], 
                                t['leechers'], 
                                t['size']
                            ))
                
                # 1. Запись торрентов (только для тех, где нашли новое)
                if insert_batch:
                    placeholders = ','.join('?' * len(delete_ids))
                    await db.execute(f"DELETE FROM torrents WHERE tmdb_id IN ({placeholders})", tuple(delete_ids))
                    await db.executemany("""
                        INSERT INTO torrents (tmdb_id, torrent_title, magnet, seeders, leechers, size) 
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, insert_batch)
                    await db.commit()
                
                # 2. Обновление даты проверки в items_minimal для ВСЕХ проверенных (даже если пусто)
                # Это важно, чтобы при следующем запуске они ушли в конец очереди
                async with aiosqlite.connect(TMDB_DB_PATH) as tmdb_db:
                    placeholders_all = ','.join('?' * len(all_batch_ids))
                    params = [current_date] + all_batch_ids
                    await tmdb_db.execute(
                        f"UPDATE items_minimal SET updated_at = ? WHERE id IN ({placeholders_all})",
                        tuple(params)
                    )
                    await tmdb_db.commit()

                pbar.update(len(batch))
                
                # Локальный парсинг пачками (чтобы не копить 52к ID)
                if processed_tmdb_ids:
                    try:
                        run_local_parsing(list(processed_tmdb_ids))
                        processed_tmdb_ids.clear()
                    except Exception:
                        pass

    await parser.stop()
    logger.info(f"✅ Глобальное обновление завершено. Найдено новых раздач: {total_new}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано.")
