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

# --- КОНФИГУРАЦИЯ ---
BASE_DIR = Path(os.getcwd())
TMDB_DB_PATH = BASE_DIR / "tmdb_data" / "tmdb_minimal_no_original.db"
TORRENTS_DB_PATH = BASE_DIR / "tmdb_data" / "torrents.db"
DATA_DB_PATH = BASE_DIR / "tmdb_data" / "torrents_data.db"

# Логируем в консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger("aiosqlite").setLevel(logging.WARNING)

# --- REGEX CONFIG (Метаданные) ---
REGEX_CONFIG = {
    'resolution': {'pattern': re.compile(r'\b(3840x2160|4K|2160p|1920x1080|1080p|1280x720|720p)\b', re.IGNORECASE), 'type': 'resolution'},
    'audio_channels': {'pattern': re.compile(r'\b(5\.1|7\.1)\b', re.IGNORECASE), 'type': 'audio_channels'},
    'quality': {'pattern': re.compile(r'\b(HEVC|HDR10\+|HDR10|HDR|Dolby Vision|DV|BDRemux|BluRay|Web-DL|Hybrid|IMAX)\b', re.IGNORECASE), 'type': 'quality'},
    'audio_track': {'pattern': re.compile(r'\b(Red Head Sound|RHS|Bluebird|HDRezka|Jaskier|TVShows|NewStudio|BaibaKo|AlexFilm|LostFilm|Кубик в [Кк]убе|Octopus|LineFilm|Cold Film|AlphaProject|TVG|Good People|Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|Дубляж|Dub|MVO|DVO|AVO|Original|ENG|RUS|UKR)\b', re.IGNORECASE), 'type': 'audio_lang'},
    'subtitles': {'pattern': re.compile(r'Sub\s*[:(]\s*([^)]+)\)?', re.IGNORECASE), 'type': 'subtitles'}
}

# --- КЛАСС ПАРСЕРА ---
class JacredParser:
    def __init__(self):
        self.browser = None
        self.context = None
        self.playwright = None

    async def start(self):
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
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()

    def filter_by_year(self, torrents, target_year):
        """
        Фильтрация: 
        - Если года нет -> оставляем.
        - Если год есть -> проверяем совпадение (+/- 1 год).
        """
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

    async def parse_movie(self, tmdb_id: int, search_query: str, target_year: int, enable_year_filter: bool) -> dict:
        page = await self.context.new_page()
        try:
            logger.info(f"🌐 Ищем: {search_query} (Фильтр по году: {enable_year_filter})")
            
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

                # Ждем магниты
                try:
                    await page.wait_for_selector('a[href^="magnet:"]', timeout=20000)
                    await page.wait_for_timeout(2000)
                except: 
                    pass

            except Exception as e:
                logger.error(f"Ошибка интерфейса: {e}")
                return {'tmdb_id': tmdb_id, 'torrents': []}

            # JS Parsing (Smart Traverse)
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
            
            logger.info(f"🔎 Найдено на странице: {len(raw_torrents)}")

            if enable_year_filter:
                final_torrents = self.filter_by_year(raw_torrents, target_year)
                logger.info(f"✂️ После фильтрации по году: {len(final_torrents)}")
            else:
                final_torrents = raw_torrents
            
            return {
                'tmdb_id': tmdb_id, 
                'torrents': final_torrents
            }

        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}")
            return {'tmdb_id': tmdb_id, 'torrents': []}
        finally:
            await page.close()

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
    logger.info(f"⚡ Анализ метаданных...")
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
    print("\n--- РУЧНОЕ ОБНОВЛЕНИЕ ФИЛЬМА ---")
    try:
        input_id = input("Введите TMDB ID фильма: ").strip()
        tmdb_id = int(input_id)
    except ValueError:
        logger.error("ID должен быть числом!")
        return

    filter_input = input("Включить фильтрацию по году? (y/n) [default: y]: ").strip().lower()
    enable_year_filter = True if filter_input != 'n' else False

    async with aiosqlite.connect(TMDB_DB_PATH) as db:
        async with db.execute("SELECT id, title, kp_id, year FROM items_minimal WHERE id = ?", (tmdb_id,)) as cursor:
            movie = await cursor.fetchone()
            
    if not movie:
        logger.error(f"Фильм с ID {tmdb_id} не найден в базе {TMDB_DB_PATH}")
        return

    title = movie[1]
    kp_id = movie[2]
    movie_year = movie[3]
    
    logger.info(f"🎥 Фильм: {title} ({movie_year}) | KP: {kp_id}")

    # Выбор стратегии поиска
    if kp_id:
        search_query = f"kp{kp_id}"
    elif title:
        search_query = title
    else:
        logger.error("У фильма нет ни названия, ни KP ID.")
        return

    parser = JacredParser()
    await parser.start()
    
    res = await parser.parse_movie(tmdb_id, search_query, movie_year, enable_year_filter)
    
    await parser.stop()

    if res['torrents']:
        count = len(res['torrents'])
        logger.info(f"✅ Найдено {count} раздач. Сохранение в БД...")
        
        async with aiosqlite.connect(TORRENTS_DB_PATH) as db:
            # Очистка старых
            await db.execute("DELETE FROM torrents WHERE tmdb_id = ?", (tmdb_id,))
            
            insert_data = []
            for t in res['torrents']:
                insert_data.append((
                    tmdb_id, 
                    t['torrent_title'], 
                    t['magnet'], 
                    t['seeders'], 
                    t['leechers'], 
                    t['size']
                ))
            
            await db.executemany("""
                INSERT INTO torrents (tmdb_id, torrent_title, magnet, seeders, leechers, size) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, insert_data)
            await db.commit()

        # Обновление даты
        current_date = datetime.now().strftime('%Y-%m-%d')
        async with aiosqlite.connect(TMDB_DB_PATH) as tmdb_db:
            await tmdb_db.execute(
                "UPDATE items_minimal SET updated_at = ? WHERE id = ?",
                (current_date, tmdb_id)
            )
            await tmdb_db.commit()

        # Локальный парсинг метаданных
        try:
            run_local_parsing([tmdb_id])
            logger.info("🎉 Успешно завершено.")
        except Exception as e:
            logger.error(f"Ошибка метаданных: {e}")

    else:
        logger.warning("❌ Раздач не найдено (либо отфильтрованы).")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано.")
