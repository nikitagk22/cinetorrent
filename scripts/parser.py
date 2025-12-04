import asyncio
import logging
import aiosqlite
from pathlib import Path
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from tqdm import tqdm

# ---------------- Конфигурация ----------------
SOURCE_DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"
DEST_DB_PATH = Path("tmdb_data") / "torrents.db"
LOG_FILE = "parser_log.txt"

MAX_CONCURRENT_TABS = 12 
BATCH_SIZE = 20

# ---------------- Логирование ----------------
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
logging.getLogger("asyncio").setLevel(logging.WARNING)

# ---------------- Класс Парсера ----------------
class JacredParser:
    def __init__(self, max_concurrent: int = 5, headless: bool = False):
        self.headless = headless
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.browser = None
        self.context = None
        self.playwright = None

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1400, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        await self.context.route("**/*.{png,jpg,jpeg,gif,webp,mp4,svg,woff,woff2}", lambda route: route.abort())
        await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    async def stop(self):
        if self.context: await self.context.close()
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()

    async def parse_movie(self, tmdb_id: int, title_query: str, year: int, limit: int = 50) -> Dict[str, Any]:
        async with self.semaphore:
            page = await self.context.new_page()
            
            # ИСПРАВЛЕНИЕ: Ищем ТОЛЬКО по названию, чтобы сайт точно нашел раздачи
            search_query = title_query
            
            try:
                page.set_default_timeout(30000)
                
                await page.goto('https://jacred.xyz', wait_until='domcontentloaded')
                
                try:
                    search_input = await page.wait_for_selector('input[type="text"]', state='visible')
                    await search_input.fill('') 
                    await search_input.type(search_query, delay=30) 
                    
                    search_button = page.locator('button', has_text="НАЙТИ").first
                    if await search_button.is_visible():
                        await search_button.click()
                    else:
                        await search_input.press('Enter')

                    await page.wait_for_timeout(2000)
                    try:
                        await page.wait_for_load_state('networkidle', timeout=3000)
                    except: pass

                except Exception as e:
                    logger.warning(f"Search interaction failed for '{search_query}': {e}")
                    return {'tmdb_id': tmdb_id, 'movie_name': title_query, 'torrents': []}

                # --- JS Парсинг с ФИЛЬТРАЦИЕЙ ---
                # Мы передаем год (yearStr) в функцию. 
                # Скрипт перебирает результаты поиска и берет ТОЛЬКО те, где есть этот год.
                torrents = await page.evaluate(f'''([limit, yearStr]) => {{
                    const allDivs = Array.from(document.querySelectorAll('div'));
                    
                    const rows = allDivs.filter(div => {{
                        const text = div.innerText || "";
                        return text.includes("GB") || text.includes("ГБ") || text.includes("MB") || text.includes("МБ");
                    }});

                    const data = [];
                    const processedMagnets = new Set();
                    let count = 0;

                    for (const el of rows) {{
                        if (count >= limit) break;

                        const magnetEl = el.querySelector('a[href^="magnet:"]');
                        if (!magnetEl) continue;

                        const magnet = magnetEl.href;
                        if (processedMagnets.has(magnet)) continue;
                        processedMagnets.add(magnet);

                        // Получаем название
                        const links = Array.from(el.querySelectorAll('a'));
                        const sourceLink = links.find(a => !a.href.startsWith('magnet:') && a.innerText.trim().length > 0);
                        
                        let url = "";
                        let title = "No Title";

                        if (sourceLink) {{
                            url = sourceLink.href;
                            title = sourceLink.innerText.trim();
                        }} else {{
                            title = el.innerText.split('\\n')[0];
                        }}

                        // --- ФИЛЬТР: ЖЕСТКАЯ ПРОВЕРКА ГОДА ---
                        // Мы ищем "Матрица", сайт выдает "Матрица (1999)" и "Матрица 4 (2021)".
                        // Если мы ищем 1999 год, то "2021" не пройдет проверку.
                        if (yearStr && !title.includes(yearStr)) {{
                             continue;
                        }}

                        const text = el.innerText;
                        
                        let size = "0 MB";
                        const sizeMatch = text.match(/(\\d+(\\.\\d+)?)\\s*(GB|MB|ГБ|МБ|TB|ТБ)/i);
                        if (sizeMatch) size = sizeMatch[0];

                        let seeders = 0;
                        let leechers = 0;
                        const seedMatch = text.match(/(?:↑|⬆)\\s*(\\d+)/);
                        const leechMatch = text.match(/(?:↓|⬇)\\s*(\\d+)/);
                        if (seedMatch) seeders = parseInt(seedMatch[1]);
                        if (leechMatch) leechers = parseInt(leechMatch[1]);

                        data.push({{
                            torrent_title: title,
                            magnet: magnet,
                            seeders: seeders,
                            leechers: leechers,
                            size: size,
                            url: url
                        }});
                        count++;
                    }}
                    return data;
                }}''', [limit, str(year)])

                return {'tmdb_id': tmdb_id, 'movie_name': title_query, 'torrents': torrents}

            except Exception as e:
                logger.error(f"Error parsing {tmdb_id}: {e}")
                return {'tmdb_id': tmdb_id, 'movie_name': title_query, 'torrents': []}
            finally:
                await page.close()

# ---------------- Работа с БД ----------------

async def init_dest_db():
    async with aiosqlite.connect(DEST_DB_PATH) as db:
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

async def get_processed_ids() -> set:
    if not DEST_DB_PATH.exists():
        return set()
    async with aiosqlite.connect(DEST_DB_PATH) as db:
        async with db.execute("SELECT DISTINCT tmdb_id FROM torrents") as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

async def save_results_batch(db_path, results_list):
    if not results_list: return

    insert_data = []
    for res in results_list:
        t_id = res['tmdb_id']
        torrents = res['torrents']
        
        if len(torrents) > 0:
            logger.info(f"[WRITE] ID: {t_id} ({res['movie_name']}) -> Found {len(torrents)} valid torrents")
            for t in torrents:
                insert_data.append((
                    t_id, 
                    t['torrent_title'], 
                    t['magnet'], 
                    t['seeders'], 
                    t['leechers'], 
                    t['size'], 
                    t.get('url', '')
                ))
    
    if insert_data:
        async with aiosqlite.connect(db_path) as db:
            await db.executemany("""
                INSERT INTO torrents (tmdb_id, torrent_title, magnet, seeders, leechers, size, url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, insert_data)
            await db.commit()

# ---------------- Main ----------------

async def main():
    logger.info(f"Starting parser v3 (Search by Title, Filter by Year).")
    if not SOURCE_DB_PATH.exists():
        logger.error("Source DB not found!")
        return
    
    await init_dest_db()
    
    logger.info("Loading movies...")
    async with aiosqlite.connect(SOURCE_DB_PATH) as db:
        async with db.execute("SELECT id, title, year FROM items_minimal WHERE title IS NOT NULL AND year IS NOT NULL") as cursor:
            rows = await cursor.fetchall()
    
    processed = await get_processed_ids()
    queue = [
        {'id': r[0], 'title': r[1], 'year': r[2]} 
        for r in rows if r[0] not in processed
    ]
    
    logger.info(f"Total: {len(rows)} | Already Done: {len(processed)} | Queue: {len(queue)}")
    if not queue: return

    # headless=False чтобы видеть процесс
    parser = JacredParser(max_concurrent=MAX_CONCURRENT_TABS, headless=True)
    await parser.start()
    
    try:
        with tqdm(total=len(queue), desc="Parsing") as pbar:
            for i in range(0, len(queue), BATCH_SIZE):
                batch = queue[i : i + BATCH_SIZE]
                tasks = []
                for m in batch:
                    # Передаем: ID, Название (для поиска), Год (для фильтрации)
                    tasks.append(parser.parse_movie(m['id'], m['title'], m['year']))
                
                res = await asyncio.gather(*tasks)
                await save_results_batch(DEST_DB_PATH, res)
                pbar.update(len(batch))
    finally:
        await parser.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")
