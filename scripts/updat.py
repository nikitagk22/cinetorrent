import asyncio
import logging
import aiosqlite
from pathlib import Path
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from tqdm import tqdm
from datetime import datetime

# ---------------- Конфигурация ----------------
SOURCE_DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"
DEST_DB_PATH = Path("tmdb_data") / "torrents.db"
LOG_FILE = "updater_log.txt"

# Настройки под Xeon E5-2680 v3
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
    def __init__(self, max_concurrent: int = 5, headless: bool = True):
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

    async def parse_movie(self, tmdb_id: int, title_query: str, year: int = None, limit: int = 50) -> Dict[str, Any]:
        async with self.semaphore:
            page = await self.context.new_page()
            search_query = title_query
            
            # Подготовка строки года для JS. Если year=None или 0, передаем пустую строку.
            year_str_for_js = str(year) if year and year > 0 else ""

            try:
                page.set_default_timeout(30000)
                await page.goto('https://jacred.xyz', wait_until='domcontentloaded')
                
                try:
                    search_input = await page.wait_for_selector('input[type="text"]', state='visible')
                    await search_input.fill('') 
                    await search_input.type(search_query, delay=5) 
                    
                    search_button = page.locator('button', has_text="НАЙТИ").first
                    if await search_button.is_visible():
                        await search_button.click()
                    else:
                        await search_input.press('Enter')

                    await page.wait_for_timeout(1500)
                    try:
                        await page.wait_for_load_state('networkidle', timeout=3000)
                    except: pass

                except Exception as e:
                    logger.warning(f"Search interaction failed for '{search_query}': {e}")
                    return {'tmdb_id': tmdb_id, 'movie_name': title_query, 'torrents': []}

                # JS Парсинг
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

                        // --- ЛОГИКА ФИЛЬТРАЦИИ ---
                        // Если yearStr пустой (мы отключили фильтр), условие пропуска не сработает.
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
                }}''', [limit, year_str_for_js])

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

async def update_results_batch(db_path, results_list):
    if not results_list: return

    ids_to_clean = []
    insert_data = []

    for res in results_list:
        t_id = res['tmdb_id']
        torrents = res['torrents']
        ids_to_clean.append(t_id)

        if len(torrents) > 0:
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
    
    if ids_to_clean:
        async with aiosqlite.connect(db_path) as db:
            placeholders = ','.join('?' * len(ids_to_clean))
            sql_delete = f"DELETE FROM torrents WHERE tmdb_id IN ({placeholders})"
            await db.execute(sql_delete, tuple(ids_to_clean))
            
            if insert_data:
                await db.executemany("""
                    INSERT INTO torrents (tmdb_id, torrent_title, magnet, seeders, leechers, size, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, insert_data)
            await db.commit()
            
            logger.info(f"[BATCH] Updated {len(ids_to_clean)} movies. Found {len(insert_data)} new torrents.")

# ---------------- Main ----------------

async def main():
    if not SOURCE_DB_PATH.exists():
        print(f"ОШИБКА: Не найдена база данных {SOURCE_DB_PATH}")
        return
    
    await init_dest_db()

    print("\n--- TMDB TORRENT UPDATER ---")
    print("1. Обновить все фильмы ТЕКУЩЕГО года (автоматически)")
    print("2. Обновить конкретные фильмы по TMDB ID")
    choice = input("Введите номер режима (1 или 2): ").strip()

    rows = []
    # Переменная, которая решит, использовать фильтр или нет
    use_year_filter = True 
    
    async with aiosqlite.connect(SOURCE_DB_PATH) as db:
        
        if choice == '1':
            current_year = datetime.now().year
            print(f"Выбран режим обновления за {current_year} год. Фильтр по году ВКЛЮЧЕН.")
            async with db.execute(
                "SELECT id, title, year FROM items_minimal WHERE year = ? AND title IS NOT NULL", 
                (current_year,)
            ) as cursor:
                rows = await cursor.fetchall()

        elif choice == '2':
            ids_input = input("Введите TMDB ID через запятую (например: 550, 12345): ")
            
            # --- СПРАШИВАЕМ ПРО ФИЛЬТР ---
            filter_ask = input("Включить строгий фильтр по году? (y/n) [По умолчанию y]: ").lower().strip()
            if filter_ask == 'n':
                use_year_filter = False
                print(">> Фильтр по году ОТКЛЮЧЕН. Будут собраны все результаты поиска.")
            else:
                print(">> Фильтр по году ВКЛЮЧЕН.")

            try:
                target_ids = [int(x.strip()) for x in ids_input.replace(',', ' ').split() if x.strip().isdigit()]
                
                if not target_ids:
                    print("Не введено корректных ID.")
                    return

                placeholders = ','.join('?' * len(target_ids))
                query = f"SELECT id, title, year FROM items_minimal WHERE id IN ({placeholders})"
                
                async with db.execute(query, tuple(target_ids)) as cursor:
                    rows = await cursor.fetchall()
            except Exception as e:
                print(f"Ошибка обработки ID: {e}")
                return
        else:
            print("Неверный выбор. Выход.")
            return

    if not rows:
        print("Фильмы не найдены в базе данных.")
        return

    # Подготовка очереди
    queue = [{'id': r[0], 'title': r[1], 'year': r[2]} for r in rows]
    print(f"Найдено фильмов для обработки: {len(queue)}")
    
    parser = JacredParser(max_concurrent=MAX_CONCURRENT_TABS, headless=True)
    await parser.start()
    
    try:
        with tqdm(total=len(queue), desc="Processing") as pbar:
            for i in range(0, len(queue), BATCH_SIZE):
                batch = queue[i : i + BATCH_SIZE]
                tasks = []
                for m in batch:
                    # ЛОГИКА ПЕРЕДАЧИ ГОДА
                    # Если use_year_filter = True, передаем реальный год из базы.
                    # Если False, передаем 0 (в парсере это превратится в "", и фильтр отключится).
                    target_year = m['year'] if use_year_filter else 0
                    
                    tasks.append(parser.parse_movie(m['id'], m['title'], target_year))
                
                res = await asyncio.gather(*tasks)
                await update_results_batch(DEST_DB_PATH, res)
                pbar.update(len(batch))
    except KeyboardInterrupt:
        print("\nСкрипт остановлен пользователем.")
    finally:
        await parser.stop()
        print("Готово.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
