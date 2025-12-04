#!/usr/bin/env python3
"""
tmdb_fast_loader.py

Скрипт синхронизации базы TMDB (MAX SPEED + MAX RUSSIAN).

Особенности:
- Скорость: ~30-32 запроса в секунду (близко к лимиту API).
- Язык: Агрессивный поиск русского языка (Translations -> Alt Titles -> Taglines).
- Картинки: Параллельное скачивание и конвертация в WebP (8 потоков).
"""

import os
import gzip
import json
import shutil
import asyncio
import time
import random
import logging
from pathlib import Path
from collections import deque
from typing import Optional, List, Tuple, Set
from datetime import datetime, timedelta

import requests
import aiohttp
import aiosqlite
from dotenv import load_dotenv
from tqdm import tqdm
from PIL import Image, ImageFile

# Разрешаем загрузку поврежденных изображений (бывает при сбоях сети)
ImageFile.LOAD_TRUNCATED_IMAGES = True

# ---------------- Config (MAX SPEED) ----------------
load_dotenv()
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise SystemExit("TMDB_API_KEY not set in .env or environment.")

LANG_FULL = os.getenv("LANGUAGE", "ru-RU")
TARGET_ISO_LANG = "ru"  # Код языка для поиска в массиве translations

DATA_DIR = Path("tmdb_data")
TEMP_POSTERS_DIR = Path("temp_posters")
POSTERS_DIR = Path("/files/posters")
DB_PATH = DATA_DIR / "tmdb_minimal_no_original.db"

DATA_DIR.mkdir(exist_ok=True)
TEMP_POSTERS_DIR.mkdir(exist_ok=True)
POSTERS_DIR.mkdir(parents=True, exist_ok=True)

DUMP_BASE_URL = "https://files.tmdb.org/p/exports/"
IMAGE_BASE = "https://image.tmdb.org/t/p/original"
TMDB_API_BASE = "https://api.themoviedb.org/3"

# --- НАСТРОЙКИ СКОРОСТИ ---
# 95 запросов за 3 секунды. Это ~31.6 rps.
# Если будут ошибки 429, TMDB попросит подождать, скрипт это умеет.
MAX_REQUESTS_PER_WINDOW = 95
WINDOW_SECONDS = 3.0

# 70 одновременных задач (чтобы пока одни качают картинки, другие слали запросы)
CONCURRENT_WORKERS = 70

# Количество воркеров для обработки картинок (CPU bound)
# Если сервер мощный, можно увеличить. 8 - оптимально для большинства VPS.
CONVERSION_WORKERS = 8

REQUEST_RETRIES = 3
DOWNLOAD_RETRIES = 3

# Очередь для конвертации изображений
conversion_queue = asyncio.Queue()

# ---------------- Rate Limiter ----------------
class SlidingWindowRateLimiter:
    def __init__(self, max_calls: int, window_seconds: float):
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls = deque()
        self._lock = asyncio.Lock()

    async def wait_for_slot(self):
        async with self._lock:
            now = time.monotonic()
            # Удаляем старые вызовы за пределами окна
            while self.calls and (now - self.calls[0]) > self.window:
                self.calls.popleft()
            
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            
            # Если лимит исчерпан, ждем освобождения слота
            earliest = self.calls[0]
            wait_for = self.window - (now - earliest)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            
            # После сна снова чистим и добавляем текущий
            now2 = time.monotonic()
            while self.calls and (now2 - self.calls[0]) > self.window:
                self.calls.popleft()
            self.calls.append(now2)

rate_limiter = SlidingWindowRateLimiter(MAX_REQUESTS_PER_WINDOW, WINDOW_SECONDS)

# ---------------- Dump Helpers ----------------
def find_valid_dump_filename(prefix: str) -> Optional[str]:
    current_date = datetime.now()
    for _ in range(3):
        date_str = current_date.strftime("%m_%d_%Y")
        filename = f"{prefix}{date_str}.json.gz"
        url = DUMP_BASE_URL + filename
        try:
            # Короткий тайм-аут для проверки заголовков
            resp = requests.head(url, timeout=5)
            if resp.status_code == 200:
                logging.info(f"Dump found: {filename}")
                return filename
        except Exception:
            pass
        current_date -= timedelta(days=1)
    return None

def download_dump_if_needed(filename: str) -> Optional[Path]:
    local_gz = DATA_DIR / filename
    local_json = DATA_DIR / filename.replace(".gz", "")
    
    if local_json.exists() and local_json.stat().st_size > 0:
        return local_json

    url = DUMP_BASE_URL + filename
    logging.info(f"Downloading dump: {url}")
    try:
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(local_gz, "wb") as f:
                for chunk in r.iter_content(1024 * 128):
                    f.write(chunk)
        
        logging.info("Extracting dump...")
        with gzip.open(local_gz, "rb") as gz, open(local_json, "wb") as out:
            shutil.copyfileobj(gz, out)
        local_gz.unlink(missing_ok=True)
        return local_json
    except Exception as e:
        logging.error(f"Failed to download dump {filename}: {e}")
        return None

def extract_ids_from_dump(path: Path) -> Set[int]:
    ids = set()
    if not path.exists():
        return ids
    logging.info(f"Parsing IDs from {path.name}...")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            try:
                obj = json.loads(line)
                if "id" in obj:
                    ids.add(int(obj["id"]))
            except json.JSONDecodeError:
                continue
    return ids

# ---------------- DB Helpers ----------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS items_minimal (
    id INTEGER,
    media_type TEXT,
    title TEXT,
    overview TEXT,
    year INTEGER,
    genres TEXT,
    production_countries TEXT,
    vote_average REAL,
    vote_count INTEGER,
    local_poster_path TEXT,
    PRIMARY KEY(id, media_type)
);"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;") # Немного ускоряет запись
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()

async def get_existing_db_ids(db_path: Path) -> Set[Tuple[str, int]]:
    existing = set()
    if not db_path.exists():
        return existing
    logging.info("Reading existing IDs from Database...")
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT media_type, id FROM items_minimal") as cursor:
            async for row in cursor:
                existing.add((row[0], row[1]))
    logging.info(f"Found {len(existing)} items in DB.")
    return existing

async def save_item_minimal(db: aiosqlite.Connection, item: dict):
    vals = (
        item.get("id"),
        item.get("media_type"),
        item.get("title"),
        item.get("overview"),
        item.get("year"),
        item.get("genres"),
        item.get("production_countries"),
        item.get("vote_average"),
        item.get("vote_count"),
        item.get("local_poster_path"),
    )
    await db.execute("""
    INSERT OR REPLACE INTO items_minimal
    (id, media_type, title, overview, year, genres, production_countries, vote_average, vote_count, local_poster_path)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, vals)
    await db.commit()

# ---------------- Image Conversion ----------------
async def poster_conversion_worker(worker_id: int):
    while True:
        task = await conversion_queue.get()
        if task is None:
            conversion_queue.task_done()
            break
        
        temp_path, media_type, item_id = task
        webp_name = f"{media_type}_{item_id}.webp"
        webp_path = POSTERS_DIR / webp_name
        
        try:
            await asyncio.to_thread(convert_image, temp_path, webp_path)
        except Exception:
            # Если конвертация упала, пробуем просто переместить исходник
            try:
                shutil.move(str(temp_path), str(POSTERS_DIR / temp_path.name))
            except: pass
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except: pass
            conversion_queue.task_done()

def convert_image(source: Path, dest: Path):
    img = Image.open(source)
    # Сохраняем в WebP с оптимизацией
    img.save(dest, "WEBP", quality=80, optimize=True, method=4)

# ---------------- HTTP Helpers ----------------
async def aio_get_json(session: aiohttp.ClientSession, url: str, params: dict = None):
    for _ in range(REQUEST_RETRIES):
        try:
            await rate_limiter.wait_for_slot()
            async with session.get(url, params=params, timeout=30) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                logging.warning(f"Rate limit hit. Sleeping {retry_after}s...")
                await asyncio.sleep(retry_after + 1)
            else:
                await asyncio.sleep(1)
        except Exception:
            await asyncio.sleep(1)
    return None

async def aio_get_bytes(session: aiohttp.ClientSession, url: str):
    for _ in range(DOWNLOAD_RETRIES):
        try:
            await rate_limiter.wait_for_slot()
            async with session.get(url, timeout=45) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                return await resp.read()
        except Exception:
            await asyncio.sleep(1)
    return None

# ---------------- Logic Helper: is_cyrillic ----------------
def is_cyrillic(text: str) -> bool:
    if not text: return False
    # Проверка наличия хотя бы одной русской буквы
    return any('а' <= char.lower() <= 'я' for char in text)

# ---------------- Item Processing ----------------
async def process_item(session: aiohttp.ClientSession, sem: asyncio.Semaphore, db: aiosqlite.Connection,
                       media_type: str, item_id: int):
    async with sem:
        url = f"{TMDB_API_BASE}/{media_type}/{item_id}"
        
        # Запрашиваем максимум данных за один раз
        params = {
            "api_key": TMDB_API_KEY,
            "language": LANG_FULL,
            "append_to_response": "translations,alternative_titles"
        }
        
        details = await aio_get_json(session, url, params)
        if details is None:
            return

        # 1. Данные из основного запроса
        original_title = details.get("original_title") if media_type == "movie" else details.get("original_name")
        base_title = details.get("title") if media_type == "movie" else details.get("name")
        
        title = base_title or original_title or ""
        overview = details.get("overview", "")
        tagline = details.get("tagline", "")

        # 2. Поиск в переводах (Translations) - самый надежный источник
        translations_data = details.get("translations", {}).get("translations", [])
        found_ru_translation = False

        for t in translations_data:
            if t.get("iso_639_1") == TARGET_ISO_LANG:
                t_data = t.get("data", {})
                
                if t_data.get("overview"):
                    overview = t_data.get("overview")
                
                t_title = t_data.get("title") if media_type == "movie" else t_data.get("name")
                if t_title:
                    title = t_title
                    found_ru_translation = True
                
                if t_data.get("tagline"):
                    tagline = t_data.get("tagline")
                break # Обычно блок ru один, нашли - выходим

        # 3. Поиск в Альтернативных названиях (Alternative Titles)
        # Ищем только если заголовок всё еще не на кириллице
        if not found_ru_translation or not is_cyrillic(title):
            alt_block = details.get("alternative_titles", {})
            alts = alt_block.get("titles", []) if media_type == "movie" else alt_block.get("results", [])
            
            for alt in alts:
                # Ищем по стране RU или коду языка ru
                if alt.get("iso_3166_1") == "RU" or alt.get("iso_639_1") == "ru":
                    alt_t = alt.get("title", "")
                    if alt_t and is_cyrillic(alt_t):
                        title = alt_t
                        break

        # 4. Фоллбэк: Если описания нет, используем слоган (если он на русском)
        if not overview and tagline and is_cyrillic(tagline):
            overview = tagline

        # 5. Если совсем ничего нет - оригинал
        if not title:
            title = original_title

        # Сборка остальных данных
        release_date = details.get("release_date") if media_type == "movie" else details.get("first_air_date")
        year = int(release_date.split("-")[0]) if release_date and "-" in release_date else None

        genres = details.get("genres") or []
        genre_names = ", ".join([g["name"] for g in genres if g.get("name")])
        
        prod = details.get("production_countries") or []
        prod_iso = ",".join([p.get("iso_3166_1") for p in prod if p.get("iso_3166_1")]) or None

        local_path_str = None
        poster_path = details.get("poster_path")
        
        if poster_path:
            ext = poster_path.split(".")[-1] if "." in poster_path else "jpg"
            poster_url = IMAGE_BASE + poster_path
            webp_filename = f"{media_type}_{item_id}.webp"
            
            # Скачиваем постер
            img_bytes = await aio_get_bytes(session, poster_url)
            if img_bytes:
                temp_file = TEMP_POSTERS_DIR / f"{media_type}_{item_id}.{ext}"
                try:
                    temp_file.write_bytes(img_bytes)
                    # Отправляем в очередь на конвертацию
                    await conversion_queue.put((temp_file, media_type, item_id))
                    local_path_str = str(POSTERS_DIR / webp_filename)
                except Exception as e:
                    logging.error(f"Write error {item_id}: {e}")

        item_data = {
            "id": item_id,
            "media_type": media_type,
            "title": title,
            "overview": overview,
            "year": year,
            "genres": genre_names,
            "production_countries": prod_iso,
            "vote_average": details.get("vote_average"),
            "vote_count": details.get("vote_count"),
            "local_poster_path": local_path_str
        }

        try:
            await save_item_minimal(db, item_data)
        except Exception as e:
            logging.error(f"DB error {item_id}: {e}")

# ---------------- Main ----------------
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    await init_db()

    logging.info("--- Step 1: Finding dumps ---")
    movie_dump = find_valid_dump_filename("movie_ids_")
    tv_dump = find_valid_dump_filename("tv_series_ids_")
    
    path_movies = download_dump_if_needed(movie_dump) if movie_dump else None
    path_tv = download_dump_if_needed(tv_dump) if tv_dump else None

    logging.info("--- Step 2: Loading IDs ---")
    dump_movies = extract_ids_from_dump(path_m
