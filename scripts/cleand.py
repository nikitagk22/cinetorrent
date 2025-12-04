import asyncio
import aiosqlite
from pathlib import Path
from tqdm import tqdm
import time

# --- НАСТРОЙКИ ПУТЕЙ ---
SOURCE_DB_PATH = Path("tmdb_data") / "tmdb_minimal_no_original.db"
DEST_DB_PATH = Path("tmdb_data") / "torrents.db"

async def clean_database():
    start_time = time.time()
    print("🚀 Запуск скрипта очистки базы данных...")
    print(f"📂 Источник эталонных годов: {SOURCE_DB_PATH}")
    print(f"📂 Целевая база (торренты):  {DEST_DB_PATH}")
    print("-" * 50)

    if not SOURCE_DB_PATH.exists() or not DEST_DB_PATH.exists():
        print("❌ ОШИБКА: Файлы баз данных не найдены.")
        return

    # 1. Загружаем эталонные годы для фильмов
    movie_years = {}
    print("📥 Шаг 1: Загружаем годы выхода фильмов...")
    async with aiosqlite.connect(SOURCE_DB_PATH) as db:
        async with db.execute("SELECT id, year FROM items_minimal WHERE year IS NOT NULL") as cursor:
            rows = await cursor.fetchall()
            for r in rows:
                movie_years[r[0]] = str(r[1]) # Сохраняем год как строку
    print(f"   ∟ Загружено фильмов: {len(movie_years)}")

    # 2. Загружаем и проверяем торренты
    print("\n🔍 Шаг 2: Анализ раздач на соответствие году...")
    
    ids_to_delete = []
    total_torrents = 0
    
    async with aiosqlite.connect(DEST_DB_PATH) as db:
        # Получаем все торренты
        async with db.execute("SELECT id, tmdb_id, torrent_title FROM torrents") as cursor:
            torrents = await cursor.fetchall()
            total_torrents = len(torrents)

        # Проходимся по списку
        for t_id, tmdb_id, title in tqdm(torrents, desc="Проверка", unit="rows"):
            target_year = movie_years.get(tmdb_id)
            
            # Если для этого ID у нас нет года в базе фильмов - пропускаем (или удаляем, тут на выбор)
            # Пока оставим (безопасный режим), но можно раскомментировать else, чтобы удалять сирот
            if not target_year:
                continue

            # ЛОГИКА ФИЛЬТРАЦИИ:
            # 1. Проверяем точный год
            if target_year in title:
                continue
            
            # 2. Проверяем смещение на +1 или -1 год (погрешность релизов)
            try:
                year_int = int(target_year)
                if str(year_int + 1) in title or str(year_int - 1) in title:
                    continue
            except:
                pass

            # Если ни одно условие не сработало -> В список на удаление
            ids_to_delete.append(t_id)

        # 3. Удаляем мусор
        print(f"\n🗑 Шаг 3: Удаление некорректных записей...")
        
        if ids_to_delete:
            # Удаляем пачками для скорости
            batch_size = 900
            for i in tqdm(range(0, len(ids_to_delete), batch_size), desc="Удаление из БД"):
                batch = ids_to_delete[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                await db.execute(f"DELETE FROM torrents WHERE id IN ({placeholders})", batch)
            
            await db.commit()
            
            # Сжимаем базу данных после удаления
            print("   ∟ Оптимизация файла БД (VACUUM)...")
            await db.execute("VACUUM")
        else:
            print("   ∟ Удалять нечего, база чиста.")

    # --- ИТОГОВЫЙ ОТЧЕТ ---
    end_time = time.time()
    duration = end_time - start_time
    deleted_count = len(ids_to_delete)
    remaining_count = total_torrents - deleted_count
    percent_deleted = (deleted_count / total_torrents * 100) if total_torrents > 0 else 0

    print("\n" + "="*40)
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("="*40)
    print(f"⏱  Время выполнения:    {duration:.2f} сек")
    print("-" * 40)
    print(f"📦 Всего раздач (БЫЛО): {total_torrents}")
    print(f"❌ Удалено (МУСОР):     {deleted_count} ({percent_deleted:.1f}%)")
    print(f"✅ Всего раздач (СТАЛО):{remaining_count}")
    print("="*40)
    print("Готово. Теперь база содержит только валидные раздачи.")

if __name__ == "__main__":
    try:
        asyncio.run(clean_database())
    except KeyboardInterrupt:
        print("\n⛔ Скрипт остановлен пользователем.")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
