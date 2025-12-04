import sqlite3
from pathlib import Path

DB_PATH = Path("tmdb_data") / "torrents.db"

def check_everything():
    if not DB_PATH.exists():
        print(f"❌ База {DB_PATH} еще не создана. Запусти сначала парсер!")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Считаем записи
        cursor.execute("SELECT COUNT(*) FROM torrents")
        count = cursor.fetchone()[0]
        print(f"📊 Всего записей в базе: {count}")
        
        if count == 0:
            print("⏳ База создана, но пока пустая. Подожди немного...")
            return

        print("=" * 100)
        print(f"{'S/L':<8} | {'Source URL':<30} | {'Movie Title'}")
        print("=" * 100)

        # Берем последние 10 записей
        cursor.execute("""
            SELECT seeders, leechers, url, torrent_title, magnet 
            FROM torrents 
            ORDER BY id DESC 
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        for row in rows:
            seeds = row[0]
            leechs = row[1]
            url = row[2]
            title = row[3]
            magnet = row[4]

            # Красивое форматирование URL (оставляем только домен и начало)
            short_url = url.replace("https://", "").replace("http://", "")
            if len(short_url) > 28:
                short_url = short_url[:25] + "..."
            if not short_url:
                short_url = "[No URL]"

            # Обрезаем название
            if len(title) > 40:
                title = title[:37] + "..."

            print(f"⬆{seeds} ⬇{leechs:<3} | {short_url:<30} | {title}")
            print(f"   🧲 Magnet: {magnet[:60]}...") # Показываем начало магнета
            print("-" * 100)

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_everything()
