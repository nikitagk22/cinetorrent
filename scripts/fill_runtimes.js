const Database = require('better-sqlite3');
const axios = require('axios');
const path = require('path');

// --- ВАШИ НАСТРОЙКИ ---
const TMDB_API_KEY = 'ba43a97bbcb31fb56b46b2966249ab8d'; 
const DB_PATH = path.join(process.cwd(), 'tmdb_data', 'tmdb_minimal_no_original.db');

async function main() {
  if (TMDB_API_KEY === 'ВАШ_API_KEY_ЗДЕСЬ') {
    console.error('❌ Ошибка: Вставьте API Key в скрипт!');
    return;
  }

  const db = new Database(DB_PATH);
  console.log('🚀 Начинаем заполнение длительности (runtime) для битрейта...');

  // 1. Создаем колонку runtime, если её нет
  try {
    const columns = db.prepare("PRAGMA table_info(items_minimal)").all().map(c => c.name);
    if (!columns.includes('runtime')) {
      db.prepare("ALTER TABLE items_minimal ADD COLUMN runtime INTEGER DEFAULT 0").run();
      console.log('✅ Колонка runtime добавлена.');
    }
  } catch (e) {
    console.log('Info:', e.message);
  }

  // 2. Берем только те фильмы, где длительности НЕТ (или она 0)
  const movies = db.prepare(`
    SELECT id, title FROM items_minimal 
    WHERE runtime IS NULL OR runtime = 0
  `).all();

  const total = movies.length;
  console.log(`📥 Нужно обновить фильмов: ${total}`);

  if (total === 0) {
    console.log('✨ Все длительности уже заполнены!');
    return;
  }

  // Подготовка запроса на обновление
  const updateStmt = db.prepare('UPDATE items_minimal SET runtime = ? WHERE id = ?');

  let success = 0;
  let errors = 0;

  // 3. Погнали обновлять
  for (let i = 0; i < total; i++) {
    const movie = movies[i];

    try {
      // Запрашиваем детали фильма (нам нужен только runtime)
      const response = await axios.get(`https://api.themoviedb.org/3/movie/${movie.id}`, {
        params: { api_key: TMDB_API_KEY, language: 'ru-RU' },
        timeout: 8000
      });

      const runtime = response.data.runtime || 0;

      // Записываем в базу
      updateStmt.run(runtime, movie.id);
      success++;

      // Лог в одну строку
      const percent = Math.round(((i + 1) / total) * 100);
      process.stdout.write(`\r⏳ ${percent}% | ID: ${movie.id} | ${runtime} мин. | ${movie.title ? movie.title.substring(0, 20) : '...'}...    `);

    } catch (err) {
      if (err.response && err.response.status === 404) {
        // Если фильма нет на TMDB, ставим -1, чтобы скрипт не пытался его обновлять вечно
        db.prepare('UPDATE items_minimal SET runtime = -1 WHERE id = ?').run(movie.id);
      }
      errors++;
    }

    // Небольшая пауза, чтобы API не забанил (40-50 запросов в сек лимит, мы делаем медленнее)
    await new Promise(r => setTimeout(r, 50));
  }

  console.log('\n\n🎉 Готово! Длительность записана.');
  db.close();
}

main();