const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(process.cwd(), 'tmdb_data', 'torrents.db');
console.log('Open:', dbPath);
const db = new Database(dbPath);

console.log('⏳ Создание индекса для tmdb_id...');
// Индекс ускорит поиск торрентов для фильма в 1000 раз
db.exec('CREATE INDEX IF NOT EXISTS idx_tmdb_id ON torrents(tmdb_id);');

console.log('✅ Индекс создан!');
db.close();