const Database = require('better-sqlite3');
const path = require('path');

// --- НАСТРОЙКИ ПУТЕЙ ---
const DB_PATHS = {
    tmdb: path.join(process.cwd(), 'tmdb_data', 'tmdb_minimal_no_original.db'),
    torrents: path.join(process.cwd(), 'tmdb_data', 'torrents.db'),
    data: path.join(process.cwd(), 'tmdb_data', 'torrents_data.db')
};

// Хранилище подключений (Singleton)
// Мы храним соединения открытыми, чтобы не тратить время на открытие файла при каждом запросе
const connections = {
    tmdb: null,
    torrents: null,
    data: null
};

/**
 * Создает оптимизированное подключение к базе.
 * Адаптировано под Ubuntu Server (Performance) и Windows (Dev).
 */
function getConnection(type) {
    // 1. Если подключение уже есть в памяти — отдаем его мгновенно
    if (connections[type]) return connections[type];

    const dbPath = DB_PATHS[type];
    // Базы 'tmdb' и 'torrents' открываем ТОЛЬКО ДЛЯ ЧТЕНИЯ (безопасность и скорость)
    // Базу 'data' открываем для записи (туда пишут скрипты и кэши)
    const readonly = type !== 'data'; 

    // Открываем базу
    // fileMustExist: true гарантирует, что мы не создадим пустой файл вместо ошибки
    const db = new Database(dbPath, { readonly, fileMustExist: false });

    try {
        // --- ОПТИМИЗАЦИИ ДЛЯ СЕРВЕРА ---
        
        // 1. Memory Map (MMAP) - КРИТИЧНО ДЛЯ СКОРОСТИ НА UBUNTU
        // Позволяет SQLite читать файл напрямую через оперативную память OS.
        // Ставим лимит 30GB (возьмет сколько сможет).
        db.pragma('mmap_size = 30000000000'); 
        
        // 2. Ждем разблокировки файла до 10 секунд (полезно при высокой нагрузке)
        db.pragma('busy_timeout = 10000');

        // 3. Настройки журналирования
        if (!readonly) {
            // Если база для записи - включаем WAL принудительно
            db.pragma('journal_mode = WAL');
            db.pragma('synchronous = NORMAL'); // Баланс скорости и надежности
        } else {
            // Если база READONLY (Windows Fix):
            // Мы НЕ пытаемся выполнить 'journal_mode = WAL', так как это операция записи.
            // Но мы ставим synchronous = NORMAL, это разрешено.
            // Примечание: Если база была оптимизирована скриптом optimize_all.js,
            // она УЖЕ находится в WAL режиме, и SQLite это поймет автоматически.
            db.pragma('synchronous = NORMAL');
        }
        
    } catch (e) {
        console.warn(`[DB Warning] Оптимизация для ${type} частично пропущена:`, e.message);
    }

    // Авто-создание таблицы только для записываемой базы
    if (type === 'data') {
        db.exec(`
            CREATE TABLE IF NOT EXISTS torrent_details (
                info_hash TEXT PRIMARY KEY,
                resolution TEXT, size INTEGER, files TEXT, 
                hdr_type TEXT, file_type TEXT, codec TEXT, 
                bitrate REAL, audio TEXT
            )
        `);
    }

    // Сохраняем подключение в глобальную переменную
    connections[type] = db;
    return db;
}

// --- ГЕТТЕРЫ (Используем их везде в коде) ---
const getTmdbDb = () => getConnection('tmdb');
const getTorrentsDb = () => getConnection('torrents');
const getTorrentDataDb = () => getConnection('data');


// =========================================================
// ФУНКЦИИ САЙТА
// (Singleton: соединение не закрывается, работает максимально быстро)
// =========================================================

function getMovies(options = {}) {
  const { limit = 20, orderBy = 'vote_average', orderDirection = 'DESC', year = null, minVoteCount = null } = options;
  const allowedOrderBy = ['vote_average', 'vote_count', 'year', 'title', 'id'];
  const safeOrderBy = allowedOrderBy.includes(orderBy) ? orderBy : 'vote_average';
  const safeOrderDirection = orderDirection.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

  const db = getTmdbDb();
  
  let query = 'SELECT * FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0';
  const params = [];

  if (year) { query += ' AND year = ?'; params.push(year); }
  if (minVoteCount !== null) { query += ' AND vote_count >= ?'; params.push(minVoteCount); }

  query += ` ORDER BY ${safeOrderBy} ${safeOrderDirection} LIMIT ?`;
  params.push(limit);

  return db.prepare(query).all(...params);
}

function searchMovies(searchQuery, limit = 50) {
  if (!searchQuery || searchQuery.trim().length === 0) return [];
  const db = getTmdbDb();
  
  try {
    const term = searchQuery.trim();
    const lowerTerm = term.toLowerCase();
    const capitalizedTerm = lowerTerm.charAt(0).toUpperCase() + lowerTerm.slice(1);
    
    const startLower = `${lowerTerm}%`;
    const startCap = `${capitalizedTerm}%`;
    const containsLower = `%${lowerTerm}%`;
    const containsCap = `%${capitalizedTerm}%`;

    const sqlQuery = `
      SELECT *, 
      CASE
        WHEN title LIKE ? OR title LIKE ? THEN 1
        ELSE 2
      END as relevance
      FROM items_minimal
      WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0
        AND (title LIKE ? OR title LIKE ? OR title LIKE ? OR title LIKE ?)
      ORDER BY relevance ASC, vote_count DESC, vote_average DESC
      LIMIT ?
    `;
    return db.prepare(sqlQuery).all(startLower, startCap, startLower, startCap, containsLower, containsCap, limit);
  } catch (error) {
    return [];
  }
}

function getMovieByIdSlug(idSlug) {
  // .get() вернет undefined, если не найдено, поэтому || null
  return getTmdbDb().prepare('SELECT * FROM items_minimal WHERE id_slug = ?').get(idSlug) || null;
}

function getAllMovieSlugs() {
  return getTmdbDb().prepare('SELECT id_slug FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0').all();
}

function getTorrentsByTmdbId(tmdbId) {
  const dbTorrents = getTorrentsDb();
  const dbData = getTorrentDataDb();

  // 1. Получаем список торрентов из основной базы
  const torrents = dbTorrents.prepare(
    'SELECT * FROM torrents WHERE tmdb_id = ? ORDER BY seeders DESC, leechers ASC'
  ).all(tmdbId) || [];

  if (torrents.length === 0) return [];

  // 2. Собираем все info_hash в массив
  const hashes = [];
  const torrentsMap = torrents.map(t => {
    const match = t.magnet.match(/btih:([a-fA-F0-9]{40})/);
    const hash = match ? match[1].toUpperCase() : null;
    if (hash) hashes.push(hash);
    return { ...t, info_hash: hash };
  });

  if (hashes.length === 0) return torrentsMap;

  // 3. БАТЧ-ЗАПРОС: Получаем детали для ВСЕХ хешей за один раз
  // Используем "IN (?,?,?)"
  try {
      const placeholders = hashes.map(() => '?').join(',');
      const detailsQuery = `SELECT * FROM torrent_details WHERE info_hash IN (${placeholders})`;
      
      const details = dbData.prepare(detailsQuery).all(hashes);
      
      // Создаем Map для быстрого поиска: hash -> details
      const detailsMap = new Map();
      details.forEach(d => detailsMap.set(d.info_hash, d));

      // 4. Объединяем данные (Merge)
      return torrentsMap.map(t => {
          const detail = detailsMap.get(t.info_hash);
          if (detail) {
              // Добавляем поля из базы деталей к объекту торрента
              return { 
                  ...t, 
                  resolution: detail.resolution,
                  hdr_type: detail.hdr_type,
                  codec: detail.codec,
                  audio: detail.audio,
                  file_type: detail.file_type,
                  size_bytes: detail.size, // Точный размер в байтах
                  bitrate: detail.bitrate
              };
          }
          return t;
      });

  } catch (e) {
      console.error("Batch details error:", e);
      return torrentsMap;
  }
}

function getRandomMovies(limit = 20, year = null) {
  const db = getTmdbDb();
  let query = 'SELECT * FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0';
  const params = [];
  if (year) { query += ' AND year = ?'; params.push(year); }
  query += ' ORDER BY RANDOM() LIMIT ?';
  params.push(limit);
  return db.prepare(query).all(...params);
}

function getRandomMoviesByYear(year, limit = 20) {
  return getRandomMovies(limit, year);
}

function getTorrentDetailsByInfoHash(info_hash) {
  const db = getTorrentDataDb();
  try {
      // COLLATE NOCASE делает поиск нечувствительным к регистру
      return db.prepare('SELECT * FROM torrent_details WHERE info_hash = ? COLLATE NOCASE').get(info_hash) || null;
  } catch (e) { return null; }
}

function getAllInfoHashesFromTorrentsDb() {
  return getTorrentsDb().prepare('SELECT DISTINCT magnet FROM torrents WHERE magnet IS NOT NULL').all().map(row => {
    const match = row.magnet.match(/btih:([a-fA-F0-9]{40})/);
    return match ? match[1].toUpperCase() : null;
  }).filter(Boolean);
}

function getMovieSlugsCount() {
  return getTmdbDb().prepare('SELECT COUNT(id) as count FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0').get().count;
}

function getMovieSlugsPaginated({ limit, offset }) {
  return getTmdbDb().prepare('SELECT id_slug FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0 ORDER BY id LIMIT ? OFFSET ?').all(limit, offset);
}

// =========================================================
// ФУНКЦИИ ДЛЯ СКРИПТОВ (Запись / Шардинг)
// =========================================================

function insertTorrentDetails(data) {
    const db = getTorrentDataDb();
    const stmt = db.prepare(`
      INSERT OR REPLACE INTO torrent_details 
      (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
      VALUES (@info_hash, @resolution, @size, @files, @hdr_type, @file_type, @codec, @bitrate, @audio)
    `);
    stmt.run(data);
}

function getTorrentsForAnalysis(instanceId = 0, totalInstances = 1) {
    // Эта функция нужна для воркеров, если вы снова решите их запустить
    const sql = `SELECT magnet, torrent_title, size, tmdb_id FROM torrents WHERE (rowid % ?) = ?`;
    return getTorrentsDb().prepare(sql).all(totalInstances, instanceId);
}

// Экспорт
module.exports = {
  getMovies, getMovieByIdSlug, getAllMovieSlugs, getTorrentsByTmdbId,
  getRandomMovies, getRandomMoviesByYear, searchMovies,
  getTorrentDetailsByInfoHash, insertTorrentDetails, getAllInfoHashesFromTorrentsDb,
  getMovieSlugsCount, getMovieSlugsPaginated, getTorrentsForAnalysis,
  getTorrentDataDb, getTorrentsDb
};