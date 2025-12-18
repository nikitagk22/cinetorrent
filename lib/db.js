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
    if (connections[type]) return connections[type];

    const dbPath = DB_PATHS[type];
    const readonly = type !== 'data'; 

    const db = new Database(dbPath, { readonly, fileMustExist: false });

    try {
        db.pragma('mmap_size = 30000000000'); 
        db.pragma('busy_timeout = 10000');

        if (!readonly) {
            db.pragma('journal_mode = WAL');
            db.pragma('synchronous = NORMAL');
        } else {
            db.pragma('synchronous = NORMAL');
        }
        
    } catch (e) {
        console.warn(`[DB Warning] Оптимизация для ${type} частично пропущена:`, e.message);
    }

    // --- ГЛАВНОЕ ИЗМЕНЕНИЕ ---
    // Регистрируем JS-функцию для правильного перевода в нижний регистр (понимает русские буквы)
    db.function('lower_utf8', (str) => str ? str.toLowerCase() : '');
    // -------------------------

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

// Вспомогательная функция (добавьте её ПЕРЕД searchMovies)
function fixKeyboardLayout(str) {
    const replacer = {
        "q":"й", "w":"ц", "e":"у", "r":"к", "t":"е", "y":"н", "u":"г", 
        "i":"ш", "o":"щ", "p":"з", "[":"х", "]":"ъ", "a":"ф", "s":"ы", 
        "d":"в", "f":"а", "g":"п", "h":"р", "j":"о", "k":"л", "l":"д", 
        ";":"ж", "'":"э", "z":"я", "x":"ч", "c":"с", "v":"м", "b":"и", 
        "n":"т", "m":"ь", ",":"б", ".":"ю", "&":"?"
    };
    return str.replace(/[A-z/,.;\'\[\]\&]/g, (x) => replacer[x.toLowerCase()] || x);
}

function searchMovies(searchQuery, limit = 50) {
  if (!searchQuery || searchQuery.trim().length === 0) return [];
  
  const db = getTmdbDb();

  try {
    const rawQuery = searchQuery.trim();
    const rawQueryLower = rawQuery.toLowerCase(); // Сразу переводим в нижний регистр JS-ом

    // 1. Исправляем раскладку и тоже переводим в нижний регистр
    const fixedQuery = fixKeyboardLayout(rawQuery);
    const fixedQueryLower = fixedQuery.toLowerCase();

    // 2. Разбиваем на слова
    const rawTerms = rawQueryLower.split(/\s+/).filter(t => t.length > 0);
    const fixedTerms = fixedQueryLower.split(/\s+/).filter(t => t.length > 0);

    // Генератор условий: используем нашу новую функцию lower_utf8(title)
    const createTermConditions = (terms) => {
        return terms.map(() => `lower_utf8(title) LIKE '%' || ? || '%'`).join(' AND ');
    };

    const sqlQuery = `
      SELECT *, 
      CASE 
        -- 0. Полное совпадение ("Мстители" == "Мстители")
        WHEN lower_utf8(title) = ? THEN 0
        -- 1. Начинается с фразы ("Мстители: Финал" начинается с "Мстители")
        WHEN lower_utf8(title) LIKE ? || '%' THEN 1
        -- 2. Просто содержит слово ("Токийские мстители")
        ELSE 2
      END as search_rank
      FROM items_minimal
      WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0
      AND (
          (${createTermConditions(rawTerms)}) 
          OR 
          (${createTermConditions(fixedTerms)})
      )
      ORDER BY search_rank ASC, vote_count DESC, vote_average DESC
      LIMIT ?
    `;

    const params = [];
    
    // Параметры для сортировки (CASE)
    params.push(rawQueryLower); // Для точного совпадения (=)
    params.push(rawQueryLower); // Для начала строки (LIKE ...%)

    // Параметры для поиска (WHERE) - передаем слова
    rawTerms.forEach(term => params.push(term));
    fixedTerms.forEach(term => params.push(term));
    
    params.push(limit);

    return db.prepare(sqlQuery).all(...params);

  } catch (error) {
    console.error('Search Error:', error);
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
  // ДОБАВИЛИ: выборку поля updated_at
  return getTmdbDb().prepare(`
    SELECT id_slug, updated_at 
    FROM items_minimal 
    WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0 
    ORDER BY id 
    LIMIT ? OFFSET ?
  `).all(limit, offset);
}

function getLatestUpdateDate() {
  try {
    const result = getTmdbDb().prepare('SELECT MAX(updated_at) as last_update FROM items_minimal').get();
    return result ? result.last_update : null;
  } catch (e) {
    return null;
  }
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
  getLatestUpdateDate,
  getTorrentDataDb, getTorrentsDb
};