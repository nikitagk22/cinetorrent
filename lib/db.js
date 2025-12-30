const path = require('path');

// Переменные для хранения соединений (Singleton)
const connections = {
    tmdb: null,
    torrents: null,
    data: null,
    cache: null
};

// Константы API
const TMDB_PROXY_BASE = 'https://tmdb.golik-niki.workers.dev/3';
const API_KEY = 'ba43a97bbcb31fb56b46b2966249ab8d'; // Ваш API ключ, если нужен

/**
 * Создает или возвращает подключение к базе данных.
 * Ленивая загрузка better-sqlite3 предотвращает ошибки на клиенте.
 */
function getConnection(type) {
    // 1. Если соединение уже есть, возвращаем его
    if (connections[type]) return connections[type];

    // 2. Лениво импортируем библиотеку ТОЛЬКО здесь
    // Это предотвращает выполнение кода на клиенте
    let Database;
    try {
        Database = require('better-sqlite3');
    } catch (e) {
        console.error('Ошибка загрузки better-sqlite3. Убедитесь, что код выполняется на сервере.');
        throw e;
    }

    // 3. Определяем пути (тоже внутри функции, чтобы process.cwd() не вызывался на клиенте)
    const dbDir = path.join(process.cwd(), 'tmdb_data');
    const DB_PATHS = {
        tmdb: path.join(dbDir, 'tmdb_minimal_no_original.db'),
        torrents: path.join(dbDir, 'torrents.db'),
        data: path.join(dbDir, 'torrents_data.db'),
        cache: path.join(dbDir, 'cache.db')
    };

    const dbPath = DB_PATHS[type];
    const readonly = (type !== 'data' && type !== 'cache' && type !== 'tmdb');

    // 4. Открываем базу
    const db = new Database(dbPath, { readonly, fileMustExist: false });

    // 5. Оптимизация
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
        // Игнорируем ошибки оптимизации
    }

    // 6. Регистрируем кастомные функции
    db.function('lower_utf8', (str) => str ? str.toLowerCase() : '');

    // 1. Вспомогательная функция для генерации триграмм
    const getTrigrams = (str) => {
        const s = ' ' + (str ? str.toLowerCase() : '') + ' ';
        const res = new Set();
        if (s.length < 3) return res;
        for (let i = 0; i < s.length - 2; i++) {
            res.add(s.slice(i, i + 3));
        }
        return res;
    };

    // 2. Регистрируем SQL-функцию similarity (коэффициент Дайса)
    // Возвращает число от 0 (нет совпадений) до 1 (полное совпадение)
    db.function('similarity', (a, b) => {
        if (!a || !b) return 0;
        const setA = getTrigrams(a);
        const setB = getTrigrams(b);
        
        if (setA.size === 0 || setB.size === 0) return 0;

        let intersection = 0;
        for (const item of setA) {
            if (setB.has(item)) intersection++;
        }
        
        return (2 * intersection) / (setA.size + setB.size);
    });

    // 7. Инициализация таблиц для записываемых баз
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

    if (type === 'cache') {
        db.exec(`
            CREATE TABLE IF NOT EXISTS api_cache (
                key TEXT PRIMARY KEY,
                ids_json TEXT,
                updated_at INTEGER
            )
        `);
    }

    connections[type] = db;
    return db;
}

// --- ГЕТТЕРЫ ---
const getTmdbDb = () => getConnection('tmdb');
const getTorrentsDb = () => getConnection('torrents');
const getTorrentDataDb = () => getConnection('data');
const getCacheDb = () => getConnection('cache');


// =========================================================
// ЛОГИКА API + КЭШИРОВАНИЕ
// =========================================================

async function fetchTmdbIds(endpoint) {
    try {
        const separator = endpoint.includes('?') ? '&' : '?';
        let url = `${TMDB_PROXY_BASE}${endpoint}${separator}language=ru-RU`;
        if (API_KEY) url += `&api_key=${API_KEY}`;
        
        const res = await fetch(url);
        if (!res.ok) return [];
        const data = await res.json();
        return data.results ? data.results.map(m => m.id) : [];
    } catch (e) {
        console.error(`[API Error] ${endpoint}:`, e.message);
        return [];
    }
}

async function getMoviesFromApiCache(cacheKey, apiEndpoint, ttlSeconds) {
    const dbCache = getCacheDb();
    const dbTmdb = getTmdbDb();
    const now = Math.floor(Date.now() / 1000);

    // 1. Проверяем кэш
    const cached = dbCache.prepare('SELECT ids_json, updated_at FROM api_cache WHERE key = ?').get(cacheKey);

    let ids = [];

    if (cached && (now - cached.updated_at < ttlSeconds)) {
        ids = JSON.parse(cached.ids_json);
    } else {
        ids = await fetchTmdbIds(apiEndpoint);
        if (ids.length > 0) {
            dbCache.prepare(`
                INSERT OR REPLACE INTO api_cache (key, ids_json, updated_at)
                VALUES (?, ?, ?)
            `).run(cacheKey, JSON.stringify(ids), now);
        }
    }

    if (!ids || ids.length === 0) return [];

    // 2. Получаем фильмы из локальной базы
    const placeholders = ids.map(() => '?').join(',');
    if (placeholders.length === 0) return [];

    const movies = dbTmdb.prepare(`
        SELECT * FROM items_minimal 
        WHERE id IN (${placeholders}) 
        AND id_slug IS NOT NULL 
        AND LENGTH(id_slug) > 0
    `).all(...ids);

    // 3. Сортируем как в API
    const moviesMap = new Map(movies.map(m => [m.id, m]));
    const sortedMovies = ids
        .map(id => moviesMap.get(id))
        .filter(Boolean);

    return sortedMovies;
}

// --- PUBLIC FUNCTIONS ---

function getMovies(options = {}) {
  const { limit = 30, orderBy = 'vote_average', orderDirection = 'DESC', year = null, minVoteCount = null } = options;
  const db = getTmdbDb();
  
  let query = 'SELECT * FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0';
  const params = [];

  if (year) { query += ' AND year = ?'; params.push(year); }
  if (minVoteCount !== null) { query += ' AND vote_count >= ?'; params.push(minVoteCount); }

  query += ` ORDER BY ${orderBy} ${orderDirection} LIMIT ?`;
  params.push(limit);

  return db.prepare(query).all(...params);
}

function fixKeyboardLayout(str) {
    const replacer = {
        "q":"й", "w":"ц", "e":"у", "r":"к", "t":"е", "y":"н", "u":"г", 
        "i":"ш", "o":"щ", "p":"з", "[":"х", "]":"ъ", "a":"ф", "s":"ы", 
        "d":"в", "f":"а", "g":"п", "h":"р", "j":"о", "k":"л", "l":"д", 
        ";":"ж", "'":"э", "z":"я", "x":"ч", "c":"с", "v":"м", "b":"и", 
        "n":"т", "m":"ь", ",":"б", ".":"ю", "&":"?"
    };
    return str.replace(/[A-z/,.;\'\"\[\]\&]/g, (x) => replacer[x.toLowerCase()] || x);
}

function searchMovies(searchQuery, limit = 50) {
  if (!searchQuery || searchQuery.trim().length === 0) return [];
  const db = getTmdbDb();
  
  try {
    const rawQuery = searchQuery.trim();
    // Оставляем фикс раскладки, он полезен (ghbdtn -> привет)
    const fixedQuery = fixKeyboardLayout(rawQuery);

    // Новый SQL запрос:
    // 1. Вычисляет схожесть (match_score) для оригинального запроса и исправленного
    // 2. Берет максимальное значение (MAX)
    // 3. Отсекает совсем мусор (match_score > 0.2)
    // 4. Сортирует: сначала самые похожие, потом популярные
    
    const sqlQuery = `
      SELECT *, 
      MAX(similarity(title, ?), similarity(title, ?)) as match_score
      FROM items_minimal
      WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0
      AND match_score > 0.2
      ORDER BY match_score DESC, vote_count DESC, vote_average DESC
      LIMIT ?
    `;

    // Передаем: оригинальный запрос, исправленный запрос, лимит
    return db.prepare(sqlQuery).all(rawQuery, fixedQuery, limit);
  } catch (error) {
    console.error('Search Error:', error);
    return [];
  }
}

function getMovieByIdSlug(idSlug) {
  return getTmdbDb().prepare('SELECT * FROM items_minimal WHERE id_slug = ?').get(idSlug) || null;
}

function getAllMovieSlugs() {
  return getTmdbDb().prepare('SELECT id_slug FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0').all();
}

function getTorrentsByTmdbId(tmdbId) {
  const dbTorrents = getTorrentsDb();
  const dbData = getTorrentDataDb();

  const torrents = dbTorrents.prepare(
    'SELECT * FROM torrents WHERE tmdb_id = ? ORDER BY seeders DESC, leechers ASC'
  ).all(tmdbId) || [];

  if (torrents.length === 0) return [];

  const hashes = [];
  const torrentsMap = torrents.map(t => {
    const match = t.magnet.match(/btih:([a-fA-F0-9]{40})/);
    const hash = match ? match[1].toUpperCase() : null;
    if (hash) hashes.push(hash);
    return { ...t, info_hash: hash };
  });

  if (hashes.length === 0) return torrentsMap;

  try {
      const placeholders = hashes.map(() => '?').join(',');
      const details = dbData.prepare(`SELECT * FROM torrent_details WHERE info_hash IN (${placeholders})`).all(hashes);
      const detailsMap = new Map(details.map(d => [d.info_hash, d]));

      return torrentsMap.map(t => {
          const detail = detailsMap.get(t.info_hash);
          if (detail) {
              return { 
                  ...t, 
                  resolution: detail.resolution,
                  hdr_type: detail.hdr_type,
                  codec: detail.codec,
                  audio: detail.audio,
                  file_type: detail.file_type,
                  size_bytes: detail.size, 
                  bitrate: detail.bitrate
              };
          }
          return t;
      });
  } catch (e) {
      return torrentsMap;
  }
}

function getRandomMovies(limit = 30, year = null) {
  const db = getTmdbDb();
  let query = 'SELECT * FROM items_minimal WHERE id_slug IS NOT NULL AND LENGTH(id_slug) > 0';
  const params = [];
  if (year) { query += ' AND year = ?'; params.push(year); }
  query += ' ORDER BY RANDOM() LIMIT ?';
  params.push(limit);
  return db.prepare(query).all(...params);
}

function getRandomMoviesByYear(year, limit = 30) {
  return getRandomMovies(limit, year);
}

function getTorrentDetailsByInfoHash(info_hash) {
  const db = getTorrentDataDb();
  try {
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
  } catch (e) { return null; }
}

function insertTorrentDetails(data) {
    const db = getTorrentDataDb();
    db.prepare(`
      INSERT OR REPLACE INTO torrent_details 
      (info_hash, resolution, size, files, hdr_type, file_type, codec, bitrate, audio) 
      VALUES (@info_hash, @resolution, @size, @files, @hdr_type, @file_type, @codec, @bitrate, @audio)
    `).run(data);
}

function getTorrentsForAnalysis(instanceId = 0, totalInstances = 1) {
    return getTorrentsDb().prepare(`SELECT magnet, torrent_title, size, tmdb_id FROM torrents WHERE (rowid % ?) = ?`).all(totalInstances, instanceId);
}

// Новые API-функции
async function getNowPlayingMoviesApi() {
    const ids = await fetchMultiplePages('/movie/now_playing', 3);
    
    if (!ids || ids.length === 0) return [];

    const dbTmdb = getTmdbDb();
    const placeholders = ids.map(() => '?').join(',');
    
    if (placeholders.length === 0) return [];

    const movies = dbTmdb.prepare(`
        SELECT * FROM items_minimal 
        WHERE id IN (${placeholders}) 
        AND id_slug IS NOT NULL 
        AND LENGTH(id_slug) > 0
    `).all(...ids);

    const moviesMap = new Map(movies.map(m => [m.id, m]));
    return ids.map(id => moviesMap.get(id)).filter(Boolean);
}
async function getPopularMoviesApi() {
    const ids = await fetchMultiplePages('/movie/popular', 3);
    
    if (!ids || ids.length === 0) return [];

    const dbTmdb = getTmdbDb();
    const placeholders = ids.map(() => '?').join(',');
    
    if (placeholders.length === 0) return [];

    const movies = dbTmdb.prepare(`
        SELECT * FROM items_minimal 
        WHERE id IN (${placeholders}) 
        AND id_slug IS NOT NULL 
        AND LENGTH(id_slug) > 0
    `).all(...ids);

    const moviesMap = new Map(movies.map(m => [m.id, m]));
    return ids.map(id => moviesMap.get(id)).filter(Boolean);
}
async function getTopRatedMoviesApi() {
    return getMoviesFromApiCache('home_top_rated', '/movie/top_rated', 172800);
}
async function getRecommendationsApi(tmdbId) {
    const ids = await fetchMultiplePages(`/movie/${tmdbId}/recommendations`, 3, 2592000);
    
    if (!ids || ids.length === 0) return [];

    const dbTmdb = getTmdbDb();
    const placeholders = ids.map(() => '?').join(',');
    
    if (placeholders.length === 0) return [];

    const movies = dbTmdb.prepare(`
        SELECT * FROM items_minimal 
        WHERE id IN (${placeholders}) 
        AND id_slug IS NOT NULL 
        AND LENGTH(id_slug) > 0
    `).all(...ids);

    const moviesMap = new Map(movies.map(m => [m.id, m]));
    return ids.map(id => moviesMap.get(id)).filter(Boolean);
}

// Хелпер для получения нескольких страниц сразу
async function fetchMultiplePages(endpoint, pages = 3, ttlSeconds = 172800) {
    let allIds = [];
    const dbCache = getCacheDb(); // Используем кэш, чтобы не бомбить API
    const now = Math.floor(Date.now() / 1000);
    const cacheKey = `multi_${endpoint}_${pages}`;

    // 1. Проверяем кэш для мульти-запроса
    const cached = dbCache.prepare('SELECT ids_json, updated_at FROM api_cache WHERE key = ?').get(cacheKey);
    if (cached && (now - cached.updated_at < ttlSeconds)) { // Используем динамический TTL
        return JSON.parse(cached.ids_json);
    }

    // 2. Если кэша нет, делаем запросы
    try {
        const promises = [];
        for (let i = 1; i <= pages; i++) {
            const separator = endpoint.includes('?') ? '&' : '?';
            let url = `${TMDB_PROXY_BASE}${endpoint}${separator}language=ru-RU&page=${i}`;
            if (API_KEY) url += `&api_key=${API_KEY}`;
            promises.push(fetch(url).then(res => res.ok ? res.json() : null));
        }

        const results = await Promise.all(promises);
        
        results.forEach(data => {
            if (data && data.results) {
                data.results.forEach(m => allIds.push(m.id));
            }
        });

        // Убираем дубликаты ID
        allIds = [...new Set(allIds)];

        // Сохраняем в кэш
        if (allIds.length > 0) {
            dbCache.prepare(`
                INSERT OR REPLACE INTO api_cache (key, ids_json, updated_at)
                VALUES (?, ?, ?)
            `).run(cacheKey, JSON.stringify(allIds), now);
        }

        return allIds;
    } catch (e) {
        console.error('Multi-page Fetch Error:', e);
        return [];
    }
}

// Новая функция для "Сейчас смотрят" (Тренды недели, 3 страницы)
async function getTrendingMoviesApi() {
    // Получаем ~60 ID из трендов
    const ids = await fetchMultiplePages('/trending/movie/week', 3);
    
    if (!ids || ids.length === 0) return [];

    const dbTmdb = getTmdbDb();
    const placeholders = ids.map(() => '?').join(',');
    
    if (placeholders.length === 0) return [];

    // Достаем фильмы из базы
    const movies = dbTmdb.prepare(`
        SELECT * FROM items_minimal 
        WHERE id IN (${placeholders}) 
        AND id_slug IS NOT NULL 
        AND LENGTH(id_slug) > 0
    `).all(...ids);

    // Сортируем в порядке популярности TMDB
    const moviesMap = new Map(movies.map(m => [m.id, m]));
    return ids.map(id => moviesMap.get(id)).filter(Boolean);
}


// --- НОВАЯ ЛОГИКА ДЛЯ ТРЕЙЛЕРОВ ---

/**
 * Пытается найти трейлер через API, если его нет.
 * Возвращает ключ трейлера (или старый, если был).
 * Перманентно сохраняет в БД.
 */
async function ensureMovieTrailer(movie) {
    // 1. Если трейлер уже есть и он не 'none', просто возвращаем его
    if (movie.trailer_key && movie.trailer_key !== 'none') {
        return movie.trailer_key;
    }

    try {
        console.log(`[Trailer] Ищем трейлер для фильма ${movie.title} (${movie.id})...`);
        
        // 2. Запрос к API (сначала пробуем на русском)
        let videoKey = await fetchTrailerKey(movie.id, 'ru-RU');

        // 3. Если на русском нет, пробуем на английском
        if (!videoKey) {
            videoKey = await fetchTrailerKey(movie.id, 'en-US');
        }

        // 4. Если нашли - сохраняем в БД навсегда
        if (videoKey) {
            const db = getTmdbDb();
            db.prepare('UPDATE items_minimal SET trailer_key = ? WHERE id = ?')
              .run(videoKey, movie.id);
            
            console.log(`[Trailer] Трейлер обновлен для ${movie.title}: ${videoKey}`);
            return videoKey;
        } else {
            // Если совсем ничего не нашли, можно записать 'none', чтобы не искать каждый раз
            // Или оставить как есть, чтобы попробовать в следующий раз.
            // Я рекомендую оставить как есть или записать 'none'.
            // db.prepare('UPDATE items_minimal SET trailer_key = ? WHERE id = ?').run('none', movie.id);
            return null;
        }

    } catch (e) {
        console.error('[Trailer] Ошибка при обновлении трейлера:', e);
        return null;
    }
}

// Вспомогательная функция для запроса к API
async function fetchTrailerKey(tmdbId, lang) {
    try {
        const url = `${TMDB_PROXY_BASE}/movie/${tmdbId}/videos?api_key=${API_KEY}&language=${lang}`;
        const res = await fetch(url);
        if (!res.ok) return null;
        
        const data = await res.json();
        const results = data.results || [];

        // Ищем именно Трейлер на YouTube
        const trailer = results.find(v => 
            v.site === 'YouTube' && 
            v.type === 'Trailer'
        );

        return trailer ? trailer.key : null;
    } catch (e) {
        return null;
    }
}

module.exports = {
  getMovies, getMovieByIdSlug, getAllMovieSlugs, getTorrentsByTmdbId,
  getRandomMovies, getRandomMoviesByYear, searchMovies,
  getTorrentDetailsByInfoHash, insertTorrentDetails, getAllInfoHashesFromTorrentsDb,
  getMovieSlugsCount, getMovieSlugsPaginated, getTorrentsForAnalysis,
  getLatestUpdateDate,
  getTorrentDataDb, getTorrentsDb,
  getNowPlayingMoviesApi, getPopularMoviesApi, getTopRatedMoviesApi, getRecommendationsApi,
  getTrendingMoviesApi,
  ensureMovieTrailer
};
