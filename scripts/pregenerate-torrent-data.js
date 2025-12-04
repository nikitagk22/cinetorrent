// 1. Глобальные перехватчики (чтобы не падал от ошибок WebTorrent)
process.on('uncaughtException', (err) => {
  if (err.name === 'AbortError' || err.message.includes('aborted') || err.code === 'ABORT_ERR') return;
  // console.error('\n[Uncaught Exception]', err.message); // Можно раскомментировать для отладки
});

process.on('unhandledRejection', (reason, promise) => {
  if (reason && (reason.name === 'AbortError' || reason.message?.includes('aborted'))) return;
});

require('events').EventEmitter.defaultMaxListeners = 0;

const readline = require('readline');
const path = require('path');
const fs = require('fs'); // <--- ВАЖНО: Для проверки наличия файла БД
const Database = require('better-sqlite3'); 
const ptt = require('parse-torrent-title'); 

const {
  getTorrentsForAnalysis,
  insertTorrentDetails,
} = require('../lib/db');

// 1. Читаем аргументы запуска: node script.js [НОМЕР] [ВСЕГО]
// Пример: node script.js 0 4 (Запуск первого из 4-х потоков)
const INSTANCE_ID = parseInt(process.argv[2]) || 0;
const TOTAL_INSTANCES = parseInt(process.argv[3]) || 1;

// --- НАСТРОЙКИ (ОПТИМИЗИРОВАННЫЕ ДЛЯ XEON) ---
// Уменьшаем лимит потоков внутри скрипта, так как запустим несколько скриптов
const CONCURRENCY_LIMIT = 25;     
const MAGNET_TIMEOUT = 12000;     // 12 сек
const UI_UPDATE_RATE = 1000;      // Реже обновляем консоль, чтобы не мерцало

const TRACKERS = [
  'udp://tracker.opentrackr.org:1337/announce',
  'udp://tracker.torrent.eu.org:451/announce',
  'udp://tracker.qu.ax:6969/announce',
  'udp://open.demonoid.ch:6969/announce',
  'wss://tracker.openwebtorrent.com/announce',
  'wss://tracker.btorrent.xyz/announce',
  'https://tracker.yemekyedim.com:443/announce',
];

class NullStore {
  constructor (chunkLength, opts) { this.chunkLength = chunkLength }
  put (index, buf, cb) { if (cb) cb(null) }
  get (index, opts, cb) { if (cb) cb(new Error('Storage disabled')) }
  close (cb) { if (cb) cb(null) }
  destroy (cb) { if (cb) cb(null) }
}

// --- ХЕЛПЕР: Быстрая загрузка уже обработанных хешей ---
function getExistingHashesSet() {
    const dbPath = path.join(process.cwd(), 'tmdb_data', 'torrents_data.db');
    
    // 1. Если файла нет, значит база пустая
    if (!fs.existsSync(dbPath)) {
        return new Set();
    }

    let db;
    try {
        db = new Database(dbPath, { readonly: true });
        
        // 2. Проверяем наличие таблицы (на случай битого файла)
        const check = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='torrent_details'").get();
        if (!check) return new Set();

        // 3. Выгружаем все хеши в память (это очень быстро)
        const rows = db.prepare('SELECT info_hash FROM torrent_details').all();
        return new Set(rows.map(row => row.info_hash));
    } catch (e) {
        return new Set();
    } finally {
        if (db) db.close();
    }
}

// --- 1. Статический анализ заголовка (НОВАЯ ВЕРСИЯ С ВАШИМ REGEX) ---

/**
 * Конфигурация парсера (портирована из вашего HTML примера)
 */
const PARSER_CONFIG = {
    resolution: {
        regex: /\b(3840x2160|4K|2160p|1920x1080|1080p|1280x720|720p)\b/ig,
        type: 'resolution'
    },
    audio_channels: {
        regex: /\b(5\.1|7\.1)\b/ig,
        type: 'audio_channels'
    },
    quality: {
        regex: /\b(HEVC|HDR10\+|HDR10|HDR|Dolby Vision|DV|BDRemux|BluRay|Web-DL|Hybrid|IMAX)\b/ig,
        type: 'quality'
    },
    audio_track: {
        // Огромный список студий и типов озвучек.
        // Важно: Фразы с пробелами (Red Head Sound) должны идти раньше коротких (RHS), чтобы RegExp жадно захватил длинное.
        regex: /\b(Red Head Sound|RHS|Bluebird|HDRezka|Jaskier|TVShows|NewStudio|BaibaKo|AlexFilm|LostFilm|Кубик в [Кк]убе|Octopus|LineFilm|Cold Film|AlphaProject|TVG|Good People|Пифагор|Flarrow Films|FF|Videofilm|Мосфильм|Невафильм|Дубляж|Dub|MVO|DVO|AVO|Original|ENG|RUS|UKR)\b/ig,
        type: 'audio_lang',
        transform: (match) => {
            // Убираем лишнее, нормализуем
            let clean = match.trim();
            // Можно привести RHS к Red Head Sound, но лучше оставить как есть для компактности
            return clean;
        }
    },
    subtitles: {
        regex: /Sub\s*[:(]\s*([^)]+)\)?/ig,
        type: 'subtitles',
        transform: (match) => {
            // Вытаскиваем только содержимое скобок
            const content = match.replace(/Sub\s*[:(]\s*([^)]+)\)?/i, '$1');
            // Разбиваем по запятым или пробелам
            return content.split(/[,+]/).map(s => {
                s = s.trim();
                // Сокращаем названия для бейджей
                if (s.toLowerCase().includes('rus')) return 'Sub: Rus';
                if (s.toLowerCase().includes('eng')) return 'Sub: Eng';
                return `Sub: ${s}`;
            }).filter(Boolean);
        }
    }
};

const analyzeTitle = (title) => {
    // 1. Сначала прогоняем через библиотеку ptt для базовых вещей (кодек, контейнер)
    // Она хороша для fallback'а
    let pttInfo = {};
    try {
        pttInfo = ptt.parse(title);
    } catch (e) {}

    // 2. Запускаем ваш мощный парсер
    const foundTags = new Set();
    const result = {
        resolution: null,
        audio_tags: [],
        quality_tags: [],
        subtitle_tags: []
    };

    for (const key in PARSER_CONFIG) {
        const { regex, type, transform } = PARSER_CONFIG[key];
        let matches;
        
        // Сброс индекса regex, так как используем глобальный флаг
        regex.lastIndex = 0;

        while ((matches = regex.exec(title)) !== null) {
            let content = matches[0];
            
            if (transform) {
                const transformed = transform(content);
                const toAdd = Array.isArray(transformed) ? transformed : [transformed];
                
                toAdd.forEach(t => {
                    const cleanT = t.trim();
                    if (!foundTags.has(cleanT.toLowerCase())) {
                        foundTags.add(cleanT.toLowerCase());
                        if (type === 'audio_lang' || type === 'subtitles') result.audio_tags.push(cleanT);
                        // Для субтитров можно тоже добавлять в аудио массив или отдельно, 
                        // здесь я сливаю в audio_tags для сохранения в колонку 'audio'
                    }
                });
            } else {
                if (!foundTags.has(content.toLowerCase())) {
                    foundTags.add(content.toLowerCase());
                    
                    if (type === 'resolution') result.resolution = content;
                    if (type === 'quality') result.quality_tags.push(content);
                    if (type === 'audio_channels') result.audio_tags.push(content); 
                }
            }
        }
    }

    // --- ФОРМИРОВАНИЕ ИТОГОВЫХ ДАННЫХ ---

    // 1. Разрешение (берем из парсера или fallback на библиотеку)
    let resolution = result.resolution || pttInfo.resolution || 'N/A';
    if (resolution.toLowerCase() === '4k') resolution = '4K';

    // 2. HDR и Качество
    // Собираем все теги качества в одну строку для колонки hdr_type (или codec)
    // Но лучше hdr_type использовать для HDR/DV, а codec для кодека
    const upperTitle = title.toUpperCase();
    let hdr_type = 'SDR';
    
    // Проверка HDR через ваши теги
    const isDV = result.quality_tags.some(t => /Dolby|DV/i.test(t));
    const isHDR = result.quality_tags.some(t => /HDR/i.test(t));
    
    if (isDV) hdr_type = 'Dolby Vision';
    else if (isHDR) hdr_type = 'HDR';
    else if (pttInfo.hdr) hdr_type = 'HDR'; // Fallback

    // Чистим теги качества от HDR информации, чтобы не дублировать, и добавляем кодек
    const codec = pttInfo.codec || null;
    
    // 3. Аудио (Самое важное обновление)
    // Объединяем все найденные аудио теги через разделитель " | "
    let audioString = result.audio_tags.join(' | ');
    if (!audioString && pttInfo.audio) {
        audioString = pttInfo.audio; // Fallback если ваш парсер ничего не нашел
    }

    // 4. Контейнер
    const container = pttInfo.container || 'mkv';

    return { resolution, hdr_type, codec, audio: audioString, container };
};

// 2. Расчет битрейта
const calculateBitrate = (sizeBytes, runtimeMinutes) => {
    if (!sizeBytes || !runtimeMinutes || runtimeMinutes <= 0) return null;
    const sizeBits = sizeBytes * 8;
    const seconds = runtimeMinutes * 60;
    const mbps = (sizeBits / seconds) / 1000000;
    return parseFloat(mbps.toFixed(2));
};

// 3. Анализ файлов из WebTorrent
const analyzeFiles = (torrent) => {
    if (!torrent.files || torrent.files.length === 0) return { file_type: 'folder', files: [] };
    const fileList = torrent.files.map(f => f.name);
    const mainFile = torrent.files.reduce((prev, curr) => (prev.length > curr.length) ? prev : curr);
    const ext = mainFile.name.split('.').pop().toLowerCase();
    return { file_type: ext, files: fileList.slice(0, 15) };
};

function drawProgressBar(stats, startTime) {
    const { current, total, success, fallback, skipped } = stats;
    const totalWork = total; 
    const width = 20;
    const percent = totalWork > 0 ? current / totalWork : 0;
    const filled = Math.round(width * percent);
    const empty = width - filled;
    
    const bar = '█'.repeat(filled) + '░'.repeat(empty);
    const percentStr = (percent * 100).toFixed(2);
    
    const elapsed = (Date.now() - startTime) / 1000;
    const speed = (current > 0 && elapsed > 0) ? (current / elapsed).toFixed(1) : 0;

    readline.cursorTo(process.stdout, 0);
    process.stdout.write(`\r${bar} ${percentStr}% | ${current}/${totalWork} | Skipped:${skipped} | OK:${success} TitleOnly:${fallback} | Act:${stats.active} | ${speed}/s  `);
}

async function main() {
  console.log(`🚀 Запуск WORKER ${INSTANCE_ID + 1} из ${TOTAL_INSTANCES}`);
  console.log(`   (Параметры: Xeon Mode, Concurrency=${CONCURRENCY_LIMIT}, WAL=ON)`);

  const { default: WebTorrent } = await import('webtorrent');
  
  // ВАЖНО: Уменьшаем maxConns, чтобы не убить сервер сетевыми прерываниями
  const client = new WebTorrent({ 
      maxConns: 300,   // Было 3500 -> Стало 300 (умножить на кол-во скриптов = 1500-2000 итого)
      dht: true, 
      lsd: false,      // На сервере локальный поиск не нужен
      tracker: true 
  });
  client.on('error', () => {});

  // 1. Загружаем задачи ТОЛЬКО для этого шарда
  console.log('⏳ Чтение части базы данных...');
  
  // !!! ВЫЗЫВАЕМ ОБНОВЛЕННУЮ ФУНКЦИЮ !!!
  const myTorrents = getTorrentsForAnalysis(INSTANCE_ID, TOTAL_INSTANCES);
  
  console.log(`   ∟ Задач для этого воркера: ${myTorrents.length}`);

  // 2. Загружаем КЭШ (getExistingHashesSet оставляем как было, он быстрый)
  const existingSet = getExistingHashesSet();
  console.log(`   ∟ В базе уже есть: ${existingSet.size} записей`);

  // 3. Фильтруем (убираем то, что уже есть в базе результатов)
  console.log('⚡ Фильтрация очереди (исключаем повторы)...');
  const queue = myTorrents.filter(item => {
      // Парсим info_hash из магнета, если его нет в объекте item
      let hash = item.info_hash;
      if (!hash && item.magnet) {
          const match = item.magnet.match(/btih:([a-zA-Z0-9]+)/);
          if (match) hash = match[1].toLowerCase();
      }
      return hash && !existingSet.has(hash);
  });
  
  // Добавляем hash в объект item для удобства
  queue.forEach(item => {
       if (!item.info_hash && item.magnet) {
           const match = item.magnet.match(/btih:([a-zA-Z0-9]+)/);
           if (match) item.info_hash = match[1].toLowerCase();
       }
  });
  
  const skippedCount = myTorrents.length - queue.length;
  console.log(`📥 ОСТАЛОСЬ ОБРАБОТАТЬ: ${queue.length} (Уже готово: ${skippedCount})`);
  
  let activeCount = 0;
  let processedCount = 0;
  const stats = { current: 0, total: queue.length, active: 0, success: 0, fallback: 0, skipped: skippedCount };
  
  const trackerParams = `&tr=${TRACKERS.join('&tr=')}`;
  const startTime = Date.now();

  const uiInterval = setInterval(() => {
      stats.current = processedCount;
      stats.active = activeCount;
      drawProgressBar(stats, startTime);
  }, UI_UPDATE_RATE);

  const processNext = () => {
    if (queue.length === 0 && activeCount === 0) {
      clearInterval(uiInterval);
      drawProgressBar(stats, startTime);
      console.log(`\n\n✅ ЗАВЕРШЕНО.`);
      client.destroy();
      process.exit(0);
      return;
    }

    while (activeCount < CONCURRENCY_LIMIT && queue.length > 0) {
      const item = queue.shift();
      processItem(item);
    }
  };

  const processItem = (item) => {
    activeCount++;
    
    // 1. Fallback данные (Title Analysis + Bitrate Math)
    const titleInfo = analyzeTitle(item.title);
    const bitrate = calculateBitrate(item.size_scraped, item.runtime);
    
    let finalData = {
        info_hash: item.info_hash,
        resolution: titleInfo.resolution || 'N/A', 
        size: item.size_scraped || 0,
        files: JSON.stringify(['(pending)']),
        hdr_type: titleInfo.hdr_type || 'SDR',
        file_type: titleInfo.container || 'folder',
        codec: titleInfo.codec,
        bitrate: bitrate,
        audio: titleInfo.audio
    };

    // 2. WebTorrent попытка
    let isDone = false;
    let torrent = null;
    const magnet = `magnet:?xt=urn:btih:${item.info_hash}${trackerParams}`;

    const timeoutId = setTimeout(() => {
      finalize('TIMEOUT');
    }, MAGNET_TIMEOUT);

    const finalize = (status) => {
        if (isDone) return;
        isDone = true;
        clearTimeout(timeoutId);

        if (torrent) try { torrent.destroy(() => {}); } catch (e) {}

        if (status === 'SUCCESS' && torrent && torrent.metadata) {
            // Успех WebTorrent
            const fileInfo = analyzeFiles(torrent);
            finalData.size = torrent.length; // Точный размер
            finalData.files = JSON.stringify(fileInfo.files);
            finalData.file_type = fileInfo.file_type;
            // Пересчет битрейта с точным размером
            finalData.bitrate = calculateBitrate(torrent.length, item.runtime);
            stats.success++;
        } else {
            // Неудача (Таймаут) -> Сохраняем данные из Title Parser
            finalData.files = JSON.stringify(['(info_only)']);
            stats.fallback++;
        }

        // Пишем в БД
        try {
            insertTorrentDetails(finalData);
        } catch (e) {}

        activeCount--;
        processedCount++;
        setImmediate(processNext);
    };

    try {
        torrent = client.add(magnet, { store: NullStore, skipVerify: true });
        torrent.on('metadata', () => finalize('SUCCESS'));
        torrent.on('error', () => finalize('ERROR'));
    } catch (e) {
        finalize('ERROR_ADD');
    }
  };

  processNext();
}

main().catch(err => {
    console.error('\nFATAL:', err);
    process.exit(1);
});