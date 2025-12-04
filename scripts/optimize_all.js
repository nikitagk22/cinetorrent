const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const DATABASES = [
    'tmdb_data/tmdb_minimal_no_original.db',
    'tmdb_data/torrents.db',
    'tmdb_data/torrents_data.db'
];

console.log('🚀 Начинаем полную оптимизацию всех баз данных...');

DATABASES.forEach(relativePath => {
    const dbPath = path.join(process.cwd(), relativePath);
    
    if (!fs.existsSync(dbPath)) {
        console.log(`⚠️ Файл не найден, пропускаем: ${dbPath}`);
        return;
    }

    console.log(`\n📂 Обработка: ${relativePath}`);
    
    try {
        const db = new Database(dbPath);
        
        // 1. Принудительно вливаем WAL файл (это уберет файлы .db-wal и .db-shm)
        console.log('   ∟ Checkpoint (вливаем временные данные)...');
        db.pragma('wal_checkpoint(TRUNCATE)');

        // 2. Полная пересборка базы (уменьшает размер файла, дефрагментирует)
        console.log('   ∟ VACUUM (сжатие и дефрагментация)...');
        db.exec('VACUUM;');

        // 3. Обновление статистики для планировщика запросов
        console.log('   ∟ ANALYZE (оптимизация индексов)...');
        db.exec('ANALYZE;');

        db.close();
        console.log('   ✅ Успешно.');
    } catch (e) {
        console.error(`   ❌ Ошибка: ${e.message}`);
    }
});

console.log('\n✨ Все готово! Теперь перезапустите сайт.');