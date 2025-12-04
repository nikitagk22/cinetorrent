import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';
import crypto from 'crypto';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Метод не разрешен. Используйте POST.' });
  }

  try {
    const { magnet } = req.body;

    if (!magnet || typeof magnet !== 'string') {
      return res.status(400).json({ error: 'Magnet-ссылка обязательна' });
    }

    // Валидация magnet-ссылки
    if (!magnet.startsWith('magnet:?') && !/^[0-9a-f]{40}$/i.test(magnet)) {
      return res.status(400).json({ error: 'Неверный формат magnet-ссылки' });
    }

    // Создаем временное имя файла
    const tmpName = crypto.randomBytes(8).toString('hex');
    const tmpDir = os.tmpdir();
    const outPath = path.join(tmpDir, `${tmpName}.torrent`);

    // Вызываем m2t CLI для конвертации magnet в torrent
    // Используем npx для автоматического поиска локально установленного пакета
    // Команда: npx m2t -o <output_file> '<magnet_link>'
    const isWindows = process.platform === 'win32';
    
    console.log(`[magnet-to-torrent] Начало конвертации для ${magnet.substring(0, 50)}...`);
    console.log(`[magnet-to-torrent] Выходной файл: ${outPath}`);
    console.log(`[magnet-to-torrent] Платформа: ${process.platform}`);
    console.log(`[magnet-to-torrent] Рабочая директория: ${process.cwd()}`);
    
    // Используем npx для запуска локально установленного m2t
    // shell: true работает надежнее на всех платформах
    const proc = spawn('npx', ['m2t', '-o', outPath, magnet], {
      stdio: 'ignore', // Игнорируем вывод, чтобы не засорять логи
      shell: true, // Используем shell для надежности
      cwd: process.cwd(),
      env: process.env,
    });
    
    console.log(`[magnet-to-torrent] Процесс запущен, PID: ${proc.pid}`);

    // Таймаут 45 секунд
    const TIMEOUT = 45000;
    const timeout = setTimeout(() => {
      try {
        proc.kill('SIGKILL');
      } catch (e) {
        // Игнорируем ошибки при завершении
      }
    }, TIMEOUT);

    return new Promise((resolve) => {
      proc.on('close', (code) => {
        clearTimeout(timeout);
        console.log(`[magnet-to-torrent] Процесс завершен с кодом: ${code}`);

        // Проверяем, создан ли файл
        const fileExists = fs.existsSync(outPath);
        console.log(`[magnet-to-torrent] Файл существует: ${fileExists}, путь: ${outPath}`);
        
        if (code !== 0 || !fileExists) {
          // Очищаем временный файл, если он частично создан
          try {
            if (fs.existsSync(outPath)) {
              fs.unlinkSync(outPath);
            }
          } catch (e) {
            // Игнорируем ошибки удаления
          }

          res.status(504).json({
            error: 'Не удалось получить metadata из сети. Попробуйте позже или используйте magnet-ссылку напрямую.',
            detail: code !== 0 ? `Процесс завершился с кодом ${code}` : 'Файл не был создан',
          });
          return resolve();
        }

        try {
          // Читаем созданный торрент-файл
          const torrentBuffer = fs.readFileSync(outPath);
          
          // Получаем безопасное имя файла для скачивания
          // Извлекаем имя из magnet, если возможно
          let filename = 'torrent.torrent';
          try {
            const nameMatch = magnet.match(/dn=([^&]+)/);
            if (nameMatch) {
              filename = decodeURIComponent(nameMatch[1])
                .replace(/[^a-z0-9а-яё\s.-]/gi, '_')
                .substring(0, 100) + '.torrent';
            } else {
              filename = `${tmpName}.torrent`;
            }
          } catch (e) {
            filename = `${tmpName}.torrent`;
          }

          // Отправляем файл клиенту
          res.setHeader('Content-Type', 'application/x-bittorrent');
          res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
          res.setHeader('Content-Length', torrentBuffer.length);
          res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
          
          res.send(torrentBuffer);

          // Удаляем временный файл после отправки
          setTimeout(() => {
            try {
              if (fs.existsSync(outPath)) {
                fs.unlinkSync(outPath);
              }
            } catch (e) {
              console.error('Ошибка при удалении временного файла:', e);
            }
          }, 5000); // Удаляем через 5 секунд

          return resolve();
        } catch (readError) {
          console.error('Ошибка при чтении торрент-файла:', readError);
          
          // Пытаемся удалить файл
          try {
            if (fs.existsSync(outPath)) {
              fs.unlinkSync(outPath);
            }
          } catch (e) {
            // Игнорируем
          }

          res.status(500).json({
            error: 'Ошибка при обработке торрент-файла',
            detail: readError.message,
          });
          return resolve();
        }
      });

      proc.on('error', (err) => {
        clearTimeout(timeout);
        console.error(`[magnet-to-torrent] Ошибка при запуске процесса:`, err);
        console.error(`[magnet-to-torrent] Код ошибки: ${err.code}, сообщение: ${err.message}`);
        console.error(`[magnet-to-torrent] PATH: ${process.env.PATH}`);
        
        // Проверяем, установлен ли m2t
        if (err.code === 'ENOENT') {
          console.error(`[magnet-to-torrent] npx не найден в системе`);
          res.status(500).json({
            error: 'npx не найден в системе. Убедитесь, что Node.js установлен правильно.',
            detail: err.message,
            code: err.code,
          });
        } else {
          res.status(500).json({
            error: 'Ошибка при запуске процесса конвертации',
            detail: err.message,
            code: err.code,
          });
        }
        return resolve();
      });
    });
  } catch (error) {
    console.error('Ошибка в API magnet-to-torrent:', error);
    return res.status(500).json({
      error: 'Внутренняя ошибка сервера',
      detail: error.message,
    });
  }
}

