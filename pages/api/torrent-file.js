import fs from 'fs';
import { getTorrentFile } from '../../lib/aria2';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ message: 'Method not allowed' });
  }

  // Получаем magnet и title из запроса
  const { magnet, title } = req.query;

  if (!magnet) {
    return res.status(400).json({ error: 'Magnet link is required' });
  }

  try {
    // Ждем скачивания файла (логика aria2 осталась в lib/aria2.js)
    const filePath = await getTorrentFile(magnet);

    // --- ФОРМИРОВАНИЕ ИМЕНИ ФАЙЛА ---
    let downloadName = 'movie.torrent';

    if (title) {
        // 1. Декодируем, если пришло криво
        let cleanName = decodeURIComponent(title);
        
        // 2. Убираем запрещенные в файловых системах символы: / \ : * ? " < > |
        cleanName = cleanName.replace(/[/\\?%*:|"<>]/g, '_');
        
        // 3. Убираем лишние пробелы и точки в конце
        cleanName = cleanName.trim().replace(/\.+$/, '');
        
        // 4. Добавляем расширение
        downloadName = `${cleanName}.torrent`;
    } else {
        // Если названия нет, пробуем вытащить dn из магнита
        const nameMatch = magnet.match(/dn=([^&]+)/);
        if (nameMatch) {
             downloadName = decodeURIComponent(nameMatch[1]) + '.torrent';
        }
    }

    // Читаем файл
    if (!fs.existsSync(filePath)) {
        throw new Error('File lost after download');
    }
    const fileBuffer = fs.readFileSync(filePath);

    // Отправляем заголовки для скачивания
    res.setHeader('Content-Type', 'application/x-bittorrent');
    
    // Кодируем имя файла для заголовка Content-Disposition (поддержка русских букв)
    const encodedName = encodeURIComponent(downloadName);
    
    // Используем формат, понятный всем браузерам (filename*=UTF-8''...)
    res.setHeader(
        'Content-Disposition', 
        `attachment; filename="${encodedName}"; filename*=UTF-8''${encodedName}`
    );
    
    res.send(fileBuffer);

  } catch (error) {
    console.error('Torrent API Error:', error);
    res.status(500).json({ 
      error: 'Не удалось подготовить файл. Попробуйте Magnet.',
      details: error.message 
    });
  }
}