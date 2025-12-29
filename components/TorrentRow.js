import { useState } from 'react';
import { Magnet, Video, Monitor, Mic, FileText, Signal, Star, Zap, FileDown, Loader2 } from 'lucide-react'; // Добавил Zap для битрейта (или можно Signal)

export default function TorrentRow({ torrent }) {
  const details = torrent.cached_details || {};
  
  // Состояние загрузки торрент-файла
  const [isGenerating, setIsGenerating] = useState(false);

  // --- ФУНКЦИЯ СКАЧИВАНИЯ ТОРРЕНТА ---
  const handleTorrentDownload = async (e) => {
    e.preventDefault();
    if (isGenerating) return;

    setIsGenerating(true);
    
    try {
        // ИЗМЕНЕНИЕ ЗДЕСЬ: Передаем название торрента на сервер
        const params = new URLSearchParams({ 
          magnet: torrent.magnet,
          // Берем заголовок, если его нет — fallback, обрезаем до 100 символов чтобы URL не был гигантским
          title: (torrent.torrent_title || 'movie').substring(0, 100)
        });
        
        const response = await fetch(`/api/torrent-file?${params}`);

        if (!response.ok) {
            const errData = await response.json();
            throw new Error(errData.error || 'Ошибка генерации');
        }

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        
        // Пытаемся взять имя файла из заголовков ответа (сервер его там красиво сформирует)
        const contentDisposition = response.headers.get('Content-Disposition');
        let fileName = 'download.torrent';
        
        // Парсим имя файла из заголовка (поддержка UTF-8)
        if (contentDisposition) {
             // Сначала пробуем современный стандарт filename*=UTF-8''...
             const starMatch = contentDisposition.match(/filename\*=UTF-8''([^;]+)/);
             if (starMatch && starMatch[1]) {
                 fileName = decodeURIComponent(starMatch[1]);
             } else {
                 // Иначе обычный filename="..."
                 const quoteMatch = contentDisposition.match(/filename="?([^"]+)"?/);
                 if (quoteMatch && quoteMatch[1]) {
                     fileName = decodeURIComponent(quoteMatch[1]);
                 }
             }
        }
        
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

    } catch (error) {
        alert(`Не удалось создать .torrent файл: ${error.message}. Пожалуйста, используйте Magnet-ссылку.`);
    } finally {
        setIsGenerating(false);
    }
  };

  
  // Хелпер для цвета качества
  const getQualityColor = (res) => {
    if (res?.includes('4K') || res?.includes('2160')) return 'bg-purple-100 text-purple-700 border-purple-200';
    if (res?.includes('1080')) return 'bg-green-100 text-green-700 border-green-200';
    if (res?.includes('720')) return 'bg-blue-50 text-blue-700 border-blue-200';
    return 'bg-gray-100 text-gray-600 border-gray-200';
  };

  // Хелпер для HDR
  const getHdrColor = (hdr) => {
    if (!hdr) return null;
    if (hdr.includes('Dolby') || hdr.includes('DV')) return 'bg-indigo-50 text-indigo-700 border-indigo-200';
    if (hdr.includes('HDR')) return 'bg-amber-50 text-amber-700 border-amber-200';
    return null;
  };

  const getAudioBadges = (audioString) => {
    if (!audioString) return [];
    return audioString.split('|').map(s => s.trim()).filter(Boolean);
  };

  // --- КОМПОНЕНТ БЕЙДЖА АУДИО ---
  const AudioBadge = ({ tag }) => {
    const lowerTag = tag.toLowerCase();
    // Убираем показ субтитров
    if (lowerTag.includes('sub')) {
        return null; 
    }

    let badgeStyle = "bg-rose-50 text-rose-700 border-rose-100";
    let Icon = Mic;

    if (lowerTag.includes('red head') || lowerTag.includes('rhs') || lowerTag.includes('дубляж') || lowerTag.includes('мосфильм')) {
        badgeStyle = "bg-rose-100 text-rose-800 border-rose-200 font-bold shadow-sm";
        Icon = Star;
    }
    else if (lowerTag.includes('5.1') || lowerTag.includes('7.1')) {
        badgeStyle = "bg-orange-50 text-orange-700 border-orange-100";
        Icon = Signal;
    }
    else if (lowerTag.includes('original') || lowerTag.includes('eng')) {
        badgeStyle = "bg-blue-50 text-blue-700 border-blue-100";
    }
    else if (lowerTag.match(/(rezka|jaskier|tvshows|lostfilm|newstudio|kubik)/)) {
        badgeStyle = "bg-teal-50 text-teal-700 border-teal-100";
    }

    return (
        <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[10px] md:text-[11px] font-medium border ${badgeStyle} whitespace-nowrap`}>
            <Icon className="w-3 h-3 opacity-70" /> {tag}
        </span>
    );
  };

  const audioTags = getAudioBadges(details.audio);
  const hdrBadgeClass = getHdrColor(details.hdr_type);

  // --- КОМПОНЕНТ БЕЙДЖА БИТРЕЙТА (Общий для ПК и Мобилки) ---
  // Делаем его заметным: светлый фон, четкие границы, иконка молнии
  const BitrateBadge = ({ bitrate }) => (
     <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-white border border-gray-300 text-gray-600 text-[10px] md:text-[11px] font-mono font-medium shadow-sm">
        {bitrate} Mbps
     </span>
  );

  return (
    <tr className="group hover:bg-gray-50/80 transition-colors border-b border-gray-100 last:border-0 text-left">
      
      {/* --- НАЗВАНИЕ И МОБИЛЬНЫЕ МЕТАДАННЫЕ --- */}
      <td className="py-4 pl-4 pr-2 md:pl-6 md:pr-4 max-w-[200px] md:max-w-[400px]">
        <div className="flex flex-col gap-2">
          <span className="font-medium text-gray-900 line-clamp-2 text-sm md:text-base leading-snug break-words">
              {torrent.torrent_title || 'Без названия'}
            </span>

          {/* --- МОБИЛЬНАЯ ВЕРСИЯ --- */}
          <div className="flex flex-col gap-1.5 md:hidden">
             
             {/* Строка 1: Технические данные (Размер, БИТРЕЙТ, Качество, HDR) */}
             <div className="flex flex-wrap items-center gap-1.5">
                {/* Размер */}
                <span className="text-[10px] font-semibold text-gray-500 bg-gray-100 px-1.5 py-0.5 rounded border border-gray-200">
                    {torrent.size}
                </span>

                {/* БИТРЕЙТ НА ТЕЛЕФОНЕ (Добавлено сюда) */}
                {details.bitrate > 0 && (
                    <BitrateBadge bitrate={details.bitrate} />
                )}
                
                {/* Разрешение */}
                {details.resolution && (
                    <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded border ${getQualityColor(details.resolution)}`}>
                        {details.resolution}
                    </span>
                )}

                {/* HDR */}
                {details.hdr_type && details.hdr_type !== 'SDR' && (
                <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded border ${hdrBadgeClass}`}>
                    {details.hdr_type}
                </span>
                )}

                {/* Формат файла */}
                {details.file_type && details.file_type !== 'folder' && (
                    <span className="text-[10px] uppercase bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded border border-gray-200 font-mono tracking-wider">
                        {details.file_type}
                    </span>
                )}
             </div>

             {/* Строка 2: Озвучки */}
             {audioTags.length > 0 && (
                 <div className="flex flex-wrap items-center gap-1.5 mt-0.5">
                    {audioTags.slice(0, 4).map((tag, idx) => (
                        <AudioBadge key={idx} tag={tag} />
                    ))}
                    {audioTags.length > 4 && (
                        <span className="text-[10px] text-gray-400">+{audioTags.length - 4}</span>
                    )}
                 </div>
             )}
          </div>
          
          {/* --- ДЕСКТОП ВЕРСИЯ (Список тегов под названием) --- */}
          <div className="hidden md:flex flex-wrap items-center gap-2 mt-1">
             {details.codec && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-medium bg-gray-100 text-gray-600 border border-gray-200">
                   <Video className="w-3 h-3" /> {details.codec}
                </span>
             )}
             {audioTags.slice(0, 6).map((tag, idx) => (
                <AudioBadge key={idx} tag={tag} />
             ))}
             {audioTags.length > 6 && (
                 <span className="text-[10px] text-gray-400 font-medium">+{audioTags.length - 6}</span>
             )}
          </div>

        </div>
      </td>
      
      {/* --- КОЛОНКИ ДЛЯ ПК --- */}
      
      {/* Размер */}
      <td className="py-4 px-4 hidden md:table-cell text-gray-600 font-medium whitespace-nowrap text-sm align-top pt-5">
        {torrent.size || '-'}
      </td>
      
      {/* Качество */}
      <td className="py-4 px-4 hidden md:table-cell text-center align-top pt-5">
        <div className="flex flex-col items-center gap-1.5">
            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold border ${getQualityColor(details.resolution)}`}>
            {details.resolution || 'N/A'}
            </span>
            {details.file_type && details.file_type !== 'folder' && (
                <span className="text-[10px] uppercase text-gray-400 font-mono tracking-wider">
                    {details.file_type}
                </span>
            )}
        </div>
      </td>
      
      {/* Инфо (HDR + Bitrate) - ТЕПЕРЬ ЗАМЕТНЕЕ */}
      <td className="py-4 px-4 hidden lg:table-cell text-center align-top pt-5">
        <div className="flex flex-col items-center gap-2"> {/* Увеличил gap */}
          
          {details.hdr_type && details.hdr_type !== 'SDR' && (
            <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold border whitespace-nowrap ${hdrBadgeClass}`}>
              {details.hdr_type === 'Dolby Vision' ? 'DV' : details.hdr_type}
            </span>
          )}
          
          {/* БИТРЕЙТ (Теперь как бейдж, а не серый текст) */}
          {details.bitrate > 0 && (
             <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded bg-gray-50 border border-gray-200 text-gray-600 text-[11px] font-mono font-medium whitespace-nowrap" title="Средний битрейт"> 
               {details.bitrate} Mbps
             </span>
           )}
        </div>
      </td>
      
      {/* Сиды / Пиры */}
      <td className="py-4 px-4 hidden md:table-cell text-center align-top pt-5">
        <div className="flex flex-col items-center">
          <div className="flex items-center gap-1">
            <span className="text-green-600 font-bold text-sm">{torrent.seeders || 0}</span>
            <span className="text-gray-300">/</span>
            <span className="text-red-500 font-bold text-sm text-opacity-70">{torrent.leechers || 0}</span>
          </div>
        </div>
      </td>
      
      {/* Кнопки действий */}
      <td className="py-4 pl-2 pr-4 text-right w-auto align-middle">
        <div className="flex items-center justify-end gap-3">
          
          {/* 1. КНОПКА .TORRENT (Голубая) */}
          {torrent.magnet && (
            <button
              onClick={handleTorrentDownload}
              disabled={isGenerating}
              className={`group relative inline-flex items-center justify-center w-11 h-11 rounded-2xl border transition-all duration-300 ease-out
                ${isGenerating 
                    ? 'bg-gray-50 text-gray-400 border-gray-200 cursor-wait' 
                    : 'bg-sky-50 text-sky-600 border-sky-100 hover:bg-sky-500 hover:text-white hover:border-sky-500 hover:-translate-y-1 hover:shadow-lg hover:shadow-sky-200'
                }`}
              title="Скачать .torrent файл"
            >
              {isGenerating ? (
                <Loader2 className="h-5 w-5 animate-spin" />
              ) : (
                <FileDown className="h-5 w-5" />
              )}
              
              {/* Бейдж FILE (появляется при наведении) */}
              {!isGenerating && (
                <span className="absolute -bottom-2 left-1/2 -translate-x-1/2 translate-y-2 opacity-0 group-hover:translate-y-0 group-hover:opacity-100 transition-all duration-300 pointer-events-none z-10">
                  <span className="px-1.5 py-0.5 rounded-md bg-white border border-sky-100 text-[9px] font-bold text-sky-600 shadow-sm whitespace-nowrap">
                    FILE
                  </span>
                </span>
              )}
            </button>
          )}

          {/* 2. КНОПКА MAGNET (Фиолетовая) */}
          {torrent.magnet && (
            <a
              href={torrent.magnet}
              className="group relative inline-flex items-center justify-center w-11 h-11 rounded-2xl bg-purple-50 text-purple-600 border border-purple-100 hover:bg-purple-600 hover:text-white hover:border-purple-600 hover:-translate-y-1 hover:shadow-lg hover:shadow-purple-200 transition-all duration-300 ease-out"
              title="Скачать Magnet-ссылку"
            >
              <Magnet className="h-5 w-5" />

              {/* Бейдж MAGNET (появляется при наведении) */}
              <span className="absolute -bottom-2 left-1/2 -translate-x-1/2 translate-y-2 opacity-0 group-hover:translate-y-0 group-hover:opacity-100 transition-all duration-300 pointer-events-none z-10">
                  <span className="px-1.5 py-0.5 rounded-md bg-white border border-purple-100 text-[9px] font-bold text-purple-600 shadow-sm whitespace-nowrap">
                    MAGNET
                  </span>
              </span>
            </a>
          )}
          
        </div>
      </td>
    </tr>
  );
}