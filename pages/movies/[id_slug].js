import Head from 'next/head';
import Link from 'next/link';
import Image from 'next/image';
import { useState, useMemo } from 'react';
import {
  ArrowLeft,
  Download,
  Film,
  Globe,
  Star,
  Search,
  X,
  Sparkles,
  HelpCircle,
  Youtube,
  ArrowUpDown,
  ChevronUp,
  ChevronDown
} from 'lucide-react';
import { getMovieByIdSlug, getTorrentsByTmdbId, getTorrentDetailsByInfoHash, getRandomMovies, getRecommendationsApi } from '../../lib/db';
import TorrentRow from '../../components/TorrentRow';
import Breadcrumbs from '../../components/Breadcrumbs';
import SearchBar from '../../components/SearchBar';
import MovieCard from '../../components/MovieCard';

// Хелпер для размеров файлов
const convertEnglishUnitsToRussian = (sizeString) => {
  if (typeof sizeString !== 'string') return sizeString;
  return sizeString
    .replace(/GB/g, 'ГБ')
    .replace(/MB/g, 'МБ')
    .replace(/KB/g, 'КБ')
    .replace(/B/g, 'Б');
};

// --- Хелперы для сортировки ---

// Парсинг размера в байты
const parseSizeToBytes = (sizeStr) => {
  if (!sizeStr) return 0;
  const num = parseFloat(sizeStr.replace(/,/g, '.').replace(/[^\d.]/g, ''));
  const lower = sizeStr.toLowerCase();
  if (lower.includes('гб') || lower.includes('gb')) return num * 1024 * 1024 * 1024;
  if (lower.includes('мб') || lower.includes('mb')) return num * 1024 * 1024;
  if (lower.includes('кб') || lower.includes('kb')) return num * 1024;
  return num;
};

// Ранжирование качества (чем выше число, тем лучше качество)
const getResolutionRank = (res) => {
  if (!res) return 0;
  if (res.includes('4K') || res.includes('2160')) return 40;
  if (res.includes('1080')) return 30;
  if (res.includes('720')) return 20;
  return 10; // SD или неизвестно
};

export default function MoviePage({ movie, torrents, pageTitle, seoDescription, seo, techSpecs, recommendations }) {
  
  // 1. Список ID заблокированных фильмов (или бери это из пропсов, если добавишь в БД)
  const BANNED_IDS = [1086260, 604079, 156670, 1016084]; // ID фильма "Астронавт" из письма (проверь, это ID TMDB или твой внутренний)
  
  // Если у тебя id_slug содержит ID, можно проверять так:
  const isBanned = BANNED_IDS.some(id => movie?.id === id || movie?.tmdb_id === id);

  // --- Состояние для открытия поиска ---
  const [isSearchOpen, setIsSearchOpen] = useState(false);

  // 1. Состояние сортировки (по умолчанию: сиды, по убыванию)
  const [sortConfig, setSortConfig] = useState({ key: 'seeders', direction: 'desc' });

  // 2. Функция обработки клика по заголовку
  const handleSort = (key) => {
    setSortConfig((current) => {
      if (current.key === key) {
        // Если кликнули по той же колонке, меняем направление
        return { key, direction: current.direction === 'asc' ? 'desc' : 'asc' };
      }
      // Если новая колонка, сортируем по убыванию (обычно удобнее для торрентов)
      return { key, direction: 'desc' };
    });
  };

  // 3. Вычисление отсортированного массива
  // Используем useMemo, чтобы не пересчитывать при каждом рендере, если данные не менялись
  const sortedTorrents = useMemo(() => {
    if (!torrents) return [];
    
    const sorted = [...torrents].sort((a, b) => {
      const dir = sortConfig.direction === 'asc' ? 1 : -1;
      
      switch (sortConfig.key) {
        case 'name':
          const titleA = a.torrent_title || '';
          const titleB = b.torrent_title || '';
          return titleA.localeCompare(titleB) * dir;
          
        case 'size':
          return (parseSizeToBytes(a.size) - parseSizeToBytes(b.size)) * dir;
          
        case 'resolution':
          const resA = getResolutionRank(a.cached_details?.resolution);
          const resB = getResolutionRank(b.cached_details?.resolution);
          return (resA - resB) * dir;
          
        case 'bitrate':
          const bitA = parseFloat(a.cached_details?.bitrate || 0);
          const bitB = parseFloat(b.cached_details?.bitrate || 0);
          return (bitA - bitB) * dir;
          
        case 'seeders':
        default:
          // Сортируем по сидам. Если сидов поровну, то по пирам
          const seedDiff = (a.seeders || 0) - (b.seeders || 0);
          if (seedDiff !== 0) return seedDiff * dir;
          return ((a.leechers || 0) - (b.leechers || 0)) * dir;
      }
    });
    
    return sorted;
  }, [torrents, sortConfig]);

  // Хелпер для иконки в заголовке
  const SortIcon = ({ columnKey }) => {
    if (sortConfig.key !== columnKey) return <ArrowUpDown className="w-3 h-3 opacity-30" />;
    return sortConfig.direction === 'asc' 
      ? <ChevronUp className="w-3 h-3 text-primary-600" /> 
      : <ChevronDown className="w-3 h-3 text-primary-600" />;
  };

  if (!movie) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center bg-white p-8 rounded-2xl shadow-soft">
          <h1 className="text-2xl font-bold text-gray-800 mb-2">Фильм не найден</h1>
          <Link href="/" className="text-primary-600 hover:underline">Вернуться на главную</Link>
        </div>
      </div>
    );
  }

  // --- SEO Content Generation ---
  const currentUrl = `/movies/${movie.id_slug}`;
  const siteUrl = 'https://cinetorrent.ru';
  const baseKeywords = [
    movie.title,
    'скачать торрент',
    movie.year,
    movie.genres,
    movie.production_countries,
    movie.trailer_key && movie.trailer_key !== 'none' ? `смотреть трейлер ${movie.title}` : null,
  ].filter(Boolean).join(', ');
  const dynamicKeywords = seo.dynamicKeywords.join(', ');
  const finalKeywords = [baseKeywords, dynamicKeywords].filter(Boolean).join(', ');
  const ogImage = movie.local_poster_path || `${siteUrl}/og-default.jpg`;
  // --- End SEO ---

  const formatVotes = (count) => {
    if (!count && count !== 0) return '-';
    if (count === 0) return '-';
    
    if (count >= 1000) {
      return (
        <span className="inline-flex items-baseline">
          {(count / 1000).toFixed(1)}
          <span className="text-[0.6em] font-extrabold opacity-80 ml-[1px]">k</span>
        </span>
      );
    }
    return count;
  };

  const breadcrumbsItems = [
    { label: movie.title, href: `/movies/${movie.id_slug}` }
  ];

  // Build the tech info string
  let techInfoString = '';
  if (techSpecs) {
    if (techSpecs.uniqueResolutions && techSpecs.uniqueResolutions.length > 0) {
      techInfoString += `Для фильма «${movie.title}» доступны раздачи в качестве <span class="font-medium text-gray-800">${techSpecs.uniqueResolutions.join(', ')}</span>. `;
    }
    if (techSpecs.minBitrate > 0 && techSpecs.maxBitrate > 0) {
      techInfoString += `Средний битрейт видеопотока варьируется от ${techSpecs.minBitrate} до ${techSpecs.maxBitrate} Мбит/с. `;
    }
    
    if (techSpecs.hasHDR) {
      techInfoString += 'Присутствуют версии с расширенным динамическим диапазоном (HDR/Dolby Vision).';
    } else {
      // ONLY add SDR text if other info already exists.
      if (techInfoString) {
        techInfoString += 'Доступны версии со стандартным динамическим диапазоном (SDR).';
      }
    }
  }
  techInfoString = techInfoString.trim();

  // --- ЛОГИКА FAQ (Вставьте это перед return) ---
  
  // 1. Определяем диапазон размеров
  let sizeText = "зависит от качества";
  if (torrents && torrents.length > 0) {
      const sizes = torrents.map(t => t.size); // Предполагаем, что это строки "2.5 ГБ"
      sizeText = `от ${sizes[sizes.length - 1]} до ${sizes[0]}`; 
  }

  const has4k = torrents.some(t => t.cached_details?.resolution === '4K');
  const hasHDR = torrents.some(t => t.cached_details?.hdr_type && t.cached_details.hdr_type !== 'SDR');

  // 2. Массив вопросов и ответов
  const faqData = [
    {
      q: `Как скачать фильм «${movie.title}» бесплатно?`,
      a: `Скачать «${movie.title}» ${movie.year} можно бесплатно через торрент на нашем сайте. Выберите подходящий файл в таблице выше и нажмите на магнит-ссылку.`
    },
    {
      q: `Есть ли версия в 4K или HDR?`,
      a: has4k || hasHDR 
         ? `Да! Доступны версии: ${has4k ? '4K Ultra HD' : ''} ${hasHDR ? '+ HDR/Dolby Vision' : ''}.` 
         : `На данный момент доступны качественные HD версии (1080p).`
    },
    {
      q: `Сколько весит фильм?`,
      a: `Вес файлов варьируется ${sizeText}. Для просмотра на телефоне выбирайте файлы 1.5–2 ГБ, для ТВ — от 10 ГБ.`
    }
  ];

  // 3. Schema.org для Google/Yandex
  const faqSchema = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "mainEntity": faqData.map(item => ({
      "@type": "Question",
      "name": item.q,
      "acceptedAnswer": { "@type": "Answer", "text": item.a }
    }))
  };

  const breadcrumbSchema = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": [
      {
        "@type": "ListItem",
        "position": 1,
        "name": "Главная",
        "item": siteUrl
      },
      {
        "@type": "ListItem",
        "position": 2,
        "name": movie.title,
        "item": siteUrl + currentUrl
      }
    ]
  };

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
        <meta name="description" content={seoDescription} />
        <link rel="canonical" href={currentUrl} />
        
        {/* Open Graph */}
        <meta property="og:type" content="video.movie" />
        <meta property="og:url" content={currentUrl} />
        <meta property="og:title" content={pageTitle} />
        <meta property="og:description" content={seoDescription} />
        <meta property="og:image" content={ogImage} />
        <meta property="og:site_name" content="CineTorrent" />
        <meta property="og:locale" content="ru_RU" />
        
        {/* Twitter */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={pageTitle} />
        <meta name="twitter:description" content={seoDescription} />
        <meta name="twitter:image" content={ogImage} />
        
        {/* Keywords & Robots */}
        <meta name="keywords" content={finalKeywords} />
        <meta name="robots" content="index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1" />
        
        {/* Schema.org */}
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "Movie",
              "name": movie.title,
              "description": movie.overview || seoDescription,
              "image": ogImage,
              "datePublished": movie.year ? `${movie.year}-01-01` : undefined,
              "aggregateRating": movie.vote_average ? {
                "@type": "AggregateRating",
                "ratingValue": movie.vote_average,
                "ratingCount": movie.vote_count || 0,
                "bestRating": "10",
                "worstRating": "1"
              } : undefined,
              "genre": movie.genres ? movie.genres.split(',').map(g => g.trim()) : [],
            })
          }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(faqSchema) }}
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumbSchema) }}
        />
      </Head>

      <div className="min-h-screen bg-gray-50 pb-20 relative">
        {/* Background Blur */}
        <div className="absolute inset-0 h-[600px] overflow-hidden z-0 pointer-events-none">
           {movie.backdrop_path ? (
              <Image 
                 src={movie.backdrop_path} 
                 alt="" 
                 fill 
                 className="object-cover opacity-20 blur-3xl scale-110" 
                 priority
                 unoptimized
              />
           ) : (
              <div className="w-full h-full bg-gradient-to-b from-primary-100/50 via-white/50 to-gray-50" />
           )}
           <div className="absolute inset-0 bg-gradient-to-b from-white/10 via-white/60 to-gray-50" />
        </div>

        {/* --- HEADER START --- */}
        <header className="sticky top-0 z-50 bg-white/70 backdrop-blur-md border-b border-white/20 shadow-sm transition-all h-[65px] flex items-center">
          <div className="container mx-auto px-4 flex items-center justify-between gap-4">
            
            {isSearchOpen ? (
              // --- РЕЖИМ ПОИСКА ---
              <div className="flex items-center w-full gap-2 animate-fade-in">
                <div className="flex-1 relative z-50">
                   <div className="transform scale-95 origin-left w-full"> 
                      <SearchBar />
                   </div>
                </div>
                <button 
                  onClick={() => setIsSearchOpen(false)}
                  className="p-2 text-gray-500 hover:text-gray-800 bg-gray-100 hover:bg-gray-200 rounded-full transition-colors flex-shrink-0"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>
            ) : (
              // --- ОБЫЧНЫЙ РЕЖИМ ---
              <>
                <Link
                  href="/"
                  className="flex items-center gap-2 text-gray-600 hover:text-primary-600 transition-colors font-medium group px-2 py-1 rounded-lg hover:bg-gray-100/50"
                >
                  <ArrowLeft className="h-5 w-5 group-hover:-translate-x-1 transition-transform" />
                  <span className="hidden sm:inline">Назад</span>
                </Link>
                
                <Link href="/" className="text-xl font-display font-bold text-gray-800 tracking-tight absolute left-1/2 transform -translate-x-1/2">
                  Cine<span className="text-primary-600">Torrent</span>
                </Link>

                {/* Кнопка открытия поиска */}
                <button
                  onClick={() => setIsSearchOpen(true)}
                  className="p-2 text-gray-600 hover:text-primary-600 hover:bg-primary-50 rounded-xl transition-all"
                  title="Поиск"
                >
                  <Search className="h-5 w-5" />
                </button>
              </>
            )}

          </div>
        </header>
        {/* --- HEADER END --- */}

        {/* Main Content */}
        <main className="container mx-auto px-4 md:px-6 py-8 relative z-10">
          <div className="max-w-6xl mx-auto">
            
            <Breadcrumbs items={breadcrumbsItems} />

            {/* Movie Info Card */}
            <div className="bg-white/80 backdrop-blur-md rounded-3xl shadow-card border border-white/50 overflow-hidden mb-10">
              <div className="grid grid-cols-1 lg:grid-cols-12 gap-0">
                
                {/* 
                    1. ПОСТЕР (Вернули старые настройки)
                    - h-[500px] для мобилок (фиксированная высота)
                    - lg:h-auto для ПК (растягивается на высоту контента)
                    - object-cover (заполняет всё пространство без серых полос)
                */}
                {/* Возвращаем оригинальную структуру: h-auto позволяет тянуться, object-cover заполняет всё */}
                <div className="lg:col-span-3 relative h-[500px] lg:h-auto bg-gray-200 min-h-[400px]">
                  {movie.local_poster_path ? (
                    <Image
                      src={movie.local_poster_path}
                      alt={`Постер фильма ${movie.title}`}
                      fill
                      className="object-cover"
                      priority
                      // --- НАСТРОЙКИ КАЧЕСТВА ---
                      unoptimized // 1. Самое важное: загружать оригинальный файл "как есть" с диска, без ресайза Next.js
                      quality={100} // 2. Максимальное качество JPEG (на случай, если unoptimized уберете)
                      sizes="100vw" // 3. Обман браузера: "Картинка будет на весь экран, дай мне самую большую версию"
                    />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-gray-400">
                      <Film className="h-20 w-20" />
                    </div>
                  )}
                  
                  {/* Тень для мобилок */}
                  <div className="absolute inset-0 bg-gradient-to-t from-black/30 via-transparent to-transparent lg:hidden" />
                </div>

                {/* 
                    2. ПРАВАЯ ЧАСТЬ (ИНФОРМАЦИЯ)
                */}
                <div className="lg:col-span-9 p-6 md:p-8 flex flex-col">
                  
                  {/* --- ШАПКА (Заголовок, Год, Рейтинги) --- */}
                  <div className="mb-6">
                    <div className="flex flex-wrap items-center gap-3 mb-3 text-sm font-medium">
                        {movie.year && (
                        <span className="px-3 py-1 bg-gray-100 text-gray-700 rounded-full border border-gray-200 shadow-sm">
                            {movie.year}
                        </span>
                        )}
                        {movie.production_countries && (
                        <span className="flex items-center gap-1 text-gray-500">
                            <Globe className="w-4 h-4" />
                            {movie.production_countries.split(',')[0]}
                        </span>
                        )}
                    </div>

                    <h1 className="text-3xl md:text-4xl lg:text-5xl font-display font-bold text-gray-900 mb-4 leading-tight">
                        {movie.title}
                    </h1>

                    {/* Рейтинги и Жанры в одну строку */}
                    <div className="flex flex-wrap items-center gap-4 md:gap-6">
                        {/* TMDB */}
                        <div className="flex items-center gap-2 px-3 py-2 bg-yellow-50 rounded-xl border border-yellow-100/60">
                            <div className="flex items-center justify-center w-8 h-8 bg-yellow-400 rounded-lg text-white shadow-sm">
                                <Star className="w-4 h-4 fill-white" />
                            </div>
                            <div className="flex flex-col leading-none">
                                <span className="text-lg font-bold text-gray-900">
                                    {movie.vote_average ? movie.vote_average.toFixed(1) : '-'}
                                </span>
                                <span className="text-[10px] text-yellow-600 font-bold uppercase tracking-wider">
                                    TMDB
                                </span>
                            </div>
                        </div>

                        {/* KP */}
                        <div className="flex items-center gap-2 px-3 py-2 bg-orange-50 rounded-xl border border-orange-100/60">
                            <div className="flex items-center justify-center w-8 h-8 bg-gradient-to-br from-orange-500 to-red-500 rounded-lg text-white shadow-sm font-bold text-sm">
                                K
                            </div>
                            <div className="flex flex-col leading-none">
                                <span className="text-lg font-bold text-gray-900">
                                    {movie.kp_rating !== null && movie.kp_rating !== -1 ? movie.kp_rating.toFixed(1) : '-'}
                                </span>
                                <span className="text-[10px] text-orange-600 font-bold uppercase tracking-wider">
                                    КП
                                </span>
                            </div>
                        </div>

                        {/* Жанры (скрываем на совсем маленьких, если не влезают) */}
                        {movie.genres && (
                            <div className="hidden sm:flex flex-wrap gap-2 ml-auto">
                                {movie.genres.split(',').slice(0, 3).map((genre, idx) => (
                                <span key={idx} className="px-2.5 py-1 bg-gray-100 text-gray-600 rounded-lg text-xs font-medium border border-gray-200">
                                    {genre.trim()}
                                </span>
                                ))}
                            </div>
                        )}
                    </div>
                  </div>

                  {/* --- ОСНОВНОЙ КОНТЕНТ (Сюжет слева, Трейлер справа) --- */}
                  <div className="flex flex-col xl:flex-row gap-8 h-full">
                    
                    {/* Левая колонка: Описание */}
                    <div className="flex-1">
                        {movie.overview && (
                            <div>
                            <h3 className="text-lg font-bold text-gray-900 mb-2 flex items-center gap-2">
                                <Film className="w-5 h-5 text-primary-500" />
                                Сюжет
                            </h3>
                            <p className="text-gray-600 leading-relaxed text-base font-light">
                                {movie.overview}
                            </p>
                            </div>
                        )}
                        
                        {/* Если жанры не влезли в шапку на мобиле, покажем их тут */}
                        {movie.genres && (
                            <div className="flex sm:hidden flex-wrap gap-2 mt-4">
                                {movie.genres.split(',').slice(0, 5).map((genre, idx) => (
                                <span key={idx} className="px-2.5 py-1 bg-gray-100 text-gray-600 rounded-lg text-xs font-medium border border-gray-200">
                                    {genre.trim()}
                                </span>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Правая колонка: Трейлер (Маленькое окошко) */}
                    {movie.trailer_key && movie.trailer_key !== 'none' && (
                        <div className="w-full xl:w-[400px] shrink-0">
                            <h3 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-2">
                                Трейлер
                            </h3>
                            <div className="relative w-full aspect-video rounded-xl overflow-hidden shadow-md border border-gray-200 bg-black">
                                <iframe
                                    src={`https://www.youtube.com/embed/${movie.trailer_key}?autoplay=0&rel=0&showinfo=0&iv_load_policy=3&modestbranding=1`}
                                    title="YouTube trailer"
                                    className="absolute top-0 left-0 w-full h-full"
                                    frameBorder="0"
                                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                                    allowFullScreen
                                    loading="lazy"
                                />
                            </div>
                        </div>
                    )}

                  </div>
                  
                </div>
              </div>
            </div>

            {/* Torrents Table */}
            <div className="bg-white rounded-3xl shadow-soft border border-gray-100 overflow-hidden">
              <div className="p-6 md:p-8 border-b border-gray-100 bg-gray-50/50">
                <h2 className="text-2xl font-display font-bold text-gray-900 flex items-center gap-3">
                  <Download className="h-6 w-6" />
                  {isBanned ? 'Файлы недоступны' : `Доступные раздачи для ${movie.title}`}
                </h2>
              </div>

              {isBanned ? (
                <div className="p-10 text-center">
                   <div className="mx-auto w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mb-4">
                     <X className="h-8 w-8 text-red-500" />
                   </div>
                   <h3 className="text-lg font-bold text-gray-800 mb-2">Доступ ограничен</h3>
                   <p className="text-gray-600">
                     Контент заблокирован по требованию правообладателя.
                   </p>
                </div>
              ) : (
                // Твой обычный код таблицы с торрентами
                torrents && torrents.length > 0 ? (
                  <>
                    {/* Tech Specs Block */}
                    {techInfoString && (
                        <div className="p-6 border-b border-gray-100 bg-gray-50/20 text-sm">
                          <h3 className="font-bold text-gray-900 mb-2">Техническая информация о файлах:</h3>
                          <p className="text-gray-600" dangerouslySetInnerHTML={{ __html: techInfoString }} />
                        </div>
                    )}
  
                    <div className="overflow-x-auto">
                      <table className="w-full text-left border-collapse">
                        <thead className="bg-gray-50/80 border-b border-gray-100">
                          <tr>
                            {/* Имя (Раздача) */}
                            <th 
                              onClick={() => handleSort('name')}
                              className="py-4 pl-4 md:pl-6 pr-2 font-semibold text-gray-500 text-xs uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors group select-none"
                            >
                              <div className="flex items-center gap-1">
                                Раздача
                                <SortIcon columnKey="name" />
                              </div>
                            </th>
                            
                            {/* Размер */}
                            <th 
                              onClick={() => handleSort('size')}
                              className="py-4 px-4 font-semibold text-gray-500 text-xs uppercase tracking-wider hidden md:table-cell cursor-pointer hover:bg-gray-100 transition-colors select-none"
                            >
                              <div className="flex items-center gap-1">
                                Размер
                                <SortIcon columnKey="size" />
                              </div>
                            </th>
                            
                            {/* Качество */}
                            <th 
                              onClick={() => handleSort('resolution')}
                              className="py-4 px-4 font-semibold text-gray-500 text-xs uppercase tracking-wider text-center hidden md:table-cell cursor-pointer hover:bg-gray-100 transition-colors select-none"
                            >
                              <div className="flex items-center justify-center gap-1">
                                Качество
                                <SortIcon columnKey="resolution" />
                              </div>
                            </th>
                            
                            {/* Инфо (Битрейт) */}
                            <th 
                              onClick={() => handleSort('bitrate')}
                              className="py-4 px-4 font-semibold text-gray-500 text-xs uppercase tracking-wider text-center hidden lg:table-cell cursor-pointer hover:bg-gray-100 transition-colors select-none"
                            >
                              <div className="flex items-center justify-center gap-1">
                                Инфо
                                <SortIcon columnKey="bitrate" />
                              </div>
                            </th>
                            
                            {/* Сиды / Пиры */}
                            <th 
                              onClick={() => handleSort('seeders')}
                              className="py-4 px-4 font-semibold text-gray-500 text-xs uppercase tracking-wider text-center hidden md:table-cell cursor-pointer hover:bg-gray-100 transition-colors select-none"
                            >
                              <div className="flex items-center justify-center gap-1">
                                S / L
                                <SortIcon columnKey="seeders" />
                              </div>
                            </th>
                            
                            {/* Пустая колонка для кнопки (без сортировки) */}
                            <th className="py-4 pr-4 font-semibold text-gray-500 text-xs uppercase tracking-wider text-right"></th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                          {sortedTorrents.map((torrent, index) => (
                            <TorrentRow key={index} torrent={torrent} />
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <div className="py-20 flex flex-col items-center justify-center text-center px-4">
                    <div className="w-16 h-16 bg-gray-100 rounded-full flex items-center justify-center mb-4">
                      <Download className="h-8 w-8 text-gray-400" />
                    </div>
                    <p className="text-gray-900 font-semibold text-lg">Торренты не найдены</p>
                    <p className="text-gray-500 text-sm mt-1">К сожалению, для этого фильма пока нет активных раздач.</p>
                  </div>
                )
              )}
            </div>

            {/* --- БЛОК РЕКОМЕНДАЦИЙ --- */}
            {recommendations && recommendations.length > 0 && (
              <div className="mt-12">
                <h3 className="text-2xl font-bold text-gray-900 mb-6 flex items-center gap-2">
                  <Sparkles className="w-6 h-6 text-yellow-500" />
                  Смотрите также
                </h3>
                {/* Список фильмов (горизонтальный скролл) */}
                <div className="flex overflow-x-auto space-x-4 pb-4">
                  {recommendations.map(rec => (
                    <MovieCard key={rec.id} movie={rec} />
                  ))}
                </div>
              </div>
            )}

            {!isBanned && (
              <div className="mt-12 bg-gray-50 rounded-3xl p-6 md:p-8 border border-gray-200">
                <h3 className="text-xl font-bold text-gray-900 mb-6 flex items-center gap-2">
                  <HelpCircle className="w-6 h-6 text-primary-600" />
                  Частые вопросы (FAQ)
                </h3>
                <div className="space-y-6">
                  {faqData.map((item, index) => (
                    <div key={index} itemScope itemProp="mainEntity" itemType="https://schema.org/Question">
                      <h4 className="font-semibold text-gray-900 mb-2 flex items-start gap-2" itemProp="name">
                        <span className="text-primary-500 font-bold">?</span> {item.q}
                      </h4>
                      <div itemScope itemProp="acceptedAnswer" itemType="https://schema.org/Answer">
                        <p className="text-sm text-gray-600 leading-relaxed pl-5 border-l-2 border-gray-200" itemProp="text">
                          {item.a}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
            
                      </div>
                  </main>
                  {/* Footer */}
                  <footer className="mt-20 border-t border-gray-200 bg-white/60 backdrop-blur-md">
                    <div className="container mx-auto px-4 md:px-6 py-10 text-center">
                      
                      {/* Логотип (Теперь картинка) */}
                      <div className="flex items-center justify-center gap-2 mb-4 opacity-90">
                        <Image 
                          src="/web_icons/favicon-32x32.png"
                          alt="CineTorrent Logo" 
                          width={24} 
                          height={24} 
                          className="w-6 h-6 object-contain" 
                          unoptimized // Для .ico файлов иногда нужно, чтобы не мылилось
                        />
                        <span className="font-display font-bold text-gray-800">CineTorrent</span>
                      </div>
          
                      {/* Копирайт */}
                      <p className="text-gray-500 text-sm mb-3">
                        &copy; 2025 CineTorrent. All Rights Reserved.
                      </p>
          
                      {/* Контакты: и для людей, и для роботов */}
                      <div className="text-sm text-gray-500 bg-gray-50/50 inline-block px-4 py-2 rounded-lg border border-gray-100">
                        <span className="font-medium text-gray-600">Связь с администрацией / DMCA:</span>
                        <br className="sm:hidden" /> 
                        <a href="mailto:help@cinetorrent.ru" className="ml-0 sm:ml-2 text-primary-600 hover:text-primary-700 font-bold hover:underline transition-colors">
                          help@cinetorrent.ru
                        </a>
                      </div>
                      
                    </div>
                  </footer>
                </div>
              </>  );
}

export async function getServerSideProps({ params }) {
  try {
    const movie = getMovieByIdSlug(params.id_slug);
    
    if (!movie) {
      return {
        notFound: true,
      };
    }

    const torrents = getTorrentsByTmdbId(movie.id).map(torrent => {
      // Convert size units to Russian
      torrent.size = convertEnglishUnitsToRussian(torrent.size);

      if (torrent.info_hash) {
        const cached = getTorrentDetailsByInfoHash(torrent.info_hash);
        if (cached) {
          try {
            // Безопасный парсинг списка файлов
            cached.files = JSON.parse(cached.files);
          } catch (e) {
            cached.files = [];
          }

          // Передаем расширенные данные в пропсы
          torrent.cached_details = {
             resolution: cached.resolution,
             hdr_type: cached.hdr_type,
             codec: cached.codec,
             audio: cached.audio,
             bitrate: cached.bitrate,
             files: cached.files,
             file_type: cached.file_type
          };
        }
      }
      return torrent;
    });

    // --- SEO Data Generation ---
    const qualities = torrents.map(t => t.cached_details?.resolution).filter(Boolean);
    const has4k = qualities.includes('4K');
    const has1080 = qualities.includes('1080p');

    let pageTitle = `Скачать фильм ${movie.title} ${movie.year} торрент бесплатно`;
    if (has4k) pageTitle += ' в 4K HDR';
    else if (has1080) pageTitle += ' в хорошем качестве (1080p)';
    pageTitle += ' | CineTorrent';

    const seoDescription = `Скачать фильм ${movie.title} ${movie.year} через торрент бесплатно и без регистрации. ${has4k ? 'Доступно в 4K Ultra HD с HDR и Dolby Vision. ' : ''}Сюжет: ${movie.overview ? movie.overview.substring(0, 150) : ''}... Быстрая загрузка, высокое качество, magnet-ссылка.`;
    
    const availableResolutions = [...new Set(
      torrents
        .map(t => t.cached_details?.resolution)
        .filter(r => r && r !== 'N/A')
    )];
    const dynamicKeywords = availableResolutions.map(r => `скачать ${r.toLowerCase()}`);

    // --- Technical Specs Data Generation ---
    const bitrates = torrents
        .map(t => t.cached_details?.bitrate)
        .filter(b => b)
        .map(b => parseFloat(b));

    const minBitrate = bitrates.length > 0 ? Math.min(...bitrates) : 0;
    const maxBitrate = bitrates.length > 0 ? Math.max(...bitrates) : 0;

    const hasHDR = torrents.some(t => t.cached_details?.hdr_type && t.cached_details.hdr_type !== 'SDR');
    
    const resolutionOrder = ['4K', '1080p', '720p'];
    const uniqueResolutions = [...availableResolutions].sort((a, b) => resolutionOrder.indexOf(a) - resolutionOrder.indexOf(b));

    // 1. Пробуем получить умные рекомендации через API (закэшированные в cache.db)
    let recommendations = await getRecommendationsApi(movie.id);

    // 2. Если API вернул мало фильмов (или фильм старый и API молчит, или фильмов нет у нас в базе)
    if (!recommendations || recommendations.length < 5) {
        // Добиваем случайными фильмами, чтобы блок не был пустым
        const randomRecs = getRandomMovies(12);
        
        // Объединяем, убирая дубликаты (чтобы один фильм не повторялся)
        const existingIds = new Set(recommendations.map(r => r.id));
        const filteredRandom = randomRecs.filter(r => !existingIds.has(r.id));
        
        recommendations = [...recommendations, ...filteredRandom].slice(0, 12);
    }

    return {
      props: {
        movie: JSON.parse(JSON.stringify({
          ...movie,
          local_poster_path: movie.local_poster_path
            ? movie.local_poster_path.replace('/home/niki/projects/torrent', '')
            : null,
          backdrop_path: movie.backdrop_path
            ? movie.backdrop_path.replace('/home/niki/projects/torrent', '')
            : null,
        })),
        torrents: JSON.parse(JSON.stringify(torrents)),
        pageTitle,
        seoDescription,
        recommendations: JSON.parse(JSON.stringify(recommendations)),
        seo: {
          dynamicKeywords,
        },
        techSpecs: {
            uniqueResolutions,
            minBitrate,
            maxBitrate,
            hasHDR
        }
      },
    };
  } catch (error) {
    console.error('Ошибка при получении данных фильма:', error);
    return {
      notFound: true,
    };
  }
}