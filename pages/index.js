import Head from 'next/head';
import Link from 'next/link';
import Image from 'next/image';
import { getMovies, getRandomMovies, getRandomMoviesByYear } from '../lib/db';
import MovieRow from '../components/MovieRow';
import SearchBar from '../components/SearchBar';
import MovieCard from '../components/MovieCard';

export default function Home({
  newMovies2025,
  mostPopular,
  mostViewed,
  randomMovies,
}) {
  const currentYear = new Date().getFullYear();

  return (
    <>
      <Head>
        <title>CineTorrent - Библиотека фильмов | Скачать торренты бесплатно</title>
        <meta
          name="description"
          content="CineTorrent - огромная библиотека фильмов с возможностью скачать торрент бесплатно. Новинки кино 2025, популярные фильмы, топ рейтинги. Более 50,000 фильмов в каталоге."
        />
        <meta name="keywords" content="скачать фильмы торрент, бесплатные фильмы, новинки кино 2025, популярные фильмы, торренты фильмов" />
        <link rel="canonical" href="https://cinetorrent.ru" />
        
        {/* Open Graph / Facebook */}
        <meta property="og:type" content="website" />
        <meta property="og:url" content="https://cinetorrent.ru" />
        <meta property="og:title" content="CineTorrent - Библиотека фильмов | Скачать торренты бесплатно" />
        <meta property="og:description" content="Огромная библиотека фильмов с возможностью скачать торрент бесплатно. Новинки кино, популярные фильмы и многое другое." />
        <meta property="og:site_name" content="CineTorrent" />
        <meta property="og:locale" content="ru_RU" />
        
        {/* Twitter Card */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="CineTorrent - Библиотека фильмов" />
        <meta name="twitter:description" content="Огромная библиотека фильмов с возможностью скачать торрент бесплатно." />
        
        {/* Additional SEO */}
        <meta name="robots" content="index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1" />
        <meta name="author" content="CineTorrent" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
        
        {/* Structured Data (JSON-LD) */}
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "WebSite",
              "name": "CineTorrent",
              "url": "https://cinetorrent.ru",
              "description": "Огромная библиотека фильмов с возможностью скачать торрент бесплатно",
              "potentialAction": {
                "@type": "SearchAction",
                "target": {
                  "@type": "EntryPoint",
                  "urlTemplate": "https://cinetorrent.ru/api/search?q={search_term_string}"
                },
                "query-input": "required name=search_term_string"
              }
            })
          }}
        />
      </Head>

      <div className="min-h-screen pb-20">
        {/* Header (Optimized Glassmorphism) */}
        <header className="sticky top-0 z-40 transform-gpu">
          {/* Фон хедера: используем MD blur вместо XL для производительности */}
          <div className="absolute inset-0 bg-white/90 backdrop-blur-md border-b border-white/40 shadow-sm" />
          
          <div className="container mx-auto px-4 md:px-6 relative z-10">
            <div className="flex flex-col md:flex-row items-center justify-between py-3 md:py-4 gap-4">
              
              {/* Logo Area */}
              <div className="flex items-center justify-between w-full md:w-auto">
                <Link href="/" className="flex items-center gap-2.5 group">
                  <div className="w-9 h-9 bg-gradient-to-br from-primary-600 to-primary-800 rounded-lg flex items-center justify-center text-white shadow-lg shadow-primary-500/20 transform-gpu group-hover:scale-105 transition-transform duration-200">
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                  </div>
                  <span className="text-xl font-display font-bold text-gray-800 tracking-tight">
                    Cine<span className="text-primary-600">Torrent</span>
                  </span>
                </Link>
              </div>
              
              {/* SearchBar Container */}
              <div className="w-full md:max-w-xl lg:max-w-2xl">
                <SearchBar />
              </div>

              {/* Spacer for Desktop Balance (чтобы поиск был по центру) */}
              <div className="hidden md:block w-[140px]"></div>
            </div>
          </div>
        </header>

        <main className="container mx-auto px-4 md:px-6 py-8 md:py-12 space-y-14 md:space-y-16">
          
          {/* Section 1: Новинки */}
          {newMovies2025 && newMovies2025.length > 0 && (
            <section className="animate-fade-in">
              <MovieRow
                movies={newMovies2025}
                title={`Новинки ${currentYear}`}
              />
            </section>
          )}

          {/* Section 2: Популярные */}
          {mostPopular && mostPopular.length > 0 && (
            <section>
              <MovieRow 
                movies={mostPopular} 
                title="Популярные фильмы" 
              />
            </section>
          )}

          {/* Section 3: Сейчас смотрят */}
          {mostViewed && mostViewed.length > 0 && (
            <section>
              <MovieRow
                movies={mostViewed}
                title="Сейчас смотрят"
              />
            </section>
          )}

          {/* Section 4: Случайные фильмы (Grid) */}
          {randomMovies && randomMovies.length > 0 && (
            <section>
              <div className="flex items-center gap-3 mb-6 px-2 border-l-4 border-primary-500 py-1 ml-4 md:ml-6">
                <h2 className="text-2xl md:text-3xl font-display font-bold text-gray-900">
                  Случайный выбор
                </h2>
              </div>
              
              <div className="px-4 md:px-6">
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-x-4 md:gap-x-5 gap-y-8">
                  {randomMovies.map((movie) => (
                    <MovieCard key={movie.id} movie={movie} />
                  ))}
                </div>
              </div>
            </section>
          )}

          {/* Empty State / Error State */}
          {(!newMovies2025 || newMovies2025.length === 0) &&
            (!mostPopular || mostPopular.length === 0) &&
            (!mostViewed || mostViewed.length === 0) &&
            (!randomMovies || randomMovies.length === 0) && (
              <div className="flex flex-col items-center justify-center py-24 px-4 bg-white/50 backdrop-blur-sm rounded-3xl border border-dashed border-gray-300 mx-4 md:mx-6">
                <div className="w-20 h-20 bg-gray-100 rounded-full flex items-center justify-center mb-4">
                  <svg className="w-10 h-10 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 4v16M17 4v16M3 8h4m10 0h4M3 12h18M3 16h4m10 0h4M4 20h16a1 1 0 001-1V5a1 1 0 00-1-1H4a1 1 0 00-1 1v14a1 1 0 001 1z" />
                  </svg>
                </div>
                <h3 className="text-gray-900 text-xl font-semibold mb-2">
                  Фильмы не найдены
                </h3>
                <p className="text-gray-500 text-center max-w-md text-sm">
                  Попробуйте изменить параметры запроса или обновите страницу позже.
                </p>
              </div>
            )}
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
              &copy; {currentYear} CineTorrent. All Rights Reserved.
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
    </>
  );
}

export async function getServerSideProps() {
  try {
    const currentYear = new Date().getFullYear();
    const [newMovies2025, mostPopular, mostViewed, randomMovies] =
      await Promise.all([
        getRandomMoviesByYear(currentYear, 20),
        getMovies({
          limit: 20,
          orderBy: 'vote_average',
          orderDirection: 'DESC',
          minVoteCount: 600,
        }),
        getMovies({
          limit: 20,
          orderBy: 'vote_count',
          orderDirection: 'DESC',
        }),
        getRandomMovies(100),
      ]);
      
    return {
      props: {
        newMovies2025: JSON.parse(JSON.stringify(newMovies2025)),
        mostPopular: JSON.parse(JSON.stringify(mostPopular)),
        mostViewed: JSON.parse(JSON.stringify(mostViewed)),
        randomMovies: JSON.parse(JSON.stringify(randomMovies)),
      },
    };
  } catch (error) {
    console.error('Ошибка при получении данных:', error);
    return {
      props: {
        newMovies2025: [],
        mostPopular: [],
        mostViewed: [],
        randomMovies: [],
      },
    };
  }
}