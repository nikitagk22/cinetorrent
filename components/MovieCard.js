import Link from 'next/link';
import Image from 'next/image';
import { Star, Calendar } from 'lucide-react';

export default function MovieCard({ movie }) {
  // Хелпер для получения жанра
  const getGenre = () => {
    if (!movie.genres) return null;
    if (Array.isArray(movie.genres)) return movie.genres[0]?.name || movie.genres[0];
    if (typeof movie.genres === 'string') return movie.genres.split(',')[0].trim();
    return null;
  };

  const genre = getGenre();

  // --- ЛОГИКА СРЕДНЕГО РЕЙТИНГА ---
  let sumRatings = 0;
  let numRatings = 0;

  if (movie.vote_average !== null && movie.vote_average !== 0 && movie.vote_average !== -1) {
    sumRatings += movie.vote_average;
    numRatings++;
  }

  if (movie.kp_rating !== null && movie.kp_rating !== 0 && movie.kp_rating !== -1) {
    sumRatings += movie.kp_rating;
    numRatings++;
  }

  let finalRating = 0;
  if (numRatings > 0) {
    finalRating = sumRatings / numRatings;
  }
  // --------------------------------

  return (
    <Link
      key={movie.id}
      href={`/movies/${movie.id_slug}`}
      // Добавили transform-gpu для использования видеокарты
      className="flex-shrink-0 w-44 md:w-48 snap-start group cursor-pointer relative py-4 px-1 transform-gpu"
    >
      <div 
        // УБРАЛИ style={{ contentVisibility: 'auto'... }} - это решит проблему мигания
        // Добавили will-change-transform для плавности при наведении
        className="flex flex-col h-full bg-white rounded-2xl shadow-sm hover:shadow-xl transition-all duration-300 overflow-hidden border border-gray-100 group-hover:-translate-y-1 will-change-transform"
      >
        
        {/* Poster Wrapper */}
        <div className="relative aspect-[2/3] bg-gray-100 overflow-hidden isolate">
          {movie.local_poster_path ? (
            <Image
              src={movie.local_poster_path.replace('/home/niki/projects/torrent', '')}
              alt={`Постер фильма ${movie.title}` || 'Постер'}
              fill
              // Убрали loading="lazy" по умолчанию для карточек в видимой зоне, 
              // но Next.js сам разберется. Главное - убрали content-visibility.
              className="object-cover group-hover:scale-105 transition-transform duration-500"
              sizes="(max-width: 768px) 176px, 192px"
            />
          ) : (
            <div className="w-full h-full flex flex-col items-center justify-center text-gray-300 gap-2">
              <span className="text-xs font-medium">Нет постера</span>
            </div>
          )}

          {/* Бейдж со средним рейтингом */}
          {finalRating > 0 && (
            <div className="absolute top-2 right-2 bg-white/90 px-2 py-1 rounded-lg shadow-sm flex items-center gap-1 z-10 border border-gray-100">
              <Star className="h-3 w-3 text-orange-400 fill-orange-400" />
              <span className="text-xs font-bold text-gray-800">
                {finalRating.toFixed(1)}
              </span>
            </div>
          )}
        </div>

        {/* Info Block */}
        <div className="p-3 flex flex-col flex-grow">
          <h3 className="font-bold text-sm md:text-base text-gray-800 leading-tight line-clamp-2 mb-2 group-hover:text-primary-600 transition-colors">
            {movie.title}
          </h3>
          
          <div className="mt-auto flex items-center justify-between text-xs text-gray-400 font-medium">
            <span className="flex items-center gap-1">
              {movie.year && (
                <>
                  <Calendar className="w-3 h-3" />
                  {movie.year}
                </>
              )}
            </span>
            {genre && (
              <span className="bg-gray-50 px-2 py-0.5 rounded-full border border-gray-100 truncate max-w-[70px]">
                {genre}
              </span>
            )}
          </div>
        </div>
      </div>
    </Link>
  );
}