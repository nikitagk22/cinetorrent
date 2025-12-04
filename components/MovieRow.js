import MovieCard from './MovieCard';
import { ChevronRight } from 'lucide-react';

export default function MovieRow({ movies, title }) {
  if (!movies || movies.length === 0) {
    return null;
  }

  return (
    <div className="mb-12 relative">
      <div className="flex items-center justify-between mb-6 px-4 md:px-6">
        <div className="flex items-center gap-3 border-l-4 border-primary-500 py-1 pl-3">
          <h2 className="text-2xl md:text-3xl font-display font-bold text-gray-900 tracking-tight">
            {title}
          </h2>
        </div>
      </div>

      <div className="relative">
        <div className="absolute left-0 top-0 bottom-4 w-8 bg-gradient-to-r from-gray-50 to-transparent z-10 pointer-events-none md:block hidden" />
        <div className="absolute right-0 top-0 bottom-4 w-8 bg-gradient-to-l from-gray-50 to-transparent z-10 pointer-events-none md:block hidden" />

        {/* 
            ИЗМЕНЕНИЯ:
            1. Заменили snap-mandatory на snap-proximity (меньше "залипаний" на ПК).
            2. Добавили !overflow-y-hidden, чтобы точно не появлялся вертикальный скролл.
            3. Убрали touch-pan классы, оставили нативное поведение.
        */}
        <div className="flex overflow-x-auto custom-scrollbar gap-4 md:gap-5 px-4 md:px-6 pb-6 snap-x snap-proximity scroll-smooth !overflow-y-hidden">
          {movies.map((movie) => (
            <MovieCard key={movie.id} movie={movie} />
          ))}
          {/* Пустой блок в конце для отступа */}
          <div className="w-2 flex-shrink-0" />
        </div>
      </div>
    </div>
  );
}