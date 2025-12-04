'use client';

import { useState, useEffect, useRef } from 'react';
import { Search, X, Film, Loader2 } from 'lucide-react';
import Link from 'next/link';

export default function SearchBar() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const wrapperRef = useRef(null);
  const searchTimeoutRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    function handleClickOutside(event) {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const performSearch = async (searchQuery) => {
    if (!searchQuery || searchQuery.trim() === '') {
      setResults([]);
      setIsSearching(false);
      return;
    }

    setIsSearching(true);
    try {
      const response = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}`);
      if (!response.ok) throw new Error('Ошибка сети');
      
      const data = await response.json();
      setResults(data.movies || []);
      setIsOpen(true);
    } catch (error) {
      console.error('Ошибка поиска:', error);
      setResults([]);
    } finally {
      setIsSearching(false);
    }
  };

  const handleInputChange = (e) => {
    const value = e.target.value;
    setQuery(value);
    
    if (value.trim() === '') {
      setResults([]);
      setIsOpen(false);
      setIsSearching(false);
      return;
    }

    setIsOpen(true);
    
    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current);
    }

    searchTimeoutRef.current = setTimeout(() => {
      performSearch(value);
    }, 300);
  };

  const clearSearch = () => {
    setQuery('');
    setResults([]);
    setIsOpen(false);
    setIsSearching(false);
    if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
  };

  const handleLinkClick = () => {
    setIsOpen(false);
    if (inputRef.current) {
      inputRef.current.blur();
    }
  };

  // Хелпер расчета рейтинга внутри компонента
  const calculateRating = (movie) => {
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

    if (numRatings > 0) {
      return sumRatings / numRatings;
    }
    return 0; // Return 0 if no valid ratings
  };

  return (
    <div ref={wrapperRef} className="relative w-full max-w-2xl mx-auto z-50">
      <div className="relative group">
        <div className="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none">
          <Search className="h-5 w-5 text-gray-400 group-focus-within:text-primary-500 transition-colors" />
        </div>
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleInputChange}
          onFocus={() => { if (query) setIsOpen(true); }}
          placeholder="Найти фильм, сериал..."
          className="block w-full pl-11 pr-12 py-3.5 bg-white/90 backdrop-blur-xl border border-gray-200/60 rounded-full shadow-soft text-gray-800 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-primary-500/20 focus:border-primary-500 transition-all text-base font-medium"
          autoComplete="off"
        />
        {query && (
          <button
            onClick={clearSearch}
            className="absolute inset-y-0 right-0 pr-4 flex items-center text-gray-400 hover:text-gray-600 transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        )}
      </div>

      <div 
        className={`absolute w-full mt-3 bg-white/95 backdrop-blur-xl rounded-2xl shadow-card border border-gray-100 overflow-hidden transition-all duration-200 origin-top z-50 ${
          isOpen && query.trim() !== '' ? 'opacity-100 scale-100 translate-y-0' : 'opacity-0 scale-95 -translate-y-2 pointer-events-none'
        }`}
      >
        {isSearching ? (
          <div className="p-8 flex flex-col items-center justify-center text-gray-400">
            <Loader2 className="h-6 w-6 animate-spin mb-2 text-primary-500" />
            <span className="text-sm font-medium">Поиск...</span>
          </div>
        ) : results.length > 0 ? (
          <div className="max-h-[60vh] overflow-y-auto custom-scrollbar">
            <div className="px-4 py-2 bg-gray-50/50 border-b border-gray-100 text-xs font-semibold text-gray-400 uppercase tracking-wider sticky top-0 backdrop-blur-sm">
              Результаты
            </div>
            {results.map((movie) => {
              const rating = calculateRating(movie);
              
              return (
                <Link
                  key={movie.id}
                  href={`/movies/${movie.id_slug}`}
                  onClick={handleLinkClick}
                  className="flex items-center gap-4 px-4 py-3 hover:bg-primary-50/50 transition-colors border-b border-gray-50 last:border-0 group"
                >
                  <div className="flex-shrink-0 w-10 h-14 bg-gray-200 rounded-lg overflow-hidden shadow-sm relative">
                    {movie.local_poster_path ? (
                      <img
                        src={movie.local_poster_path.replace('/home/niki/projects/torrent', '')}
                        alt={movie.title}
                        className="w-full h-full object-cover"
                      />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center bg-gray-100 text-gray-400">
                        <Film className="w-4 h-4" />
                      </div>
                    )}
                  </div>
                  
                  <div className="flex-1 min-w-0">
                    <h3 className="text-sm font-semibold text-gray-800 truncate group-hover:text-primary-700 transition-colors">
                      {movie.title}
                    </h3>
                    <div className="flex items-center gap-3 text-xs text-gray-500 mt-1">
                      {movie.year && <span>{movie.year}</span>}
                      {rating > 0 && (
                        <span className="flex items-center gap-1 text-orange-500 font-medium bg-orange-50 px-1.5 rounded text-[10px]">
                          ★ {rating.toFixed(1)}
                        </span>
                      )}
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
        ) : (
          <div className="p-8 text-center">
            <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-gray-100 mb-3">
              <Search className="h-6 w-6 text-gray-400" />
            </div>
            <p className="font-medium text-gray-900">Ничего не найдено</p>
            <p className="text-sm text-gray-500 mt-1">Попробуйте изменить запрос</p>
          </div>
        )}
      </div>
    </div>
  );
}