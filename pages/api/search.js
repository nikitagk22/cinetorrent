import { searchMovies } from '../../lib/db';

export default function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Метод не разрешен' });
  }

  const { q } = req.query;

  if (!q || typeof q !== 'string' || q.trim() === '') {
    return res.status(200).json({ movies: [] });
  }

  try {
    // Лимит 30 записей - оптимально для выпадающего списка
    const movies = searchMovies(q, 30);

    const processedMovies = movies.map(movie => {
      const processedLocalPosterPath = movie.local_poster_path
        ? movie.local_poster_path.replace('/home/niki/projects/torrent', '')
        : '/no-image.jpg';
      const processedBackdropPath = movie.backdrop_path
        ? movie.backdrop_path.replace('/home/niki/projects/torrent', '')
        : '/no-image.jpg';

      return {
        ...movie,
        local_poster_path: processedLocalPosterPath,
        backdrop_path: processedBackdropPath,
      };
    });
    return res.status(200).json({ movies: processedMovies });
  } catch (error) {
    console.error('Ошибка API поиска:', error);
    return res.status(500).json({ error: 'Внутренняя ошибка сервера' });
  }
}