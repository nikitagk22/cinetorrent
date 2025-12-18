import { getMovieSlugsCount, getMovieSlugsPaginated } from '../lib/db';

// Лимит 1500 (должен совпадать с [filename].js)
const SITEMAP_LIMIT = 1500;
const FALLBACK_DATE = '2025-12-04';

function generateSitemapIndex(sitemapsData) {
  let xml = '<?xml version="1.0" encoding="UTF-8"?>';
  xml += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">';

  sitemapsData.forEach(sitemap => {
    xml += `
      <sitemap>
        <loc>${sitemap.loc}</loc>
        <lastmod>${sitemap.lastmod}</lastmod>
      </sitemap>
    `;
  });

  xml += '</sitemapindex>';
  return xml;
}

export default function SitemapIndex() {}

export async function getServerSideProps({ req, res }) {
  if (res) {
    const host = req.headers.host;
    const protocol = req.headers['x-forwarded-proto'] || (req.connection.encrypted ? 'https' : 'http');
    const siteUrl = `${protocol}://${host}`;

    try {
      const totalMovies = getMovieSlugsCount();
      const safeTotal = totalMovies > 0 ? totalMovies : 1;
      const totalPages = Math.ceil(safeTotal / SITEMAP_LIMIT);

      const sitemapsData = [];

      // Проходим по всем страницам сайтмапа
      for (let i = 0; i < totalPages; i++) {
        const pageNum = i + 1;
        const offset = i * SITEMAP_LIMIT;
        
        // Запрашиваем фильмы только для текущей страницы
        const slugs = getMovieSlugsPaginated({ limit: SITEMAP_LIMIT, offset });

        let maxDateInPage = null;
        
        // Ищем самую свежую дату среди фильмов ТОЛЬКО этой страницы
        if (slugs && slugs.length > 0) {
          const dates = slugs
            .map(s => s.updated_at)
            .filter(d => d); // Убираем пустые даты
          
          if (dates.length > 0) {
             // Сортируем по убыванию (самая новая дата первая)
             dates.sort((a, b) => (a > b ? -1 : 1));
             maxDateInPage = dates[0];
          }
        }

        // Если нашли дату у фильмов — ставим её. Если нет — ставим заглушку.
        const finalLastMod = maxDateInPage || FALLBACK_DATE;

        sitemapsData.push({
          loc: `${siteUrl}/sitemaps/sitemap-${pageNum}.xml`,
          lastmod: finalLastMod
        });
      }

      const sitemapIndex = generateSitemapIndex(sitemapsData);

      res.setHeader('Content-Type', 'text/xml; charset=utf-8');
      // Кэш на 12 часов
      res.setHeader('Cache-Control', 'public, s-maxage=43200, stale-while-revalidate=21600');
      res.write(sitemapIndex);
      res.end();
    } catch (error) {
      console.error('Error generating sitemap index:', error);
      res.statusCode = 500;
      res.end();
    }
  }

  return { props: {} };
}