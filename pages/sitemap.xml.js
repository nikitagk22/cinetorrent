import { getMovieSlugsCount } from '../lib/db';

// Устанавливаем лимит 10 000
const SITEMAP_LIMIT = 1500;

function generateSitemapIndex(totalPages, siteUrl) {
  const currentDate = '2025-12-04';

  let xml = '<?xml version="1.0" encoding="UTF-8"?>';
  xml += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">';

  for (let i = 0; i < totalPages; i++) {
    xml += `
      <sitemap>
        <loc>${siteUrl}/sitemaps/sitemap-${i + 1}.xml</loc>
        <lastmod>${currentDate}</lastmod>
      </sitemap>
    `;
  }

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

      const sitemapIndex = generateSitemapIndex(totalPages, siteUrl);

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

  return {
    props: {},
  };
}