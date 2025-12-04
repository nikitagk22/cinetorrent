import { getMovieSlugsPaginated } from '../../lib/db';

// Лимит тоже ставим 10 000 (должен совпадать с первым файлом)
const SITEMAP_LIMIT = 1500;

// Функция для защиты от спецсимволов (&, <, >)
function escapeXml(unsafe) {
    if (!unsafe) return '';
    return unsafe.replace(/[<>&'"]/g, function (c) {
        switch (c) {
            case '<': return '&lt;';
            case '>': return '&gt;';
            case '&': return '&amp;';
            case '\'': return '&apos;';
            case '"': return '&quot;';
        }
    });
}

function generateSiteMap(slugs, siteUrl, isFirstPage) {
  const currentDate = '2025-11-28';
  
  let xml = '<?xml version="1.0" encoding="UTF-8"?>';
  xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">';
  
  // Главная страница только в первом файле
  if (isFirstPage) {
    xml += `
      <url>
        <loc>${siteUrl}/</loc>
        <lastmod>${currentDate}</lastmod>
        <changefreq>daily</changefreq>
        <priority>1.0</priority>
      </url>
    `;
  }

  xml += slugs.map(slug => `
    <url>
      <loc>${siteUrl}/movies/${escapeXml(slug.id_slug)}</loc>
      <lastmod>${currentDate}</lastmod>
      <changefreq>weekly</changefreq>
      <priority>0.8</priority>
    </url>
  `).join('');

  xml += '</urlset>';
  return xml;
}

export default function SiteMapPage() {}

export async function getServerSideProps({ req, res, params }) {
  if (res) {
    const host = req.headers.host;
    const protocol = req.headers['x-forwarded-proto'] || (req.connection.encrypted ? 'https' : 'http');
    const siteUrl = `${protocol}://${host}`;

    try {
      const filename = params.filename;
      const match = filename.match(/^sitemap-(\d+)\.xml$/);

      if (!match) {
        return { notFound: true };
      }

      const page = parseInt(match[1], 10);

      if (isNaN(page) || page < 1) {
        return { notFound: true };
      }

      const offset = (page - 1) * SITEMAP_LIMIT;
      const slugs = getMovieSlugsPaginated({ limit: SITEMAP_LIMIT, offset });

      // Если страница пустая и это не первая страница -> 404
      if (slugs.length === 0 && page !== 1) {
         return { notFound: true };
      }

      const sitemap = generateSiteMap(slugs, siteUrl, page === 1);

      res.setHeader('Content-Type', 'text/xml; charset=utf-8');
      // Ставим кэш поменьше (1 час), чтобы Google быстрее увидел изменения
      res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=1800');
      res.write(sitemap);
      res.end();
    } catch (error) {
      console.error(`Error generating sitemap for ${params.filename}:`, error);
      res.statusCode = 500;
      res.end();
    }
  }

  return {
    props: {},
  };
}