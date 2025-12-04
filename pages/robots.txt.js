export default function RobotsTxt() {}

export async function getServerSideProps({ req, res }) {
  if (res) {
    const host = req.headers.host;
    const protocol = req.headers['x-forwarded-proto'] || (req.connection.encrypted ? 'https' : 'http');
    const siteUrl = `${protocol}://${host}`;

    // Правила для всех роботов
    // Disallow: /api/ - роботам не нужен JSON, им нужен HTML
    // Disallow: /search - результаты поиска не должны попадать в индекс (дубли контента)
    // Clean-param - подсказка для Яндекса не учитывать метки UTM и т.д.
    const robotsTxt = `User-agent: *
Allow: /
Disallow: /api/
Disallow: /_next/
Allow: /_next/static/
Allow: /_next/image/
Disallow: /search
Disallow: /*?*
Disallow: /404
Disallow: /500

# Специальные настройки для Яндекса
User-agent: Yandex
Allow: /
Disallow: /api/
Disallow: /search
Disallow: /*?*
Clean-param: utm_source&utm_medium&utm_campaign&ref ${host}

# Указание карты сайта (Обязательно в конце)
Sitemap: ${siteUrl}/sitemap.xml
`;

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    // Устанавливаем кэш на сутки, чтобы роботы не долбили этот файл слишком часто
    res.setHeader('Cache-Control', 'public, s-maxage=86400, stale-while-revalidate=43200');
    res.write(robotsTxt);
    res.end();
  }

  return {
    props: {},
  };
}