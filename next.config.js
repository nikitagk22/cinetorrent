/** @type {import('next').NextConfig} */
const nextConfig = {
  // SSR режим - страницы генерируются на сервере при каждом запросе
  images: {
    unoptimized: true,
    // Для локальных изображений из базы данных
    domains: [],
  },
  reactStrictMode: true,
  webpack: (config, { isServer }) => {
    if (isServer) {
      // Исключаем better-sqlite3 из клиентской сборки
      config.externals.push('better-sqlite3');
    }
    return config;
  },
}

module.exports = nextConfig

