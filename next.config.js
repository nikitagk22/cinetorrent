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
    // 1. Для клиентской сборки (браузер)
    if (!isServer) {
      // Заменяем серверные модули на заглушки, чтобы не было ошибок "Can't resolve 'fs'"
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        path: false,
        net: false,
        tls: false,
        child_process: false,
      };
    }

    // 2. Для серверной сборки (Node.js)
    if (isServer) {
        // Добавляем better-sqlite3 в "externals", чтобы webpack не пытался его упаковать.
        // Это нативная библиотека (C++), она должна загружаться как есть.
        config.externals.push('better-sqlite3');
    }

    return config;
  },
}

module.exports = nextConfig

