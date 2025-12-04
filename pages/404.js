import Link from 'next/link';
import Head from 'next/head';
import { Film, Home, Search } from 'lucide-react';

export default function Custom404() {
  return (
    <>
      <Head>
        <title>Страница не найдена | CineTorrent</title>
        <meta name="robots" content="noindex, follow" />
      </Head>
      
      <div className="min-h-screen flex items-center justify-center bg-gray-50 p-4">
        <div className="text-center max-w-md w-full bg-white/80 backdrop-blur-xl p-8 rounded-3xl shadow-card border border-white/50">
          
        <div className="w-24 h-24 bg-primary-50 rounded-full flex items-center justify-center mx-auto mb-6 shadow-inner">
            <span className="font-bold text-3xl text-primary-600">404</span>
        </div>

          <h1 className="text-2xl font-display font-bold text-gray-900 mb-2">
            Такой страницы не существует
          </h1>
          
          <p className="text-gray-500 mb-8">
            К сожалению, эта страница была удалена или никогда не существовала. Но у нас есть тысячи других отличных фильмов.
          </p>

          <div className="space-y-3">
            <Link
              href="/"
              className="flex items-center justify-center gap-2 w-full py-3.5 bg-primary-600 hover:bg-primary-700 text-white rounded-xl font-medium transition-all shadow-lg shadow-primary-500/30"
            >
              <Home className="w-4 h-4" />
              На главную
            </Link>
            
            {/* Можно добавить ссылку на поиск, если нужно */}
          </div>
        </div>
      </div>
    </>
  );
}