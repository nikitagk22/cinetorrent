import Head from 'next/head';
import Script from 'next/script'; // <--- ЭТОЙ СТРОКИ НЕ ХВАТАЛО
import '../styles/globals.css';

export default function App({ Component, pageProps }) {
  return (
    <>
      <Head>
        {/* --- ГЛОБАЛЬНЫЕ SEO НАСТРОЙКИ --- */}

        {/* 1. Верификация Вебмастеров */}
        <meta name="yandex-verification" content="335f25316f9d4261" /> 
        
        {/* 2. Технические настройки */}
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=5" />
        <meta charSet="utf-8" />
        <meta httpEquiv="X-UA-Compatible" content="IE=edge" />
        <meta name="theme-color" content="#ffffff" />
        <meta name="format-detection" content="telephone=no" />

        {/* 3. Гео и Язык */}
        <meta httpEquiv="content-language" content="ru" />
        <meta name="geo.region" content="RU" />

        {/* 4. Иконки */}
        <link rel="apple-touch-icon" sizes="180x180" href="/web_icons/apple-touch-icon.png" />
        <link rel="icon" type="image/png" sizes="32x32" href="/web_icons/favicon-32x32.png" />
        <link rel="icon" type="image/png" sizes="16x16" href="/web_icons/favicon-16x16.png" />
        <link rel="manifest" href="/web_icons/site.webmanifest" />
        <meta name="msapplication-config" content="/web_icons/browserconfig.xml" />

        {/* 5. Open Graph */}
        <meta property="og:locale" content="ru_RU" />
        <meta property="og:site_name" content="CineTorrent" />
        <meta property="og:type" content="website" />
      </Head>
      
      {/* --- Google Analytics (GA4) --- */}
      <Script
        src="https://www.googletagmanager.com/gtag/js?id=G-SW8Q3SLQ4S"
        strategy="afterInteractive"
      />
      <Script id="google-analytics" strategy="afterInteractive">
        {`
          window.dataLayer = window.dataLayer || [];
          function gtag(){dataLayer.push(arguments);}
          gtag('js', new Date());

          gtag('config', 'G-SW8Q3SLQ4S');
        `}
      </Script>

      {/* --- Yandex.Metrika --- */}
      <Script id="yandex-metrika" strategy="afterInteractive">
        {`
          (function(m,e,t,r,i,k,a){
              m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};
              m[i].l=1*new Date();
              for (var j = 0; j < document.scripts.length; j++) {if (document.scripts[j].src === r) { return; }}
              k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)
          })(window, document,'script','https://mc.yandex.ru/metrika/tag.js?id=105467664', 'ym');

          ym(105467664, 'init', {
            ssr: true,
            webvisor: true,
            clickmap: true,
            ecommerce: "dataLayer",
            accurateTrackBounce: true,
            trackLinks: true
          });
        `}
      </Script>
      <noscript>
        <div>
          <img 
            src="https://mc.yandex.ru/watch/105467664" 
            style={{ position: 'absolute', left: '-9999px' }} 
            alt="" 
          />
        </div>
      </noscript>
      
      {/* Фиксированный фон */}
      <div className="fixed-bg" />
      
      <Component {...pageProps} />
    </>
  );
}