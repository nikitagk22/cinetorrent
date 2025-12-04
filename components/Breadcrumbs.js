import Link from 'next/link';
import { ChevronRight, Home } from 'lucide-react';

export default function Breadcrumbs({ items }) {
  if (!items) return null;

  // Структурированные данные для Google (Schema.org)
  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": items.map((item, index) => ({
      "@type": "ListItem",
      "position": index + 1,
      "name": item.label,
      "item": `https://cinetorrent.ru${item.href}`
    }))
  };

  return (
    <>
      {/* JSON-LD для поисковиков (скрыто, для роботов) */}
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />

      {/* Визуальная часть для людей */}
      <nav className="flex items-center text-sm text-gray-500 mb-6 overflow-x-auto whitespace-nowrap pb-2 md:pb-0">
        <Link href="/" className="hover:text-primary-600 transition-colors flex items-center gap-1">
          <Home className="w-4 h-4" />
          <span className="hidden sm:inline">Главная</span>
        </Link>

        {items.map((item, index) => (
          <div key={index} className="flex items-center">
            <ChevronRight className="w-4 h-4 mx-2 text-gray-300 flex-shrink-0" />
            {index === items.length - 1 ? (
              <span className="font-medium text-gray-800 truncate max-w-[200px] md:max-w-xs">
                {item.label}
              </span>
            ) : (
              <Link 
                href={item.href} 
                className="hover:text-primary-600 transition-colors"
              >
                {item.label}
              </Link>
            )}
          </div>
        ))}
      </nav>
    </>
  );
}