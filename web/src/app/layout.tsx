import type { Metadata } from "next";
import Script from "next/script";
import "./globals.css";
import "maplibre-gl/dist/maplibre-gl.css";

const SITE_URL = "https://admdongkor.vw-lab.com";
const OG_IMAGE = `${SITE_URL}/admdongkor.png`;
const TITLE = "admdongkor — 대한민국 행정동 경계 지도 (1975–2026)";
const DESCRIPTION =
  "1975년부터 현재까지 62개 시점의 대한민국 행정동(읍면동·시군구·시도) 경계를 조회·검색·비교하는 인터랙티브 지도. 변경이력과 통계청 코드 매칭까지 한눈에.";

export const metadata: Metadata = {
  title: TITLE,
  description: DESCRIPTION,
  metadataBase: new URL(SITE_URL),
  keywords: [
    "행정동",
    "읍면동",
    "시군구",
    "시도",
    "행정경계",
    "행정구역",
    "대한민국 지도",
    "시계열 지도",
    "행정구역 변천",
    "admdongkor",
    "korea administrative boundary",
    "GIS",
    "GeoJSON",
    "parquet",
  ],
  authors: [{ name: "VWL Inc.", url: "https://www.vw-lab.com" }],
  creator: "VWL Inc.",
  robots: { index: true, follow: true },
  alternates: { canonical: SITE_URL },
  icons: { icon: "/admdongkor.png" },
  openGraph: {
    title: TITLE,
    description: DESCRIPTION,
    url: SITE_URL,
    siteName: "admdongkor",
    type: "website",
    locale: "ko_KR",
    images: [{ url: OG_IMAGE, width: 1200, height: 630, alt: "admdongkor" }],
  },
  twitter: {
    card: "summary_large_image",
    title: TITLE,
    description: DESCRIPTION,
    images: [OG_IMAGE],
  },
};

const JSON_LD = {
  "@context": "https://schema.org",
  "@type": "WebApplication",
  name: "admdongkor",
  url: SITE_URL,
  description: DESCRIPTION,
  applicationCategory: "DataVisualizationApplication",
  operatingSystem: "Web Browser",
  inLanguage: "ko",
  author: {
    "@type": "Organization",
    name: "VWL Inc.",
    url: "https://www.vw-lab.com",
  },
  offers: { "@type": "Offer", price: "0", priceCurrency: "KRW" },
  keywords:
    "행정동, 읍면동, 시군구, 시도, 행정경계, 대한민국 지도, 시계열 지도, 행정구역 변천",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ko" suppressHydrationWarning>
      <head>
        <link
          rel="preconnect"
          href="https://cdn.jsdelivr.net"
          crossOrigin="anonymous"
        />
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.min.css"
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(JSON_LD) }}
        />
      </head>
      <body suppressHydrationWarning>
        {children}
        <Script
          src="https://www.googletagmanager.com/gtag/js?id=G-DQGV42KK1P"
          strategy="afterInteractive"
        />
        <Script id="gtag-init" strategy="afterInteractive">
          {`
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', 'G-DQGV42KK1P');
          `}
        </Script>
      </body>
    </html>
  );
}
