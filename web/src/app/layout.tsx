import type { Metadata } from "next";
import Script from "next/script";
import "./globals.css";
import "maplibre-gl/dist/maplibre-gl.css";

/**
 * GA4 측정 ID. 소스에 두지 않고 빌드 시 주입한다 (CI 의 secret).
 *
 * 값이 없으면 GA 스크립트를 아예 렌더하지 않는다 — 이 repo 를 fork 하거나
 * 로컬에서 돌리는 사람이 원저자의 GA 속성으로 데이터를 보내지 않도록.
 * 정적 export 라 빌드 타임에 인라인되므로 배포된 HTML 에는 값이 남는다
 * (측정 ID 는 원래 브라우저에 공개되는 값이라 정상).
 */
const GA_ID = process.env.NEXT_PUBLIC_GA_ID;

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
        {/* 지도 라벨(검색·조회 MapLibre symbol + 시계열추적 canvas) 공용 한글 서체.
            두 렌더러가 같은 Noto Sans KR 로 라벨을 그리도록 실제 로드. */}
        <link
          rel="preconnect"
          href="https://fonts.gstatic.com"
          crossOrigin="anonymous"
        />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@500;700&display=swap"
        />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(JSON_LD) }}
        />
      </head>
      <body suppressHydrationWarning>
        {children}
        {GA_ID && (
          <>
            <Script
              src={`https://www.googletagmanager.com/gtag/js?id=${GA_ID}`}
              strategy="afterInteractive"
            />
            <Script id="gtag-init" strategy="afterInteractive">
              {`
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', '${GA_ID}');
          `}
            </Script>
          </>
        )}
      </body>
    </html>
  );
}
