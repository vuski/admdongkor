import type { Metadata } from "next";
import "./globals.css";
import "maplibre-gl/dist/maplibre-gl.css";

export const metadata: Metadata = {
  title: "admdongkor — 한국 행정동 경계 지도",
  description:
    "1975년부터 현재까지 62개 시점의 한국 행정동(emd/sgg/sido) 경계를 조회·검색·비교하는 인터랙티브 지도.",
  metadataBase: new URL("https://admdongkor.vw-lab.com"),
  openGraph: {
    title: "admdongkor",
    description:
      "1975년부터 현재까지 한국 행정동 경계를 조회·검색·비교하는 지도.",
    url: "https://admdongkor.vw-lab.com",
    siteName: "admdongkor",
    type: "website",
    locale: "ko_KR",
  },
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
      </head>
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
