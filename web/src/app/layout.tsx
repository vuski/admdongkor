import type { Metadata } from "next";
import "./globals.css";
import "maplibre-gl/dist/maplibre-gl.css";

export const metadata: Metadata = {
  title: "admdongkor — 한국 행정동 경계 지도",
  description:
    "1975년부터 현재까지 62개 시점의 한국 행정동(emd/sgg/sido) 경계를 조회·검색·비교하는 인터랙티브 지도.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ko" suppressHydrationWarning>
      <body suppressHydrationWarning>{children}</body>
    </html>
  );
}
