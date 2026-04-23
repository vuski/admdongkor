import type { Level } from "admdongkor";

export type RGBA = [number, number, number, number];

// koreaData 팔레트 (light 테마):
//   sido #1c7057, sgg #7ab648, emd #999999
// A (주 선택) = 녹색 계열 그대로, B (비교) = 주황 계열.

export const A_LINE_BY_LEVEL: Record<Level, RGBA> = {
  sido: [28, 112, 87, 230], // #1c7057
  sgg: [94, 143, 54, 210], // #5e8f36 (7ab648 살짝 짙게)
  emd: [110, 110, 110, 180], // #6e6e6e
};

export const A_FILL_BY_LEVEL: Record<Level, RGBA> = {
  sido: [28, 112, 87, 18],
  sgg: [122, 182, 72, 14],
  emd: [150, 150, 150, 10],
};

// B 팔레트: 주황/갈색 계열 (slider B · badge B 와 통일 — #c2500f 기준)
export const B_LINE_BY_LEVEL: Record<Level, RGBA> = {
  sido: [194, 80, 15, 230], // #c2500f
  sgg: [214, 110, 40, 210],
  emd: [180, 130, 90, 180],
};

export const B_FILL_BY_LEVEL: Record<Level, RGBA> = {
  sido: [194, 80, 15, 18],
  sgg: [214, 110, 40, 14],
  emd: [180, 130, 90, 10],
};

/** 레벨별 선 굵기 (px). sido 가 가장 굵고 emd 가 가장 얇음. */
export function lineWidthForLevel(level: Level): number {
  switch (level) {
    case "sido":
      return 3.2;
    case "sgg":
      return 2.0;
    case "emd":
      return 1.0;
  }
}

/** deck.gl 레이어 z-순서용: 작을수록 아래. emd 아래, sgg 중간, sido 위. */
export function zOrderForLevel(level: Level): number {
  return level === "emd" ? 0 : level === "sgg" ? 1 : 2;
}
