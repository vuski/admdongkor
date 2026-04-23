import type { Level } from "admdongkor";

export interface HoverInfo {
  side: "A" | "B";
  level: Level;
  /** 화면 좌표 — 툴팁 위치용. */
  x: number;
  y: number;
  /** feature properties 를 평탄하게. 레벨에 따라 fields 가 다름. */
  sidonm?: string | null;
  sidocd?: string | null;
  sggnm?: string | null;
  sggcd?: string | null;
  emdnm?: string | null;
  emdcd?: string | null;
  emd7?: string | null;
  emd8?: string | null;
  /** m². */
  area?: number;
}
