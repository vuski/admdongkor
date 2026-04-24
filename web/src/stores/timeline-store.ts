"use client";

import { create } from "zustand";
import type { DecodedGeometry } from "@/lib/wkb";

export interface TimelineQuerySelection {
  code: string;
  name: string;
  /** sgg 인 경우 sido 이름도 함께 (표시용). */
  parentName?: string;
}

export interface TimelineQuery {
  /** 기준 버전 (YYYYMMDD). */
  baseVersion: string;
  level: "sido" | "sgg";
  /** 선택된 지역들. 복수 가능. */
  selections: TimelineQuerySelection[];
}

/** 모든 캔버스가 공유하는 viewport (lon/lat world 기준).
 *  center 가 화면 중앙에 오고, scale 은 '1 단위 위도 당 픽셀' 근사치. */
export interface TimelineViewport {
  center: [number, number]; // [lon, lat]
  /** 1 도당 픽셀 수 (위도 기준). 커지면 확대. */
  scale: number;
  /** 마지막으로 fit 된 bbox. 새 쿼리 시 갱신. */
  initialBBox?: [number, number, number, number];
}

interface TimelineState {
  query: TimelineQuery | null;
  /** 체크된 버전 키 배열 (정렬). */
  selectedVersions: string[];
  viewport: TimelineViewport | null;
  /** 기준 연도에서의 조회 대상 geometry 들 (선택 수만큼). 빨간 실선 오버레이용. */
  baseGeometries: DecodedGeometry[];

  setQuery: (q: TimelineQuery | null) => void;
  setSelectedVersions: (v: string[]) => void;
  toggleVersion: (v: string) => void;
  setViewport: (vp: TimelineViewport | null) => void;
  updateViewport: (patch: Partial<TimelineViewport>) => void;
  setBaseGeometries: (gs: DecodedGeometry[]) => void;
  reset: () => void;
}

export const useTimelineStore = create<TimelineState>((set) => ({
  query: null,
  selectedVersions: [],
  viewport: null,
  baseGeometries: [],

  setQuery: (q) => set({ query: q }),
  setBaseGeometries: (gs) => set({ baseGeometries: gs }),
  setSelectedVersions: (v) => set({ selectedVersions: [...v].sort() }),
  toggleVersion: (v) =>
    set((s) => {
      const has = s.selectedVersions.includes(v);
      const next = has
        ? s.selectedVersions.filter((x) => x !== v)
        : [...s.selectedVersions, v];
      next.sort();
      return { selectedVersions: next };
    }),
  setViewport: (vp) => set({ viewport: vp }),
  updateViewport: (patch) =>
    set((s) =>
      s.viewport ? { viewport: { ...s.viewport, ...patch } } : s,
    ),
  reset: () =>
    set({
      query: null,
      selectedVersions: [],
      viewport: null,
      baseGeometries: [],
    }),
}));
