"use client";

import { create } from "zustand";
import type { Level } from "admdongkor";
import { VERSIONS } from "admdongkor";

export interface FlyToRequest {
  /** GeoJSON bbox [minLon, minLat, maxLon, maxLat]. */
  bbox?: [number, number, number, number];
  center?: [number, number];
  zoom?: number;
  /** 매번 새 요청임을 구분하기 위한 카운터. */
  seq: number;
  /** pending: 지도 데이터 로딩 완료 대기 중. done: 실행 완료. */
  status: "pending" | "done";
  /** 클릭한 feature 를 찾기 위한 정보 (pending 상태에서 bbox 직접 계산용). */
  codeField?: string | null;
  codeValue?: string | null;
  level?: string;
  /** 코드가 없을 때 이름으로 fallback 매칭. */
  nameFields?: Record<string, string>;
}

interface AppState {
  versionKey: string;
  versionKeyB: string;
  level: Level;
  flyToRequest: FlyToRequest | null;
  compareMode: boolean;
  /** 0–1, 지도 컨테이너 width 기준. 0.5 = 가운데. */
  compareSplit: number;
  /** split divider 드래그 중 여부. 드래그 중에는 hover pick 을 중단해 깜빡임/비용 방지. */
  compareDragging: boolean;
  showBasemap: boolean;
  showLabels: boolean;
  isSidebarOpen: boolean;
  isRightPanelOpen: boolean;

  setVersionKey: (v: string) => void;
  setVersionKeyB: (v: string) => void;
  setLevel: (l: Level) => void;
  requestFlyTo: (req: Omit<FlyToRequest, "seq" | "status">) => void;
  clearFlyTo: () => void;
  setCompareMode: (v: boolean) => void;
  toggleCompareMode: () => void;
  setCompareSplit: (v: number) => void;
  setCompareDragging: (v: boolean) => void;
  setShowBasemap: (v: boolean) => void;
  toggleBasemap: () => void;
  setShowLabels: (v: boolean) => void;
  toggleLabels: () => void;
  toggleSidebar: () => void;
  toggleRightPanel: () => void;
  setSidebarOpen: (v: boolean) => void;
  setRightPanelOpen: (v: boolean) => void;
}

const DEFAULT_VERSION = VERSIONS[VERSIONS.length - 1];
const EARLIER_VERSION = VERSIONS[Math.max(0, VERSIONS.length - 20)];

export const useAppStore = create<AppState>((set) => ({
  versionKey: DEFAULT_VERSION,
  versionKeyB: EARLIER_VERSION,
  level: "sido",
  flyToRequest: null,
  compareMode: false,
  compareSplit: 0.5,
  compareDragging: false,
  showBasemap: true,
  showLabels: true,
  isSidebarOpen: true,
  isRightPanelOpen: true,

  setVersionKey: (v) => set({ versionKey: v }),
  setVersionKeyB: (v) => set({ versionKeyB: v }),
  setLevel: (l) => set({ level: l }),
  requestFlyTo: (req) =>
    set((s) => ({
      flyToRequest: { ...req, seq: (s.flyToRequest?.seq ?? 0) + 1, status: "pending" },
    })),
  clearFlyTo: () => set({ flyToRequest: null }),
  // 비교 모드 켜질 때 레이블을 자동 OFF (시각적 잡음 제거).
  // 사용자가 다시 켜는 건 자유 — 한 번 더 덮어쓰지 않는다.
  setCompareMode: (v) =>
    set((s) => (v && !s.compareMode ? { compareMode: v, showLabels: false } : { compareMode: v })),
  toggleCompareMode: () =>
    set((s) =>
      !s.compareMode ? { compareMode: true, showLabels: false } : { compareMode: false },
    ),
  setCompareSplit: (v) => set({ compareSplit: Math.max(0, Math.min(1, v)) }),
  setCompareDragging: (v) => set({ compareDragging: v }),
  setShowBasemap: (v) => set({ showBasemap: v }),
  toggleBasemap: () => set((s) => ({ showBasemap: !s.showBasemap })),
  setShowLabels: (v) => set({ showLabels: v }),
  toggleLabels: () => set((s) => ({ showLabels: !s.showLabels })),
  toggleSidebar: () => set((s) => ({ isSidebarOpen: !s.isSidebarOpen })),
  toggleRightPanel: () => set((s) => ({ isRightPanelOpen: !s.isRightPanelOpen })),
  setSidebarOpen: (v) => set({ isSidebarOpen: v }),
  setRightPanelOpen: (v) => set({ isRightPanelOpen: v }),
}));
