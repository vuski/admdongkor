"use client";

import { create } from "zustand";
import type { Level } from "admdongkor";
import { VERSIONS } from "admdongkor";

interface AppState {
  versionKey: string;
  versionKeyB: string;
  level: Level;
  detail: boolean;
  compareMode: boolean;
  /** 0–1, 지도 컨테이너 width 기준. 0.5 = 가운데. */
  compareSplit: number;
  isSidebarOpen: boolean;
  isRightPanelOpen: boolean;

  setVersionKey: (v: string) => void;
  setVersionKeyB: (v: string) => void;
  setLevel: (l: Level) => void;
  setDetail: (d: boolean) => void;
  setCompareMode: (v: boolean) => void;
  toggleCompareMode: () => void;
  setCompareSplit: (v: number) => void;
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
  detail: false,
  compareMode: false,
  compareSplit: 0.5,
  isSidebarOpen: true,
  isRightPanelOpen: true,

  setVersionKey: (v) => set({ versionKey: v }),
  setVersionKeyB: (v) => set({ versionKeyB: v }),
  setLevel: (l) => set({ level: l }),
  setDetail: (d) => set({ detail: d }),
  setCompareMode: (v) => set({ compareMode: v }),
  toggleCompareMode: () => set((s) => ({ compareMode: !s.compareMode })),
  setCompareSplit: (v) => set({ compareSplit: Math.max(0, Math.min(1, v)) }),
  toggleSidebar: () => set((s) => ({ isSidebarOpen: !s.isSidebarOpen })),
  toggleRightPanel: () => set((s) => ({ isRightPanelOpen: !s.isRightPanelOpen })),
  setSidebarOpen: (v) => set({ isSidebarOpen: v }),
  setRightPanelOpen: (v) => set({ isRightPanelOpen: v }),
}));
