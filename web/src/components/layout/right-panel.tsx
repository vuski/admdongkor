"use client";

import { PanelRightClose } from "lucide-react";
import { useAppStore } from "@/stores/app-store";
import { SearchPanel } from "@/components/search/search-panel";

const WIDTH = 360;

export function RightPanel() {
  const isOpen = useAppStore((s) => s.isRightPanelOpen);
  const toggle = useAppStore((s) => s.toggleRightPanel);

  return (
    <div
      className="relative h-full border-l border-border bg-background transition-[width] duration-300 overflow-hidden shrink-0"
      style={{ width: isOpen ? `${WIDTH}px` : "0px" }}
    >
      <div className="h-full flex flex-col" style={{ width: WIDTH }}>
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <h2 className="text-sm font-semibold">검색 · 조회</h2>
          <button
            onClick={toggle}
            className="p-1.5 rounded-md hover:bg-muted"
            aria-label="우측 패널 닫기"
          >
            <PanelRightClose className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-4">
          <SearchPanel />
        </div>
      </div>
    </div>
  );
}
