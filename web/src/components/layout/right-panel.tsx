"use client";

import { PanelRightClose } from "lucide-react";
import { useAppStore } from "@/stores/app-store";
import { SearchPanel } from "@/components/search/search-panel";
import { TimelinePanel } from "@/components/search/timeline-panel";
import { cn } from "@/lib/cn";

const WIDTH_SEARCH = 360;
const WIDTH_TIMELINE = 280;

export function RightPanel() {
  const isOpen = useAppStore((s) => s.isRightPanelOpen);
  const toggle = useAppStore((s) => s.toggleRightPanel);
  const tab = useAppStore((s) => s.rightPanelTab);
  const setTab = useAppStore((s) => s.setRightPanelTab);
  const setTimelineViewActive = useAppStore((s) => s.setTimelineViewActive);
  const width = tab === "timeline" ? WIDTH_TIMELINE : WIDTH_SEARCH;

  return (
    <div
      className="relative h-full border-l border-border bg-background transition-[width] duration-300 overflow-hidden shrink-0"
      style={{ width: isOpen ? `${width}px` : "0px" }}
    >
      <div className="h-full flex flex-col" style={{ width }}>
        <div className="flex items-center justify-between px-2 py-2 border-b border-border gap-1">
          <div className="flex gap-0.5">
            <TabButton
              active={tab === "search"}
              onClick={() => {
                setTab("search");
                // 검색 탭으로 돌아오면 타임라인 뷰 해제 — 지도로 복귀
                setTimelineViewActive(false);
              }}
            >
              검색·조회
            </TabButton>
            <TabButton active={tab === "timeline"} onClick={() => setTab("timeline")}>
              시계열추적
            </TabButton>
          </div>
          <button
            onClick={toggle}
            className="p-1.5 rounded-md hover:bg-muted"
            aria-label="우측 패널 닫기"
          >
            <PanelRightClose className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-4">
          {tab === "search" ? <SearchPanel /> : <TimelinePanel />}
        </div>
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "text-xs px-2.5 py-1.5 rounded-md transition",
        active
          ? "bg-accent text-accent-foreground font-semibold"
          : "text-muted-foreground hover:bg-muted",
      )}
    >
      {children}
    </button>
  );
}
