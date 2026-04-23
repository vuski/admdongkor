"use client";

import { PanelLeftClose } from "lucide-react";
import { useAppStore } from "@/stores/app-store";
import { VersionSlider } from "@/components/controls/version-slider";
import { LevelToggle } from "@/components/controls/level-toggle";
import { DetailToggle } from "@/components/controls/detail-toggle";
import { BasemapToggle } from "@/components/controls/basemap-toggle";
import { LabelToggle } from "@/components/controls/label-toggle";

const WIDTH = 320;

export function SidebarPanel() {
  const isOpen = useAppStore((s) => s.isSidebarOpen);
  const toggle = useAppStore((s) => s.toggleSidebar);

  return (
    <div
      className="relative h-full border-r border-border bg-background transition-[width] duration-300 overflow-hidden shrink-0"
      style={{ width: isOpen ? `${WIDTH}px` : "0px" }}
    >
      <div className="h-full flex flex-col" style={{ width: WIDTH }}>
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <div>
            <h1 className="text-sm font-bold tracking-tight">admdongkor</h1>
            <p className="text-[11px] text-muted-foreground">
              대한민국 행정동 경계 · 1975–
            </p>
          </div>
          <button
            onClick={toggle}
            className="p-1.5 rounded-md hover:bg-muted"
            aria-label="사이드바 닫기"
          >
            <PanelLeftClose className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-4 space-y-5">
          <VersionSlider />
          <LevelToggle />
          <DetailToggle />
          <BasemapToggle />
          <LabelToggle />
        </div>

        <div className="px-4 py-3 border-t border-border text-[11px] text-muted-foreground space-y-1">
          <div>
            Data:{" "}
            <a
              href="https://github.com/vuski/admdongkor"
              target="_blank"
              rel="noreferrer"
              className="underline hover:text-accent"
            >
              github.com/vuski/admdongkor
            </a>
          </div>
          <div>© VWL Inc.</div>
        </div>
      </div>
    </div>
  );
}
