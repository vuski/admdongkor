"use client";

import dynamic from "next/dynamic";
import { SidebarPanel } from "./sidebar-panel";
import { RightPanel } from "./right-panel";
import { useAppStore } from "@/stores/app-store";
import { PanelLeftOpen, PanelRightOpen } from "lucide-react";

const MapContainer = dynamic(
  () => import("@/components/map/map-container").then((m) => ({ default: m.MapContainer })),
  { ssr: false },
);

export function AppShell() {
  const isSidebarOpen = useAppStore((s) => s.isSidebarOpen);
  const isRightPanelOpen = useAppStore((s) => s.isRightPanelOpen);
  const setSidebarOpen = useAppStore((s) => s.setSidebarOpen);
  const setRightPanelOpen = useAppStore((s) => s.setRightPanelOpen);

  return (
    <div className="flex h-screen w-screen overflow-hidden">
      <SidebarPanel />
      <div className="relative flex-1 h-full">
        {!isSidebarOpen && (
          <button
            onClick={() => setSidebarOpen(true)}
            className="absolute top-3 left-3 z-10 p-2 rounded-md bg-[var(--background)] border border-[var(--border)] shadow hover:bg-[var(--muted)]"
            aria-label="사이드바 열기"
          >
            <PanelLeftOpen className="h-4 w-4" />
          </button>
        )}
        <MapContainer />
        {!isRightPanelOpen && (
          <button
            onClick={() => setRightPanelOpen(true)}
            className="absolute top-3 right-3 z-10 p-2 rounded-md bg-[var(--background)] border border-[var(--border)] shadow hover:bg-[var(--muted)]"
            aria-label="우측 패널 열기"
          >
            <PanelRightOpen className="h-4 w-4" />
          </button>
        )}
      </div>
      <RightPanel />
    </div>
  );
}
