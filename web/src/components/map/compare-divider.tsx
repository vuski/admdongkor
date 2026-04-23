"use client";

import { useCallback, useEffect, useRef } from "react";
import { useAppStore } from "@/stores/app-store";
import { ChevronLeft, ChevronRight } from "lucide-react";

export function CompareDivider() {
  const split = useAppStore((s) => s.compareSplit);
  const setSplit = useAppStore((s) => s.setCompareSplit);
  const draggingRef = useRef(false);
  const hostRef = useRef<HTMLDivElement>(null);

  const updateFromClientX = useCallback(
    (clientX: number) => {
      const host = hostRef.current?.parentElement;
      if (!host) return;
      const rect = host.getBoundingClientRect();
      const ratio = (clientX - rect.left) / rect.width;
      setSplit(ratio);
    },
    [setSplit],
  );

  useEffect(() => {
    const onMove = (e: PointerEvent) => {
      if (!draggingRef.current) return;
      e.preventDefault();
      updateFromClientX(e.clientX);
    };
    const onUp = () => {
      draggingRef.current = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    window.addEventListener("pointercancel", onUp);
    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("pointercancel", onUp);
    };
  }, [updateFromClientX]);

  return (
    <div
      ref={hostRef}
      className="absolute top-0 bottom-0 z-20 pointer-events-none"
      style={{ left: `calc(${split * 100}% - 1px)` }}
    >
      <div className="absolute top-0 bottom-0 w-[2px] bg-white shadow-[0_0_8px_rgba(0,0,0,0.4)]" />
      <button
        onPointerDown={(e) => {
          e.preventDefault();
          draggingRef.current = true;
          document.body.style.cursor = "ew-resize";
          document.body.style.userSelect = "none";
        }}
        className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 pointer-events-auto h-9 w-9 rounded-full bg-white shadow-lg border border-black/10 flex items-center justify-center cursor-ew-resize hover:scale-105 transition"
        aria-label="비교 분할선 드래그"
      >
        <ChevronLeft className="h-3.5 w-3.5 text-black -mr-1" />
        <ChevronRight className="h-3.5 w-3.5 text-black -ml-1" />
      </button>
    </div>
  );
}
