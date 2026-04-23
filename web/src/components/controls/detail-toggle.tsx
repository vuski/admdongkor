"use client";

import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

export function DetailToggle() {
  const detail = useAppStore((s) => s.detail);
  const setDetail = useAppStore((s) => s.setDetail);
  const compareMode = useAppStore((s) => s.compareMode);
  const toggleCompareMode = useAppStore((s) => s.toggleCompareMode);

  return (
    <div className="space-y-3">
      <div>
        <label className="text-xs font-medium block mb-1.5">해상도</label>
        <div className="grid grid-cols-2 gap-1 p-0.5 rounded-md bg-[var(--muted)] border border-[var(--border)]">
          <button
            onClick={() => setDetail(false)}
            className={cn(
              "px-2 py-1.5 rounded text-xs transition",
              !detail
                ? "bg-[var(--background)] shadow-sm font-semibold"
                : "text-[var(--muted-foreground)] hover:text-[var(--foreground)]",
            )}
          >
            light
          </button>
          <button
            onClick={() => setDetail(true)}
            className={cn(
              "px-2 py-1.5 rounded text-xs transition",
              detail
                ? "bg-[var(--background)] shadow-sm font-semibold"
                : "text-[var(--muted-foreground)] hover:text-[var(--foreground)]",
            )}
          >
            detail
          </button>
        </div>
        <p className="text-[10px] text-[var(--muted-foreground)] mt-1">
          detail = 원본 해상도 (emd ~11MB). 느릴 수 있음.
        </p>
      </div>

      <div>
        <label className="text-xs font-medium block mb-1.5">비교 모드</label>
        <button
          onClick={toggleCompareMode}
          className={cn(
            "w-full px-3 py-2 rounded-md text-xs border transition",
            compareMode
              ? "bg-[var(--accent)] text-white border-[var(--accent)]"
              : "bg-[var(--muted)] border-[var(--border)] hover:border-[var(--accent)]",
          )}
        >
          {compareMode ? "비교 모드 ON (분할선 드래그)" : "비교 모드 켜기"}
        </button>
      </div>
    </div>
  );
}
