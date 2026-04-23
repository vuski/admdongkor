"use client";

import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

export function DetailToggle() {
  const compareMode = useAppStore((s) => s.compareMode);
  const toggleCompareMode = useAppStore((s) => s.toggleCompareMode);

  return (
    <div>
      <label className="text-xs font-medium block mb-1.5">비교 모드</label>
      <button
        onClick={toggleCompareMode}
        className={cn(
          "w-full px-3 py-2 rounded-md text-xs border transition",
          compareMode
            ? "bg-accent text-accent-foreground border-accent"
            : "bg-muted border-border hover:border-accent",
        )}
      >
        {compareMode ? "비교 모드 ON (분할선 드래그)" : "비교 모드 켜기"}
      </button>
      <p className="text-[10px] text-muted-foreground mt-1 leading-relaxed">
        분할선을 좌우로 드래그해 두 시점 경계를 나란히 비교.
      </p>
    </div>
  );
}
