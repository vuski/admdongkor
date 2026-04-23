"use client";

import type { Level } from "admdongkor";
import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

const LEVELS: { value: Level; label: string; hint: string }[] = [
  { value: "sido", label: "시도", hint: "~0.5MB" },
  { value: "sgg", label: "시군구", hint: "~1MB" },
  { value: "emd", label: "읍면동", hint: "~2.4MB" },
];

export function LevelToggle() {
  const level = useAppStore((s) => s.level);
  const setLevel = useAppStore((s) => s.setLevel);

  return (
    <div>
      <label className="text-xs font-medium block mb-1.5">레벨</label>
      <div className="grid grid-cols-3 gap-1 p-0.5 rounded-md bg-[var(--muted)] border border-[var(--border)]">
        {LEVELS.map((l) => (
          <button
            key={l.value}
            onClick={() => setLevel(l.value)}
            className={cn(
              "px-2 py-1.5 rounded text-xs transition",
              level === l.value
                ? "bg-[var(--background)] shadow-sm font-semibold"
                : "text-[var(--muted-foreground)] hover:text-[var(--foreground)]",
            )}
          >
            <div>{l.label}</div>
            <div className="text-[10px] opacity-60">{l.hint}</div>
          </button>
        ))}
      </div>
    </div>
  );
}
