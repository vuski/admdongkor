"use client";

import { Type, TypeOutline } from "lucide-react";
import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

export function LabelToggle() {
  const showLabels = useAppStore((s) => s.showLabels);
  const toggle = useAppStore((s) => s.toggleLabels);

  return (
    <div>
      <label className="text-xs font-medium block mb-1.5">지명 레이블</label>
      <button
        onClick={toggle}
        className={cn(
          "w-full px-3 py-2 rounded-md text-xs border transition flex items-center justify-center gap-2",
          showLabels
            ? "bg-accent text-accent-foreground border-accent"
            : "bg-muted border-border hover:border-accent",
        )}
      >
        {showLabels ? (
          <>
            <Type className="h-3.5 w-3.5" /> 켜짐
          </>
        ) : (
          <>
            <TypeOutline className="h-3.5 w-3.5" /> 꺼짐
          </>
        )}
      </button>
      <p className="text-[10px] text-muted-foreground mt-1 leading-relaxed">
        시도/시군구/읍면동 레이블 표시. 줌 레벨에 따라 세부 단계로 전환.
      </p>
    </div>
  );
}
