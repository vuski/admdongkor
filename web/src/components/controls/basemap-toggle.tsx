"use client";

import { Map as MapIcon, EyeOff } from "lucide-react";
import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

export function BasemapToggle() {
  const showBasemap = useAppStore((s) => s.showBasemap);
  const toggle = useAppStore((s) => s.toggleBasemap);

  return (
    <div>
      <label className="text-xs font-medium block mb-1.5">배경지도</label>
      <button
        onClick={toggle}
        className={cn(
          "w-full px-3 py-2 rounded-md text-xs border transition flex items-center justify-center gap-2",
          showBasemap
            ? "bg-muted border-border hover:border-accent"
            : "bg-accent text-accent-foreground border-accent",
        )}
      >
        {showBasemap ? (
          <>
            <MapIcon className="h-3.5 w-3.5" /> 켜짐 (Positron)
          </>
        ) : (
          <>
            <EyeOff className="h-3.5 w-3.5" /> 꺼짐 (경계만)
          </>
        )}
      </button>
    </div>
  );
}
