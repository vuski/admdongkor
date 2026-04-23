"use client";

import { Loader2, AlertCircle } from "lucide-react";

interface Props {
  position: "top-left" | "top-right";
  label: string;
  color: string;
  loading?: boolean;
  error?: Error | null;
}

function formatKey(k: string) {
  return `${k.slice(0, 4)}-${k.slice(4, 6)}-${k.slice(6, 8)}`;
}

export function VersionBadge({ position, label, color, loading, error }: Props) {
  const pos = position === "top-left" ? "top-3 left-3" : "top-3 right-3";
  return (
    <div
      className={`absolute ${pos} z-10 px-2.5 py-1.5 rounded-md bg-white/95 shadow-md border border-black/10 text-[11px] font-mono flex items-center gap-1.5`}
    >
      <span
        className="inline-block h-2.5 w-2.5 rounded-sm"
        style={{ background: color }}
      />
      <span className="text-black">{formatKey(label)}</span>
      {loading && <Loader2 className="h-3 w-3 animate-spin text-[var(--muted-foreground)]" />}
      {error && (
        <span title={error.message} className="text-red-600">
          <AlertCircle className="h-3 w-3" />
        </span>
      )}
    </div>
  );
}
