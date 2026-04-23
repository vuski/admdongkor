"use client";

import { Loader2, AlertCircle } from "lucide-react";

export interface VersionBadgeProps {
  label: string;
  color: string;
  loading?: boolean;
  error?: Error | null;
}

function formatKey(k: string) {
  return `${k.slice(0, 4)}-${k.slice(4, 6)}-${k.slice(6, 8)}`;
}

export function VersionBadge({ label, color, loading, error }: VersionBadgeProps) {
  return (
    <div className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-md bg-white/95 shadow-md border border-black/10 text-[11px] font-mono">
      <span
        className="inline-block h-2.5 w-2.5 rounded-sm"
        style={{ background: color }}
      />
      <span className="text-black">{formatKey(label)}</span>
      {loading && (
        <Loader2 className="h-3 w-3 animate-spin text-neutral-500" />
      )}
      {error && (
        <span title={error.message} className="text-red-600">
          <AlertCircle className="h-3 w-3" />
        </span>
      )}
    </div>
  );
}

/** A/B 비교 badge 를 화면 세로 중앙, 각각 좌/우 끝에 세로로 배치. */
export function VersionBadgeA({ children }: { children: React.ReactNode }) {
  return (
    <div className="absolute left-3 top-1/2 -translate-y-1/2 z-10 pointer-events-none">
      {children}
    </div>
  );
}

export function VersionBadgeB({ children }: { children: React.ReactNode }) {
  return (
    <div className="absolute right-3 top-1/2 -translate-y-1/2 z-10 pointer-events-none">
      {children}
    </div>
  );
}
