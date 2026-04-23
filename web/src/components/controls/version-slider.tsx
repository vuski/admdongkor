"use client";

import { VERSIONS } from "admdongkor";
import { useAppStore } from "@/stores/app-store";

function formatKey(key: string): string {
  return `${key.slice(0, 4)}-${key.slice(4, 6)}-${key.slice(6, 8)}`;
}

export function VersionSlider() {
  const versionKey = useAppStore((s) => s.versionKey);
  const setVersionKey = useAppStore((s) => s.setVersionKey);
  const versionKeyB = useAppStore((s) => s.versionKeyB);
  const setVersionKeyB = useAppStore((s) => s.setVersionKeyB);
  const compareMode = useAppStore((s) => s.compareMode);

  const list = VERSIONS as readonly string[];
  const idxA = list.indexOf(versionKey);
  const idxB = list.indexOf(versionKeyB);

  return (
    <div className="space-y-3">
      <SliderRow
        label={compareMode ? "A (좌)" : "시점"}
        color="var(--accent)"
        value={idxA}
        max={VERSIONS.length - 1}
        display={formatKey(versionKey)}
        onChange={(i) => setVersionKey(list[i] ?? versionKey)}
      />
      {compareMode && (
        <SliderRow
          label="B (우)"
          color="#f97316"
          value={idxB}
          max={VERSIONS.length - 1}
          display={formatKey(versionKeyB)}
          onChange={(i) => setVersionKeyB(list[i] ?? versionKeyB)}
        />
      )}
    </div>
  );
}

function SliderRow({
  label,
  color,
  value,
  max,
  display,
  onChange,
}: {
  label: string;
  color: string;
  value: number;
  max: number;
  display: string;
  onChange: (i: number) => void;
}) {
  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <div className="flex items-center gap-2">
          <span
            className="inline-block h-2 w-2 rounded-sm"
            style={{ background: color }}
          />
          <label className="text-xs font-medium">{label}</label>
        </div>
        <span className="text-[11px] font-mono text-[var(--muted-foreground)]">
          {display}
        </span>
      </div>
      <input
        type="range"
        min={0}
        max={max}
        step={1}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-[var(--accent)]"
      />
    </div>
  );
}
