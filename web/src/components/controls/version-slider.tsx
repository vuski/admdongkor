"use client";

import { useEffect, useState } from "react";
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

  return (
    <div className="space-y-3">
      <SliderRow
        label={compareMode ? "A (좌)" : "시점"}
        color="var(--side-a)"
        committed={versionKey}
        list={list}
        onCommit={(v) => setVersionKey(v)}
      />
      {compareMode && (
        <SliderRow
          label="B (우)"
          color="var(--side-b)"
          committed={versionKeyB}
          list={list}
          onCommit={(v) => setVersionKeyB(v)}
        />
      )}
    </div>
  );
}

/** 드래그 중에는 로컬 state 만 갱신, pointer up / blur / keyup 때만 commit.
 *  지도 fetch 가 드래그 중 트리거되는 걸 막는다. */
function SliderRow({
  label,
  color,
  committed,
  list,
  onCommit,
}: {
  label: string;
  color: string;
  committed: string;
  list: readonly string[];
  onCommit: (v: string) => void;
}) {
  const committedIdx = list.indexOf(committed);
  const [localIdx, setLocalIdx] = useState(committedIdx);

  // 외부에서 committed 가 바뀌면 (예: 검색 결과 클릭) 로컬도 맞춤.
  useEffect(() => {
    setLocalIdx(committedIdx);
  }, [committedIdx]);

  const max = list.length - 1;
  const display = list[localIdx] ?? committed;
  const isDragging = localIdx !== committedIdx;

  const commit = () => {
    if (localIdx === committedIdx) return;
    const v = list[localIdx];
    if (v) onCommit(v);
  };

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
        <span
          className="text-[11px] font-mono"
          style={{
            color: isDragging ? color : undefined,
            fontWeight: isDragging ? 600 : undefined,
          }}
        >
          {formatKey(display)}
          {isDragging && " …"}
        </span>
      </div>
      <input
        type="range"
        min={0}
        max={max}
        step={1}
        value={localIdx}
        onChange={(e) => setLocalIdx(Number(e.target.value))}
        onPointerUp={commit}
        onKeyUp={commit}
        onBlur={commit}
        style={{ accentColor: color }}
        className="w-full"
      />
    </div>
  );
}
