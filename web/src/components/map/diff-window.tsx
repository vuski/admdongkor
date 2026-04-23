"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AgGridReact } from "ag-grid-react";
import {
  AllCommunityModule,
  ModuleRegistry,
  themeQuartz,
  type ColDef,
} from "ag-grid-community";
import type { CompareResult, CompareRow } from "admdongkor";
import { X, GripHorizontal } from "lucide-react";
import { cn } from "@/lib/cn";

ModuleRegistry.registerModules([AllCommunityModule]);

export interface DiffWindowProps {
  result: CompareResult;
  onClose: () => void;
}

type FilterMode = "all" | "changed" | "abolished" | "created";

interface Row {
  status: "경계변경" | "폐지" | "신설";
  emdcd: string;
  emdnm: string;
  sidonm: string;
  sggnm: string;
  versionKey: string;
  iou: number | null;
}

const MIN_W = 360;
const MIN_H = 240;
const DEFAULT_W = 560;
const DEFAULT_H = 360;
const MARGIN = 8;

function clampPos(
  x: number,
  y: number,
  w: number,
  h: number,
): { x: number; y: number } {
  if (typeof window === "undefined") return { x, y };
  const maxX = Math.max(MARGIN, window.innerWidth - w - MARGIN);
  const maxY = Math.max(MARGIN, window.innerHeight - h - MARGIN);
  return {
    x: Math.min(Math.max(MARGIN, x), maxX),
    y: Math.min(Math.max(MARGIN, y), maxY),
  };
}

function clampSize(w: number, h: number): { w: number; h: number } {
  if (typeof window === "undefined") return { w, h };
  const maxW = Math.max(MIN_W, window.innerWidth - MARGIN * 2);
  const maxH = Math.max(MIN_H, window.innerHeight - MARGIN * 2);
  return {
    w: Math.min(Math.max(MIN_W, w), maxW),
    h: Math.min(Math.max(MIN_H, h), maxH),
  };
}

export function DiffWindow({ result, onClose }: DiffWindowProps) {
  const [filter, setFilter] = useState<FilterMode>("all");
  const [pos, setPos] = useState<{ x: number; y: number }>({ x: 100, y: 120 });
  const [size, setSize] = useState<{ w: number; h: number }>({
    w: DEFAULT_W,
    h: DEFAULT_H,
  });
  const [mounted, setMounted] = useState(false);

  // 초기 위치: 우측 상단 쪽
  useEffect(() => {
    const initialX = Math.max(
      MARGIN,
      window.innerWidth - DEFAULT_W - 380,
    ); // right-panel 폭 약 360 고려
    const initialY = 100;
    setPos(clampPos(initialX, initialY, DEFAULT_W, DEFAULT_H));
    setMounted(true);
  }, []);

  // 창 크기 바뀌면 다시 clamp
  useEffect(() => {
    const onResize = () => {
      setSize((s) => clampSize(s.w, s.h));
      setPos((p) => {
        const { w, h } = clampSize(size.w, size.h);
        return clampPos(p.x, p.y, w, h);
      });
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, [size.w, size.h]);

  // --- 드래그 이동 ---
  const dragOffset = useRef<{ dx: number; dy: number } | null>(null);
  const onHeaderPointerDown = useCallback(
    (e: React.PointerEvent) => {
      (e.target as HTMLElement).setPointerCapture(e.pointerId);
      dragOffset.current = { dx: e.clientX - pos.x, dy: e.clientY - pos.y };
    },
    [pos.x, pos.y],
  );
  const onHeaderPointerMove = useCallback(
    (e: React.PointerEvent) => {
      if (!dragOffset.current) return;
      const next = clampPos(
        e.clientX - dragOffset.current.dx,
        e.clientY - dragOffset.current.dy,
        size.w,
        size.h,
      );
      setPos(next);
    },
    [size.w, size.h],
  );
  const onHeaderPointerUp = useCallback((e: React.PointerEvent) => {
    (e.target as HTMLElement).releasePointerCapture(e.pointerId);
    dragOffset.current = null;
  }, []);

  // --- 리사이즈 (오른쪽 아래 corner) ---
  const resizeStart = useRef<{
    w: number;
    h: number;
    clientX: number;
    clientY: number;
  } | null>(null);
  const onResizePointerDown = useCallback(
    (e: React.PointerEvent) => {
      e.stopPropagation();
      (e.target as HTMLElement).setPointerCapture(e.pointerId);
      resizeStart.current = {
        w: size.w,
        h: size.h,
        clientX: e.clientX,
        clientY: e.clientY,
      };
    },
    [size.w, size.h],
  );
  const onResizePointerMove = useCallback(
    (e: React.PointerEvent) => {
      if (!resizeStart.current) return;
      const dx = e.clientX - resizeStart.current.clientX;
      const dy = e.clientY - resizeStart.current.clientY;
      const next = clampSize(resizeStart.current.w + dx, resizeStart.current.h + dy);
      setSize(next);
      // 리사이즈 중 위치도 뷰포트 밖으로 나가지 않게 유지
      setPos((p) => clampPos(p.x, p.y, next.w, next.h));
    },
    [],
  );
  const onResizePointerUp = useCallback((e: React.PointerEvent) => {
    (e.target as HTMLElement).releasePointerCapture(e.pointerId);
    resizeStart.current = null;
  }, []);

  // --- 데이터: compare 결과 → 표 rows ---
  const allRows: Row[] = useMemo(() => {
    const aIsPast = result.va < result.vb;
    const abolishedStatus = aIsPast ? "only_in_a" : "only_in_b";
    const createdStatus = aIsPast ? "only_in_b" : "only_in_a";

    // changed 는 emdcd 당 두 row (양쪽 시점) — 과거 시점 것만 보여주자
    const pastVersion = aIsPast ? result.va : result.vb;

    const rows: Row[] = [];
    for (const r of result.diff) {
      if (r.status === "changed") {
        if (r.version_key !== pastVersion) continue; // 중복 제거
        rows.push(projectRow(r, "경계변경"));
      } else if (r.status === abolishedStatus) {
        rows.push(projectRow(r, "폐지"));
      } else if (r.status === createdStatus) {
        rows.push(projectRow(r, "신설"));
      }
    }
    return rows;
  }, [result]);

  const filteredRows = useMemo(() => {
    if (filter === "all") return allRows;
    const keyMap: Record<FilterMode, Row["status"] | null> = {
      all: null,
      changed: "경계변경",
      abolished: "폐지",
      created: "신설",
    };
    const k = keyMap[filter];
    return k ? allRows.filter((r) => r.status === k) : allRows;
  }, [allRows, filter]);

  const columnDefs = useMemo<ColDef<Row>[]>(
    () => [
      {
        field: "status",
        headerName: "구분",
        width: 100,
        cellStyle: (p) => {
          const s = p.value as Row["status"];
          if (s === "경계변경") return { color: "#a16207", fontWeight: 600 };
          if (s === "폐지") return { color: "#b91c1c", fontWeight: 600 };
          return { color: "#15803d", fontWeight: 600 };
        },
      },
      { field: "sidonm", headerName: "시도", flex: 1, minWidth: 110 },
      { field: "sggnm", headerName: "시군구", flex: 1, minWidth: 110 },
      { field: "emdnm", headerName: "읍면동", flex: 1, minWidth: 110 },
      { field: "emdcd", headerName: "코드", width: 110 },
      {
        field: "iou",
        headerName: "IoU",
        width: 90,
        valueFormatter: (p) =>
          typeof p.value === "number" ? p.value.toFixed(3) : "—",
      },
    ],
    [],
  );

  const counts = useMemo(() => {
    const c = { changed: 0, abolished: 0, created: 0 };
    for (const r of allRows) {
      if (r.status === "경계변경") c.changed++;
      else if (r.status === "폐지") c.abolished++;
      else c.created++;
    }
    return c;
  }, [allRows]);

  // 모든 hook 호출 이후에 조기 리턴 (Rules of Hooks)
  if (!mounted) return null;

  return (
    <div
      className="fixed z-40 bg-white rounded-lg shadow-2xl border border-black/10 flex flex-col overflow-hidden"
      style={{
        left: pos.x,
        top: pos.y,
        width: size.w,
        height: size.h,
      }}
    >
      {/* 헤더: 드래그 */}
      <div
        className="flex items-center justify-between px-3 py-2 bg-neutral-50 border-b border-black/10 cursor-move select-none"
        onPointerDown={onHeaderPointerDown}
        onPointerMove={onHeaderPointerMove}
        onPointerUp={onHeaderPointerUp}
      >
        <div className="flex items-center gap-2 text-xs font-semibold text-neutral-800">
          <GripHorizontal className="h-3.5 w-3.5 text-neutral-400" />
          변경이력 — {fmtKey(result.va)} vs {fmtKey(result.vb)}
        </div>
        <button
          onClick={onClose}
          className="p-1 rounded hover:bg-neutral-200"
          aria-label="닫기"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {/* 필터 라디오 */}
      <div className="flex items-center gap-1 px-3 py-2 border-b border-black/10 text-xs">
        <FilterBtn
          active={filter === "all"}
          onClick={() => setFilter("all")}
          label={`전체 ${allRows.length}`}
          color="#525252"
        />
        <FilterBtn
          active={filter === "changed"}
          onClick={() => setFilter("changed")}
          label={`경계변경 ${counts.changed}`}
          color="#eab308"
        />
        <FilterBtn
          active={filter === "abolished"}
          onClick={() => setFilter("abolished")}
          label={`폐지 ${counts.abolished}`}
          color="#dc2626"
        />
        <FilterBtn
          active={filter === "created"}
          onClick={() => setFilter("created")}
          label={`신설 ${counts.created}`}
          color="#22c55e"
        />
      </div>

      {/* 그리드 */}
      <div className="flex-1 min-h-0">
        <AgGridReact<Row>
          theme={themeQuartz}
          rowData={filteredRows}
          columnDefs={columnDefs}
          defaultColDef={{
            sortable: true,
            filter: true,
            resizable: true,
          }}
          rowHeight={28}
          headerHeight={32}
        />
      </div>

      {/* 리사이즈 handle (오른쪽 아래) */}
      <div
        onPointerDown={onResizePointerDown}
        onPointerMove={onResizePointerMove}
        onPointerUp={onResizePointerUp}
        className="absolute bottom-0 right-0 w-4 h-4 cursor-nwse-resize"
        style={{
          background:
            "linear-gradient(135deg, transparent 50%, rgba(0,0,0,0.3) 50%, rgba(0,0,0,0.3) 60%, transparent 60%, transparent 75%, rgba(0,0,0,0.3) 75%, rgba(0,0,0,0.3) 85%, transparent 85%)",
        }}
      />
    </div>
  );
}

function FilterBtn({
  active,
  onClick,
  label,
  color,
}: {
  active: boolean;
  onClick: () => void;
  label: string;
  color: string;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "flex items-center gap-1.5 px-2.5 py-1 rounded-md border text-[11px] transition",
        active
          ? "bg-neutral-900 text-white border-neutral-900"
          : "bg-white border-neutral-200 text-neutral-700 hover:border-neutral-400",
      )}
    >
      <span
        className="inline-block w-2 h-2 rounded-sm"
        style={{ background: color }}
      />
      {label}
    </button>
  );
}

function projectRow(r: CompareRow, status: Row["status"]): Row {
  return {
    status,
    emdcd: r.emdcd,
    emdnm: r.emdnm,
    sidonm: r.sidonm ?? "",
    sggnm: r.sggnm ?? "",
    versionKey: r.version_key,
    iou: typeof r.iou === "number" ? r.iou : null,
  };
}

function fmtKey(k: string) {
  return `${k.slice(0, 4)}-${k.slice(4, 6)}-${k.slice(6, 8)}`;
}
