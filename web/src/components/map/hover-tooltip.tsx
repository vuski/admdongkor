"use client";

import type { HoverInfo } from "./hover-types";

interface Props {
  info: HoverInfo | null;
}

function formatArea(m2: number | undefined): string {
  if (typeof m2 !== "number" || !Number.isFinite(m2)) return "—";
  if (m2 >= 1_000_000) return `${(m2 / 1_000_000).toFixed(2)} km²`;
  if (m2 >= 10_000) return `${(m2 / 1_000_000).toFixed(3)} km²`;
  return `${Math.round(m2).toLocaleString()} m²`;
}

function shortSidoColor(side: "A" | "B"): string {
  return side === "A" ? "#1c7057" : "#c2500f";
}

export function HoverTooltip({ info }: Props) {
  if (!info) return null;
  const { side, level, x, y } = info;

  // 제목 / 상위 계층 / 코드 구성
  let title = "";
  let subtitle = "";
  let codeLabel = "";
  let codeValue: string | null = null;

  if (level === "sido") {
    title = info.sidonm ?? "—";
    subtitle = "시도";
    codeLabel = "sidocd";
    codeValue = info.sidocd ?? null;
  } else if (level === "sgg") {
    title = info.sggnm ?? "—";
    subtitle = info.sidonm ?? "";
    codeLabel = "sggcd";
    codeValue = info.sggcd ?? null;
  } else {
    title = info.emdnm ?? "—";
    subtitle = `${info.sidonm ?? ""} · ${info.sggnm ?? ""}`.trim();
    codeLabel = "emdcd";
    codeValue = info.emdcd ?? null;
  }

  // x,y 는 deck.gl 이 주는 로컬 pane 좌표. pane 이 좌우로 clip 되어 있지만
  // 툴팁은 fixed 대신 absolute + 현재 pane 안에 찍히면 OK (map-container 자식).
  // 오른쪽/아래로 퍼지되 화면 밖으로 나가면 왼쪽/위로 플립.
  const style: React.CSSProperties = {
    left: x + 12,
    top: y + 12,
    pointerEvents: "none",
  };

  return (
    <div
      className="absolute z-30 min-w-[180px] max-w-[260px] rounded-md bg-white shadow-lg border border-black/10 px-3 py-2 text-xs"
      style={style}
    >
      <div className="flex items-center gap-1.5 mb-1">
        <span
          className="inline-block h-2 w-2 rounded-sm"
          style={{ background: shortSidoColor(side) }}
        />
        <span className="text-[10px] font-medium text-neutral-500">
          {subtitle || " "}
        </span>
      </div>
      <div className="text-sm font-semibold text-neutral-900 leading-tight mb-1.5">
        {title}
      </div>
      <div className="grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5 text-[10px] text-neutral-600 font-mono">
        {codeValue && (
          <>
            <span className="text-neutral-400">{codeLabel}</span>
            <span className="truncate">{codeValue}</span>
          </>
        )}
        {level === "emd" && info.emd7 && (
          <>
            <span className="text-neutral-400">emd7</span>
            <span className="truncate">{info.emd7}</span>
          </>
        )}
        {level === "emd" && info.emd8 && (
          <>
            <span className="text-neutral-400">emd8</span>
            <span className="truncate">{info.emd8}</span>
          </>
        )}
        <span className="text-neutral-400">area</span>
        <span>{formatArea(info.area)}</span>
      </div>
    </div>
  );
}
