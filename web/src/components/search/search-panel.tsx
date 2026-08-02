"use client";

import { useMemo, useState } from "react";
import { Search, Loader2, AlertCircle, MapPinOff } from "lucide-react";
import type { FindRow, Level, OfficeRow } from "admdongkor";
import { useFind } from "@/hooks/use-find";
import { useFindOffices } from "@/hooks/use-find-offices";
import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

/** lib 의 find() 와 동일한 판정 — 숫자로만 이루어진 쿼리는 코드 검색. */
function isCodeQuery(q: string): boolean {
  const compact = q.trim().replace(/\s+/g, "");
  return compact.length > 0 && /^[0-9]+$/.test(compact);
}

export function SearchPanel() {
  const [query, setQuery] = useState("");
  const { rows, loading, error } = useFind(query);
  const { rows: offices } = useFindOffices(query);
  const codeMode = isCodeQuery(query);

  return (
    <div className="space-y-3">
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="행정구역명 또는 코드 (예: 판교, 11110)"
          className="w-full pl-8 pr-3 py-2 text-xs rounded-md border border-border bg-muted focus:outline-none focus:border-accent focus:bg-background"
        />
        {loading && (
          <Loader2 className="absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 animate-spin text-muted-foreground" />
        )}
        {!loading && codeMode && (
          <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[10px] px-1.5 py-0.5 rounded bg-accent/15 text-accent font-medium">
            코드
          </span>
        )}
      </div>

      {error && (
        <div className="flex items-start gap-2 text-[11px] text-red-600 px-2 py-1.5 rounded bg-red-50 dark:bg-red-950/30">
          <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>{error.message}</span>
        </div>
      )}

      <SearchHints query={query} />
      {codeMode && !loading && (
        <CodeAnswer rows={rows} offices={offices} query={query.trim()} />
      )}
      <ResultsList rows={rows} query={query} codeMode={codeMode} />
      <OfficeResults rows={offices} codeMode={codeMode} />
    </div>
  );
}

function fmtDate(d: string | null | undefined) {
  if (!d || d.length !== 8) return null;
  return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
}

/**
 * 코드 검색의 **한 줄 답**. "이 코드가 어디인가" 를 결과 목록을 훑지 않고
 * 바로 읽을 수 있게 맨 위에 고정한다.
 *
 * 같은 코드라도 시점에 따라 이름이 바뀌므로 (예: 1975 누상동 → 1980 사직동)
 * 서로 다른 이름이 여럿이면 전부 보여준다.
 */
function CodeAnswer({
  rows,
  offices,
  query,
}: {
  rows: FindRow[];
  offices: OfficeRow[];
  query: string;
}) {
  const answer = useMemo(() => {
    // 입력한 코드와 정확히 일치하는 행을 우선 (prefix 로 딸려온 하위 구역 제외).
    const exact = rows.filter(
      (r) => r.code === query || r.code7 === query || r.code8 === query,
    );
    const pool = exact.length > 0 ? exact : [];
    const byName = new Map<
      string,
      { label: string; parent: string; level: Level; versions: string[] }
    >();
    for (const r of pool) {
      const parent = [r.sidonm, r.sggnm].filter(Boolean).join(" ");
      const k = `${parent}|${r.name}`;
      const hit = byName.get(k);
      if (hit) hit.versions.push(r.version_key);
      else
        byName.set(k, {
          label: r.name,
          parent,
          level: r.level,
          versions: [r.version_key],
        });
    }
    return [...byName.values()];
  }, [rows, query]);

  const exactOffices = offices.filter((o) => o.code === query);

  if (answer.length === 0 && exactOffices.length === 0) return null;

  return (
    <div className="rounded-md border border-accent/40 bg-accent/5 px-2.5 py-2 space-y-1.5">
      <div className="text-[10px] text-muted-foreground font-mono">{query}</div>
      {answer.map((a) => (
        <div key={`${a.parent}|${a.label}`} className="space-y-0.5">
          <div className="text-sm font-semibold leading-tight">
            {a.parent ? `${a.parent} ` : ""}
            {a.label}
          </div>
          <div className="text-[10px] text-muted-foreground">
            {a.versions.length === 1
              ? fmt(a.versions[0]!)
              : `${fmt(a.versions[0]!)} – ${fmt(a.versions[a.versions.length - 1]!)} · ${a.versions.length} 시점`}
          </div>
        </div>
      ))}
      {exactOffices.map((o) => (
        <div key={o.code} className="space-y-0.5">
          <div className="text-sm font-semibold leading-tight flex items-center gap-1.5">
            <MapPinOff className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            <span>
              {[o.sidonm, o.sggnm].filter(Boolean).join(" ")} {o.name}
            </span>
          </div>
          <div className="text-[10px] text-muted-foreground">
            출장소 · 지도 없음 · {fmtDate(o.created) ?? "?"} ~{" "}
            {o.abolished ? `${fmtDate(o.abolished)} 말소` : "현재"}
          </div>
        </div>
      ))}
    </div>
  );
}

function SearchHints({ query }: { query: string }) {
  if (query.trim()) return null;
  return (
    <div className="text-[11px] text-muted-foreground space-y-1 leading-relaxed">
      <div>· 1 단어 → 전 레벨 substring</div>
      <div>· 2 단어 → 시군구 자동 필터 (예: "수원시 권선구")</div>
      <div>· 3 단어 → 읍면동 (예: "서울특별시 종로구 사직동")</div>
      <div className="pt-1">· 숫자만 입력 → 코드 검색 (하위 구역까지)</div>
      <div className="pl-2 font-mono text-[10px]">
        11 · 11110 · 1111051500 · 통계청 7/8 자리
      </div>
    </div>
  );
}

interface GroupedRow {
  key: string;
  level: Level;
  name: string;
  sidonm: string | null;
  sggnm: string | null;
  code: string | null;
  /** 통계청 7/8 자리 — 코드 검색에서 어느 체계로 걸렸는지 보여주기 위해 유지. */
  code7: string | null;
  code8: string | null;
  /** 이 매치에 등장한 version_key 목록 (정렬됨). */
  versions: string[];
}

function groupByLocation(rows: FindRow[]): GroupedRow[] {
  const map = new Map<string, GroupedRow>();
  for (const r of rows) {
    const key = `${r.level}|${r.sidonm ?? ""}|${r.sggnm ?? ""}|${r.name}|${r.code ?? ""}`;
    const existing = map.get(key);
    if (existing) {
      existing.versions.push(r.version_key);
      // 같은 지역이라도 시점별로 통계청 코드가 붙었다 없었다 하므로,
      // 처음 등장한 non-null 값을 유지한다.
      existing.code7 ??= r.code7;
      existing.code8 ??= r.code8;
    } else {
      map.set(key, {
        key,
        level: r.level,
        name: r.name,
        sidonm: r.sidonm,
        sggnm: r.sggnm,
        code: r.code,
        code7: r.code7,
        code8: r.code8,
        versions: [r.version_key],
      });
    }
  }
  return [...map.values()].sort((a, b) => {
    const la = levelRank(a.level) - levelRank(b.level);
    if (la !== 0) return la;
    return a.name.localeCompare(b.name, "ko");
  });
}

function levelRank(l: Level): number {
  return l === "sido" ? 0 : l === "sgg" ? 1 : 2;
}

function fmt(k: string) {
  return `${k.slice(0, 4)}-${k.slice(4, 6)}-${k.slice(6, 8)}`;
}

function ResultsList({
  rows,
  query,
  codeMode,
}: {
  rows: FindRow[];
  query: string;
  codeMode: boolean;
}) {
  const grouped = useMemo(() => groupByLocation(rows), [rows]);
  const setVersionKey = useAppStore((s) => s.setVersionKey);
  const setLevel = useAppStore((s) => s.setLevel);
  const requestFlyTo = useAppStore((s) => s.requestFlyTo);
  const setRightPanelOpen = useAppStore((s) => s.setRightPanelOpen);

  if (!query.trim()) return null;
  if (grouped.length === 0) {
    return (
      <div className="text-[11px] text-muted-foreground py-4 text-center space-y-1">
        <div>검색 결과 없음</div>
        {codeMode && (
          <div className="text-[10px]">
            코드로 검색 중 — 이름으로 찾으려면 문자를 포함하세요.
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-1">
      <div className="text-[10px] text-muted-foreground px-1">
        {grouped.length} 개 지역 · 총 {rows.length} 매치
      </div>
      <ul className="space-y-1">
        {grouped.slice(0, 40).map((g) => (
          <ResultCard
            key={g.key}
            row={g}
            codeMode={codeMode}
            query={query.trim()}
            onPick={(versionKey) => {
              setVersionKey(versionKey);
              setLevel(g.level);
              const codeField =
                g.level === "sido" ? "sidocd" : g.level === "sgg" ? "sggcd" : "emdcd";
              // 이름 fallback: 코드가 null 인 1975~1985 등 구버전 대응
              const nameFields: Record<string, string> = {};
              if (g.level === "sido" && g.sidonm) nameFields["sidonm"] = g.sidonm;
              if (g.level === "sgg") {
                if (g.sidonm) nameFields["sidonm"] = g.sidonm;
                if (g.name) nameFields["sggnm"] = g.name;
              }
              if (g.level === "emd") {
                if (g.name) nameFields["emdnm"] = g.name;
                if (g.sggnm) nameFields["sggnm"] = g.sggnm;
              }
              // 모바일에서는 RightPanel 이 360px 차지해 지도 영역이 거의 0 이 됨.
              // 이 상태에서 fitBounds 가 돌면 사용자는 효과를 못 보므로,
              // 좁은 화면이면 패널을 닫아 지도가 보이게 한 뒤 flyTo 를 요청한다.
              if (typeof window !== "undefined" && window.matchMedia("(max-width: 768px)").matches) {
                setRightPanelOpen(false);
              }
              requestFlyTo({
                codeField,
                codeValue: g.code,
                level: g.level,
                nameFields: Object.keys(nameFields).length > 0 ? nameFields : undefined,
              });
            }}
          />
        ))}
      </ul>
      {grouped.length > 40 && (
        <div className="text-[10px] text-muted-foreground px-1 pt-1">
          상위 40개만 표시 — 검색어를 더 구체화하세요.
        </div>
      )}
    </div>
  );
}

/**
 * 코드 검색에서 이 행이 **어느 코드 체계로** 걸렸는지 표시.
 * 행안부 `code` 로 걸렸으면 굳이 표시하지 않고 (이미 코드가 보임),
 * 통계청 7/8 자리로만 걸린 경우에만 배지를 단다.
 */
function CodeMatchTag({ row, query }: { row: GroupedRow; query: string }) {
  if (row.code?.startsWith(query)) return null;
  const via = row.code7?.startsWith(query)
    ? { label: "통계청7", value: row.code7 }
    : row.code8?.startsWith(query)
      ? { label: "통계청8", value: row.code8 }
      : null;
  if (!via) return null;
  return (
    <span
      className="shrink-0 px-1 rounded bg-muted text-muted-foreground"
      title={`통계청 코드 ${via.value} 로 매치`}
    >
      {via.label} {via.value}
    </span>
  );
}

/**
 * 출장소 결과 — 지도에 연결되지 않으므로 일반 결과와 **분리된 섹션**으로 둔다.
 * 클릭해도 flyTo 가 없다는 걸 배지와 아이콘으로 분명히 한다.
 */
function OfficeResults({
  rows,
  codeMode,
}: {
  rows: OfficeRow[];
  codeMode: boolean;
}) {
  if (rows.length === 0) return null;
  const shown = rows.slice(0, 20);
  return (
    <div className="space-y-1 pt-1">
      <div className="flex items-center gap-1.5 px-1 pt-1 border-t border-border">
        <MapPinOff className="h-3 w-3 text-muted-foreground shrink-0" />
        <span className="text-[10px] text-muted-foreground">
          출장소 {rows.length} 건 · 지도 없음
        </span>
      </div>
      <ul className="space-y-1">
        {shown.map((o) => {
          const parent = [o.sidonm, o.sggnm].filter(Boolean).join(" · ");
          return (
            <li
              key={`${o.code}|${o.created ?? ""}`}
              className="rounded-md border border-dashed border-border bg-muted/30 px-2.5 py-1.5"
            >
              <div className="flex items-center justify-between gap-2">
                <div className="min-w-0 flex-1">
                  <div className="text-xs font-medium truncate">{o.name}</div>
                  {parent && (
                    <div className="text-[10px] text-muted-foreground truncate">
                      {parent}
                    </div>
                  )}
                </div>
                <span
                  className={cn(
                    "text-[10px] px-1.5 py-0.5 rounded shrink-0",
                    o.abolished
                      ? "bg-muted text-muted-foreground"
                      : "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-200",
                  )}
                >
                  {o.abolished ? "말소" : "현존"}
                </span>
              </div>
              <div className="flex items-center justify-between gap-1 text-[10px] pt-0.5">
                <span className="text-muted-foreground font-mono">{o.code}</span>
                <span className="text-muted-foreground">
                  {fmtDate(o.created) ?? "?"} ~{" "}
                  {o.abolished ? fmtDate(o.abolished) : "현재"}
                </span>
              </div>
            </li>
          );
        })}
      </ul>
      {rows.length > shown.length && (
        <div className="text-[10px] text-muted-foreground px-1">
          상위 {shown.length}개만 표시 —{" "}
          {codeMode ? "코드를 더 길게" : "검색어를 더 구체적으로"} 입력하세요.
        </div>
      )}
    </div>
  );
}

function ResultCard({
  row,
  onPick,
  codeMode,
  query,
}: {
  row: GroupedRow;
  onPick: (versionKey: string) => void;
  codeMode: boolean;
  query: string;
}) {
  const lvlLabel =
    row.level === "sido" ? "시도" : row.level === "sgg" ? "시군구" : "읍면동";
  const parent = [row.sidonm, row.sggnm].filter(Boolean).join(" · ");
  const first = row.versions[0]!;
  const last = row.versions[row.versions.length - 1]!;

  return (
    <li className="rounded-md border border-border bg-background hover:border-accent transition">
      <div className="px-2.5 py-2 space-y-1.5">
        <div className="flex items-center justify-between gap-2">
          <div className="min-w-0 flex-1">
            <div className="text-xs font-semibold truncate">{row.name}</div>
            {parent && (
              <div className="text-[10px] text-muted-foreground truncate">
                {parent}
              </div>
            )}
          </div>
          <span
            className={cn(
              "text-[10px] px-1.5 py-0.5 rounded shrink-0",
              row.level === "sido"
                ? "bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-200"
                : row.level === "sgg"
                  ? "bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-200"
                  : "bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-200",
            )}
          >
            {lvlLabel}
          </span>
        </div>

        <div className="flex items-center justify-between gap-1 text-[10px]">
          <span className="text-muted-foreground font-mono flex items-center gap-1 min-w-0">
            <span className="truncate">{row.code ?? "—"}</span>
            {codeMode && <CodeMatchTag row={row} query={query} />}
          </span>
          <span className="text-muted-foreground">
            {row.versions.length === 1
              ? fmt(first)
              : `${fmt(first)} – ${fmt(last)} (${row.versions.length})`}
          </span>
        </div>

        <div className="flex gap-1">
          <button
            onClick={() => onPick(first)}
            className="flex-1 text-[10px] px-1.5 py-1 rounded border border-border hover:border-accent hover:text-accent"
            title="가장 이른 시점으로 지도 이동"
          >
            최초 | {fmt(first)}
          </button>
          {row.versions.length > 1 && (
            <button
              onClick={() => onPick(last)}
              className="flex-1 text-[10px] px-1.5 py-1 rounded border border-border hover:border-accent hover:text-accent"
              title="가장 늦은 시점으로 지도 이동"
            >
              최근 | {fmt(last)}
            </button>
          )}
        </div>
      </div>
    </li>
  );
}
