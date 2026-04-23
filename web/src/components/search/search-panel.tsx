"use client";

import { useMemo, useState } from "react";
import { Search, Loader2, AlertCircle } from "lucide-react";
import type { FindRow, Level } from "admdongkor";
import { useFind } from "@/hooks/use-find";
import { useAppStore } from "@/stores/app-store";
import { cn } from "@/lib/cn";

export function SearchPanel() {
  const [query, setQuery] = useState("");
  const { rows, loading, error } = useFind(query);

  return (
    <div className="space-y-3">
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--muted-foreground)]" />
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="행정구역명 (예: 판교, 서울 종로구)"
          className="w-full pl-8 pr-3 py-2 text-xs rounded-md border border-[var(--border)] bg-[var(--muted)] focus:outline-none focus:border-[var(--accent)] focus:bg-[var(--background)]"
        />
        {loading && (
          <Loader2 className="absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 animate-spin text-[var(--muted-foreground)]" />
        )}
      </div>

      {error && (
        <div className="flex items-start gap-2 text-[11px] text-red-600 px-2 py-1.5 rounded bg-red-50 dark:bg-red-950/30">
          <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>{error.message}</span>
        </div>
      )}

      <SearchHints query={query} />
      <ResultsList rows={rows} query={query} />
    </div>
  );
}

function SearchHints({ query }: { query: string }) {
  if (query.trim()) return null;
  return (
    <div className="text-[11px] text-[var(--muted-foreground)] space-y-1 leading-relaxed">
      <div>· 1 단어 → 전 레벨 substring</div>
      <div>· 2 단어 → 시군구 자동 필터 (예: "수원시 권선구")</div>
      <div>· 3 단어 → 읍면동 (예: "서울 종로 사직동")</div>
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
    } else {
      map.set(key, {
        key,
        level: r.level,
        name: r.name,
        sidonm: r.sidonm,
        sggnm: r.sggnm,
        code: r.code,
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

function ResultsList({ rows, query }: { rows: FindRow[]; query: string }) {
  const grouped = useMemo(() => groupByLocation(rows), [rows]);
  const setVersionKey = useAppStore((s) => s.setVersionKey);
  const setLevel = useAppStore((s) => s.setLevel);

  if (!query.trim()) return null;
  if (grouped.length === 0) {
    return (
      <div className="text-[11px] text-[var(--muted-foreground)] py-4 text-center">
        검색 결과 없음
      </div>
    );
  }

  return (
    <div className="space-y-1">
      <div className="text-[10px] text-[var(--muted-foreground)] px-1">
        {grouped.length} 개 지역 · 총 {rows.length} 매치
      </div>
      <ul className="space-y-1">
        {grouped.slice(0, 40).map((g) => (
          <ResultCard
            key={g.key}
            row={g}
            onPick={(versionKey) => {
              setVersionKey(versionKey);
              setLevel(g.level);
            }}
          />
        ))}
      </ul>
      {grouped.length > 40 && (
        <div className="text-[10px] text-[var(--muted-foreground)] px-1 pt-1">
          상위 40개만 표시 — 검색어를 더 구체화하세요.
        </div>
      )}
    </div>
  );
}

function ResultCard({
  row,
  onPick,
}: {
  row: GroupedRow;
  onPick: (versionKey: string) => void;
}) {
  const lvlLabel = row.level === "sido" ? "시도" : row.level === "sgg" ? "시군구" : "읍면동";
  const parent = [row.sidonm, row.sggnm].filter(Boolean).join(" · ");
  const first = row.versions[0]!;
  const last = row.versions[row.versions.length - 1]!;

  return (
    <li className="rounded-md border border-[var(--border)] bg-[var(--background)] hover:border-[var(--accent)] transition">
      <div className="px-2.5 py-2 space-y-1.5">
        <div className="flex items-center justify-between gap-2">
          <div className="min-w-0 flex-1">
            <div className="text-xs font-semibold truncate">{row.name}</div>
            {parent && (
              <div className="text-[10px] text-[var(--muted-foreground)] truncate">
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
          <span className="text-[var(--muted-foreground)] font-mono">
            {row.code ?? "—"}
          </span>
          <span className="text-[var(--muted-foreground)]">
            {row.versions.length === 1
              ? fmt(first)
              : `${fmt(first)} – ${fmt(last)} (${row.versions.length})`}
          </span>
        </div>

        <div className="flex gap-1">
          <button
            onClick={() => onPick(first)}
            className="flex-1 text-[10px] px-1.5 py-1 rounded border border-[var(--border)] hover:border-[var(--accent)] hover:text-[var(--accent)]"
            title="가장 이른 시점으로 지도 이동"
          >
            최초 → {fmt(first)}
          </button>
          {row.versions.length > 1 && (
            <button
              onClick={() => onPick(last)}
              className="flex-1 text-[10px] px-1.5 py-1 rounded border border-[var(--border)] hover:border-[var(--accent)] hover:text-[var(--accent)]"
              title="가장 늦은 시점으로 지도 이동"
            >
              최근 → {fmt(last)}
            </button>
          )}
        </div>
      </div>
    </li>
  );
}
