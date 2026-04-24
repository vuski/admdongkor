"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Search, Loader2, AlertCircle, Play, X } from "lucide-react";
import { VERSIONS, find } from "admdongkor";
import { fetchVersions } from "@/lib/timeline-client";
import type { TimelineQuerySelection } from "@/stores/timeline-store";
import {
  fmtVersionLabel,
  pickDefaultVersions,
  yearOf,
} from "@/lib/timeline-years";
import { useAppStore } from "@/stores/app-store";
import { useTimelineStore } from "@/stores/timeline-store";
import { cn } from "@/lib/cn";

const DEFAULT_BASE_VERSION = VERSIONS[VERSIONS.length - 1]!;

/** 이름 검색으로 나온 후보. 기준 연도의 find() 결과에서 level 에 맞게 필터. */
interface Candidate {
  code: string;
  name: string;
  sidonm?: string;
}

export function TimelinePanel() {
  const [allVersions, setAllVersions] = useState<string[]>([]);
  const [versionsErr, setVersionsErr] = useState<string | null>(null);

  // 부팅 시 versions.json 로드.
  useEffect(() => {
    fetchVersions()
      .then((v) => setAllVersions(v.versions))
      .catch((e) => setVersionsErr(String(e)));
  }, []);

  const [baseVersion, setBaseVersion] = useState<string>(DEFAULT_BASE_VERSION);
  const [level, setLevel] = useState<"sido" | "sgg">("sgg");
  const [query, setQuery] = useState("");
  const [searching, setSearching] = useState(false);
  const [searchErr, setSearchErr] = useState<string | null>(null);
  const [results, setResults] = useState<Candidate[]>([]);
  /** 코드 -> {name, sidonm?} 캐시 (선택된 것들 이름 표시용). */
  const [nameMap, setNameMap] = useState<Map<string, { name: string; sidonm?: string }>>(
    new Map(),
  );
  const [selectedCodes, setSelectedCodes] = useState<Set<string>>(new Set());

  // 레벨 바뀌면 선택/검색 결과 초기화.
  useEffect(() => {
    setSelectedCodes(new Set());
    setResults([]);
  }, [level]);

  // 기준 버전 바뀌면 선택 초기화 (코드가 연도마다 달라질 수 있음).
  useEffect(() => {
    setSelectedCodes(new Set());
    setResults([]);
  }, [baseVersion]);

  // 검색 (debounce 없이 즉시 — find 는 이미 캐시된 parquet 메모리 필터라 빠름).
  const searchToken = useRef(0);
  useEffect(() => {
    const q = query.trim();
    if (!q) {
      setResults([]);
      return;
    }
    const year = Number(baseVersion.slice(0, 4));
    const token = ++searchToken.current;
    setSearching(true);
    setSearchErr(null);
    find(q, { level, year: [year] })
      .then((rows) => {
        if (token !== searchToken.current) return;
        // 동일 연도 내에서 같은 code 가 중복될 수 있음 (거의 없지만 방어).
        const seen = new Set<string>();
        const out: Candidate[] = [];
        const nm = new Map(nameMap);
        for (const r of rows) {
          if (!r.code) continue;
          if (seen.has(r.code)) continue;
          seen.add(r.code);
          const c: Candidate = {
            code: r.code,
            name: r.name,
            sidonm: r.sidonm ?? undefined,
          };
          out.push(c);
          nm.set(r.code, { name: r.name, sidonm: r.sidonm ?? undefined });
          if (out.length >= 25) break;
        }
        setResults(out);
        setNameMap(nm);
      })
      .catch((e) => {
        if (token !== searchToken.current) return;
        setSearchErr(String(e));
        setResults([]);
      })
      .finally(() => {
        if (token === searchToken.current) setSearching(false);
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, level, baseVersion]);

  function toggleCode(code: string) {
    setSelectedCodes((prev) => {
      const next = new Set(prev);
      if (next.has(code)) {
        next.delete(code);
      } else {
        // sido 는 단일 선택만 (복수면 데이터 너무 많음).
        if (level === "sido") next.clear();
        next.add(code);
      }
      return next;
    });
  }
  function removeCode(code: string) {
    setSelectedCodes((prev) => {
      const next = new Set(prev);
      next.delete(code);
      return next;
    });
  }

  const setQueryStore = useTimelineStore((s) => s.setQuery);
  const selectedVersions = useTimelineStore((s) => s.selectedVersions);
  const setSelectedVersions = useTimelineStore((s) => s.setSelectedVersions);
  const toggleVersion = useTimelineStore((s) => s.toggleVersion);
  const setTimelineViewActive = useAppStore((s) => s.setTimelineViewActive);

  // 기본 체크 연도: 컴포넌트 처음 로드되고 allVersions 가 생겼을 때만 한 번.
  //   기준 버전이 기본 5년 간격에 안 걸리면 별도로 추가해 반드시 포함.
  const [versionsInitialized, setVersionsInitialized] = useState(false);
  useEffect(() => {
    if (!versionsInitialized && allVersions.length > 0) {
      setSelectedVersions(pickDefaultVersions(allVersions, 5, baseVersion));
      setVersionsInitialized(true);
    }
  }, [allVersions, versionsInitialized, baseVersion, setSelectedVersions]);

  // 기준 버전을 바꿨는데 현재 체크리스트에 없으면 자동 체크. (이미 있는 선택은 건드리지 않음.)
  useEffect(() => {
    if (!versionsInitialized) return;
    if (!allVersions.includes(baseVersion)) return;
    if (!selectedVersions.includes(baseVersion)) {
      toggleVersion(baseVersion);
    }
  }, [baseVersion, versionsInitialized, allVersions, selectedVersions, toggleVersion]);

  const canStart = selectedCodes.size > 0 && selectedVersions.length > 0;

  function onStart() {
    if (!canStart) return;
    const selections: TimelineQuerySelection[] = [];
    for (const code of selectedCodes) {
      const nm = nameMap.get(code);
      if (!nm) continue;
      const sel: TimelineQuerySelection = { code, name: nm.name };
      if (level === "sgg" && nm.sidonm) sel.parentName = nm.sidonm;
      selections.push(sel);
    }
    if (selections.length === 0) return;
    // baseVersion 이 체크 안 되어 있으면 자동 추가.
    if (!selectedVersions.includes(baseVersion) && allVersions.includes(baseVersion)) {
      setSelectedVersions([...selectedVersions, baseVersion]);
    }
    setQueryStore({
      baseVersion,
      level,
      selections,
    });
    setTimelineViewActive(true);
  }

  return (
    <div className="space-y-3">
      <FieldBaseVersion
        value={baseVersion}
        options={allVersions.length > 0 ? allVersions : [...VERSIONS]}
        onChange={setBaseVersion}
      />

      <FieldLevel value={level} onChange={setLevel} />

      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={
            level === "sido"
              ? "시도명 (예: 서울, 경기) — 1개만"
              : "시군구명 (예: 강남구, 수원시) — 복수 가능"
          }
          className="w-full pl-8 pr-3 py-2 text-xs rounded-md border border-border bg-muted focus:outline-none focus:border-accent focus:bg-background"
        />
        {searching && (
          <Loader2 className="absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 animate-spin text-muted-foreground" />
        )}
      </div>

      {(versionsErr || searchErr) && (
        <div className="flex items-start gap-2 text-[11px] text-red-600 px-2 py-1.5 rounded bg-red-50 dark:bg-red-950/30">
          <AlertCircle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>{versionsErr ?? searchErr}</span>
        </div>
      )}

      {query.trim() && (
        <ResultList
          results={results}
          level={level}
          selectedCodes={selectedCodes}
          onToggle={toggleCode}
        />
      )}

      {selectedCodes.size > 0 && (
        <SelectedChips
          level={level}
          nameMap={nameMap}
          codes={[...selectedCodes]}
          onRemove={removeCode}
          onClearAll={() => setSelectedCodes(new Set())}
          baseVersion={baseVersion}
        />
      )}

      <YearPicker
        allVersions={allVersions}
        selected={selectedVersions}
        onToggle={toggleVersion}
        onReset={() =>
          setSelectedVersions(pickDefaultVersions(allVersions, 5, baseVersion))
        }
      />

      <button
        onClick={onStart}
        disabled={!canStart}
        className={cn(
          "w-full flex items-center justify-center gap-1.5 text-xs py-2 rounded-md transition",
          canStart
            ? "bg-foreground text-background hover:opacity-90"
            : "bg-muted text-muted-foreground cursor-not-allowed",
        )}
      >
        <Play className="h-3.5 w-3.5" />
        시계열 추적 시작
      </button>
    </div>
  );
}

function FieldBaseVersion({
  value,
  options,
  onChange,
}: {
  value: string;
  options: string[];
  onChange: (v: string) => void;
}) {
  return (
    <label className="block">
      <span className="text-[10px] text-muted-foreground">기준 버전</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="mt-0.5 w-full text-xs py-1.5 px-2 rounded-md border border-border bg-muted focus:outline-none focus:border-accent focus:bg-background"
      >
        {options.map((v) => (
          <option key={v} value={v}>
            {fmtVersionLabel(v)}
          </option>
        ))}
      </select>
    </label>
  );
}

function FieldLevel({
  value,
  onChange,
}: {
  value: "sido" | "sgg";
  onChange: (v: "sido" | "sgg") => void;
}) {
  return (
    <div className="flex gap-1">
      {(["sido", "sgg"] as const).map((l) => (
        <button
          key={l}
          onClick={() => onChange(l)}
          className={cn(
            "flex-1 text-[11px] py-1.5 rounded-md border transition",
            value === l
              ? "border-accent bg-accent/10 text-accent"
              : "border-border hover:border-accent",
          )}
        >
          {l === "sido" ? "시도" : "시군구"}
        </button>
      ))}
    </div>
  );
}

function ResultList({
  results,
  level,
  selectedCodes,
  onToggle,
}: {
  results: Candidate[];
  level: "sido" | "sgg";
  selectedCodes: Set<string>;
  onToggle: (code: string) => void;
}) {
  if (results.length === 0) {
    return (
      <div className="text-[11px] text-muted-foreground py-2 text-center">
        검색 결과 없음
      </div>
    );
  }
  return (
    <ul className="space-y-1 max-h-48 overflow-y-auto">
      {results.map((r) => {
        const on = selectedCodes.has(r.code);
        return (
          <li key={r.code}>
            <button
              onClick={() => onToggle(r.code)}
              className={cn(
                "w-full text-left px-2 py-1.5 rounded border text-xs transition flex items-center gap-2",
                on
                  ? "border-accent bg-accent/10"
                  : "border-border hover:border-accent",
              )}
            >
              <div
                className={cn(
                  "w-3.5 h-3.5 rounded-sm border flex items-center justify-center shrink-0",
                  on ? "bg-accent border-accent" : "border-border",
                )}
              >
                {on && (
                  <svg viewBox="0 0 10 10" className="w-2.5 h-2.5 text-accent-foreground">
                    <path
                      d="M1.5 5l2 2 5-5"
                      stroke="currentColor"
                      strokeWidth="2"
                      fill="none"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                )}
              </div>
              <div className="min-w-0 flex-1">
                <div className="font-semibold truncate">{r.name}</div>
                {level === "sgg" && r.sidonm && (
                  <div className="text-[10px] text-muted-foreground truncate">
                    {r.sidonm}
                  </div>
                )}
                <div className="text-[10px] font-mono text-muted-foreground">
                  {r.code}
                </div>
              </div>
            </button>
          </li>
        );
      })}
    </ul>
  );
}

function SelectedChips({
  level,
  nameMap,
  codes,
  baseVersion,
  onRemove,
  onClearAll,
}: {
  level: "sido" | "sgg";
  nameMap: Map<string, { name: string; sidonm?: string }>;
  codes: string[];
  baseVersion: string;
  onRemove: (code: string) => void;
  onClearAll: () => void;
}) {
  return (
    <div className="border border-accent/50 bg-accent/5 rounded-md p-2 text-xs space-y-1">
      <div className="flex items-center justify-between gap-2 text-[10px] text-muted-foreground">
        <span>선택 {codes.length}개 · 기준 {fmtVersionLabel(baseVersion)}</span>
        <button
          type="button"
          onClick={onClearAll}
          className="px-1.5 py-0.5 rounded border border-border hover:border-accent hover:text-accent transition"
        >
          모두 비우기
        </button>
      </div>
      <div className="flex flex-wrap gap-1">
        {codes.map((code) => {
          const nm = nameMap.get(code);
          if (!nm) return null;
          return (
            <span
              key={code}
              className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-background border border-border text-[11px]"
            >
              {nm.name}
              <button
                onClick={() => onRemove(code)}
                className="p-0.5 rounded hover:bg-muted"
                aria-label="제거"
              >
                <X className="w-2.5 h-2.5" />
              </button>
            </span>
          );
        })}
      </div>
    </div>
  );
}

function YearPicker({
  allVersions,
  selected,
  onToggle,
  onReset,
}: {
  allVersions: string[];
  selected: string[];
  onToggle: (v: string) => void;
  onReset: () => void;
}) {
  const byYear = useMemo(() => {
    const m = new Map<number, string[]>();
    for (const v of allVersions) {
      const y = yearOf(v);
      const arr = m.get(y) ?? [];
      arr.push(v);
      m.set(y, arr);
    }
    return m;
  }, [allVersions]);

  const years = useMemo(() => [...byYear.keys()].sort((a, b) => a - b), [byYear]);

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between text-[10px] text-muted-foreground">
        <span>추적 대상 연도 ({selected.length})</span>
        <button
          type="button"
          onClick={onReset}
          className="text-[9px] px-1.5 py-0.5 rounded border border-border hover:border-accent hover:text-accent transition"
          title="기본 체크 상태로 재설정"
        >
          기본: 5년 간격 + 최근 + 기준연도
        </button>
      </div>
      <div className="max-h-64 overflow-y-auto border border-border rounded-md divide-y divide-border">
        {years.map((y) => {
          const vs = byYear.get(y)!;
          return (
            <div key={y} className="px-2 py-1.5">
              <div className="text-[10px] font-mono text-muted-foreground mb-0.5">{y}</div>
              <div className="flex flex-wrap gap-1">
                {vs.map((v) => {
                  const on = selected.includes(v);
                  return (
                    <button
                      key={v}
                      onClick={() => onToggle(v)}
                      className={cn(
                        "text-[10px] px-1.5 py-0.5 rounded border transition font-mono",
                        on
                          ? "border-accent bg-accent text-accent-foreground"
                          : "border-border hover:border-accent",
                      )}
                    >
                      {v.slice(4)}
                    </button>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
