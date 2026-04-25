import { clearLoaderCache, loadIndexFile } from "./_index-loader.js";
import type { Level } from "./types.js";

export interface FindRow {
  version_key: string;
  level: Level;
  sidonm: string | null;
  sggnm: string | null;
  name: string;
  code: string | null;
  code7: string | null;
  code8: string | null;
  sggcd: string | null;
  sidocd: string | null;
}

interface IndexRow extends FindRow {
  _fullpath: string;
}

export interface FindOptions {
  level?: Level;
  /** true 면 `name` 컬럼 단독 완전 일치. 공백 포함 쿼리와 결합 불가. */
  exact?: boolean;
  /** `[2025]` 단일 연도 / `[2000, 2005]` inclusive range. 길이 1 또는 2. */
  year?: number[];
  /** 인덱스 parquet 베이스 URL 오버라이드. */
  baseUrl?: string;
  /** 커스텀 fetch (테스트용). */
  fetch?: typeof fetch;
  signal?: AbortSignal;
}

const LEVELS: readonly Level[] = ["sido", "sgg", "emd"] as const;
const LEVEL_ORDER: Record<Level, number> = { sido: 0, sgg: 1, emd: 2 };
const AUTO_LEVEL: Record<number, Level | null> = { 1: null, 2: "sgg", 3: "emd" };

function nfc(s: string): string {
  return s.normalize("NFC");
}

export function clearIndexCache(): void {
  clearLoaderCache();
}

export async function find(
  name: string,
  options: FindOptions = {},
): Promise<FindRow[]> {
  if (typeof name !== "string") {
    throw new TypeError(`name must be string, got ${typeof name}`);
  }
  const { level, exact = false, year, baseUrl, fetch: fetchFn, signal } = options;

  if (level !== undefined && !LEVELS.includes(level)) {
    throw new Error(
      `level must be one of ${JSON.stringify(LEVELS)} or undefined, got ${JSON.stringify(level)}`,
    );
  }
  if (year !== undefined) {
    if (!Array.isArray(year) || !year.every((y) => Number.isInteger(y))) {
      throw new TypeError("year must be number[] (integers)");
    }
    if (year.length !== 1 && year.length !== 2) {
      throw new Error(
        `year must have length 1 (single year) or 2 (inclusive range), got ${year.length}`,
      );
    }
  }

  const tokens = nfc(name).trim().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) throw new Error("name cannot be empty");
  if (tokens.length > 3) {
    throw new Error(
      `name must have 1-3 whitespace-separated tokens (sido, sgg, emd), got ${tokens.length}`,
    );
  }
  const multiToken = tokens.length >= 2;
  if (exact && multiToken) {
    throw new Error(
      "exact=true requires a single-token name (no whitespace). Use level to narrow scope instead.",
    );
  }

  const effectiveLevel = level ?? AUTO_LEVEL[tokens.length] ?? null;
  const needle = tokens.join("").toLowerCase();

  const rows = await loadIndexFile<IndexRow>("_index_v3.parquet", {
    baseUrl,
    fetch: fetchFn,
    signal,
  });

  const yearLo = year && year.length >= 1 ? Math.min(...year) : null;
  const yearHi = year && year.length === 2 ? Math.max(...year) : yearLo;

  const matched = rows.filter((r) => {
    if (exact) {
      if (nfc(r.name ?? "").toLowerCase() !== needle) return false;
    } else {
      if (!(r._fullpath ?? "").includes(needle)) return false;
    }
    if (effectiveLevel !== null && r.level !== effectiveLevel) return false;
    if (yearLo !== null) {
      const y = Number(r.version_key.slice(0, 4));
      if (yearHi === null) return false;
      if (y < yearLo || y > yearHi) return false;
    }
    return true;
  });

  matched.sort((a, b) => {
    if (a.version_key !== b.version_key) {
      return a.version_key < b.version_key ? -1 : 1;
    }
    const la = LEVEL_ORDER[a.level];
    const lb = LEVEL_ORDER[b.level];
    if (la !== lb) return la - lb;
    const ca = a.code ?? "";
    const cb = b.code ?? "";
    if (ca !== cb) return ca < cb ? -1 : 1;
    return 0;
  });

  return matched.map(({ _fullpath: _omit, ...rest }) => rest);
}

export function findVersions(rows: FindRow[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const r of rows) {
    if (!seen.has(r.version_key)) {
      seen.add(r.version_key);
      out.push(r.version_key);
    }
  }
  return out;
}

export function findFirst(rows: FindRow[]): string | null {
  const v = findVersions(rows);
  return v.length > 0 ? (v[0] ?? null) : null;
}

export function findLast(rows: FindRow[]): string | null {
  const v = findVersions(rows);
  return v.length > 0 ? (v[v.length - 1] ?? null) : null;
}
