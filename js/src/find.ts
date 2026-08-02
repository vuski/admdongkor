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
  /**
   * 이름 검색이면 `name` 컬럼 단독 완전 일치 (공백 포함 쿼리와 결합 불가).
   * 코드 검색이면 prefix 대신 **자릿수 완전일치**.
   */
  exact?: boolean;
  /**
   * `"name"` / `"code"` 로 검색 방식 강제. 생략하면 자동 판별
   * (숫자로만 이루어진 쿼리 → 코드 검색).
   */
  by?: "name" | "code";
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

/**
 * 코드 검색이 훑는 컬럼. code = 행안부(sido 2 / sgg 5 / emd 10),
 * code7 / code8 = 통계청 (emd 레벨에서만 채워짐).
 */
const CODE_COLUMNS = ["code", "code7", "code8"] as const;

function nfc(s: string): string {
  return s.normalize("NFC");
}

/**
 * 숫자로만 이루어진 쿼리면 코드로 본다. 행정구역명 중 숫자 *만* 인 것은
 * 없으므로 이름 검색과 충돌하지 않는다.
 */
function isCodeQuery(q: string): boolean {
  return q.length > 0 && /^[0-9]+$/.test(q);
}

/** `code` / `code7` / `code8` 중 하나라도 걸리면 매치. */
function matchesCode(row: FindRow, code: string, exact: boolean): boolean {
  for (const col of CODE_COLUMNS) {
    const v = row[col];
    if (typeof v !== "string") continue;
    if (exact ? v === code : v.startsWith(code)) return true;
  }
  return false;
}

export function clearIndexCache(): void {
  clearLoaderCache();
}

/**
 * 행정구역명 **또는 코드**로 버전 검색.
 *
 * 숫자로만 이루어진 쿼리는 자동으로 **코드 검색**이 된다 (행정구역명 중 숫자로만
 * 된 것은 없어 이름 검색과 충돌하지 않는다). `by` 로 강제 가능.
 *
 * ```ts
 * await find("종로구");        // 이름 검색
 * await find("11110");         // 코드 — 시군구 11110 + 하위 읍면동 전부
 * await find("1111051500");    // 코드 — 해당 읍면동
 * await find("11110", { by: "name" });   // 이름 검색 강제
 * ```
 *
 * 코드 검색은 **prefix 매칭**이라 자릿수를 정확히 맞추지 않아도 된다.
 * `"11"` → 시도 11 + 시군구 `11xxx` + 읍면동 `11xxxxxxxx` 전부.
 * `level` 로 좁히고, `exact: true` 면 자릿수 완전일치만.
 * `code`(행안부) / `code7` / `code8`(통계청) 셋 다 매칭 대상.
 */
export async function find(
  name: string,
  options: FindOptions = {},
): Promise<FindRow[]> {
  if (typeof name !== "string") {
    throw new TypeError(`name must be string, got ${typeof name}`);
  }
  const { level, exact = false, year, by, baseUrl, fetch: fetchFn, signal } = options;

  if (level !== undefined && !LEVELS.includes(level)) {
    throw new Error(
      `level must be one of ${JSON.stringify(LEVELS)} or undefined, got ${JSON.stringify(level)}`,
    );
  }
  if (by !== undefined && by !== "name" && by !== "code") {
    throw new Error(
      `by must be "name", "code", or undefined, got ${JSON.stringify(by)}`,
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

  // 코드 검색 여부 판정. by 가 명시되면 그 쪽을 강제.
  const query = tokens.join("");
  let codeMode: boolean;
  if (by === "code") {
    if (!isCodeQuery(query)) {
      throw new Error(
        `by="code" requires a digits-only query, got ${JSON.stringify(name)}`,
      );
    }
    codeMode = true;
  } else if (by === "name") {
    codeMode = false;
  } else {
    codeMode = isCodeQuery(query);
  }

  let effectiveLevel: Level | null;
  let needle = "";
  if (codeMode) {
    effectiveLevel = level ?? null;
  } else {
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
    effectiveLevel = level ?? AUTO_LEVEL[tokens.length] ?? null;
    needle = query.toLowerCase();
  }

  const rows = await loadIndexFile<IndexRow>("_index_v3.parquet", {
    baseUrl,
    fetch: fetchFn,
    signal,
  });

  const yearLo = year && year.length >= 1 ? Math.min(...year) : null;
  const yearHi = year && year.length === 2 ? Math.max(...year) : yearLo;

  const matched = rows.filter((r) => {
    if (codeMode) {
      if (!matchesCode(r, query, exact)) return false;
    } else if (exact) {
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
