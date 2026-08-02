import { loadIndexFile } from "./_index-loader.js";

/**
 * 출장소(出張所) 한 건.
 *
 * 출장소는 행안부 행정동 코드 체계에만 존재하고 **경계 지도가 없다**.
 * 그래서 `version_key` 대신 `created` / `abolished` (YYYYMMDD) 로 유효 기간을
 * 나타낸다. `abolished` 가 null 이면 현존.
 */
export interface OfficeRow {
  /** 행안부 행정동 10자리. */
  code: string;
  name: string;
  sggnm: string | null;
  sidonm: string | null;
  sggcd: string | null;
  sidocd: string | null;
  /** 뒤 5자리가 00000 이면 "sgg", 아니면 "emd". */
  level: "sgg" | "emd";
  created: string | null;
  abolished: string | null;
}

interface OfficeIndexRow extends OfficeRow {
  _fullpath: string;
}

export interface FindOfficesOptions {
  /** 코드 완전일치 / 이름 완전일치. 기본은 prefix·substring. */
  exact?: boolean;
  /** `"name"` / `"code"` 로 검색 방식 강제. 생략하면 자동 판별. */
  by?: "name" | "code";
  baseUrl?: string;
  fetch?: typeof fetch;
  signal?: AbortSignal;
}

const OFFICES_FILE = "_offices.parquet";

function nfc(s: string): string {
  return s.normalize("NFC");
}

/**
 * 출장소 검색. 코드(prefix) 또는 이름(substring).
 *
 * **지도가 없으므로 `get()` 으로 경계를 받을 수 없고, `find()` 결과에도 포함되지
 * 않는다.** 코드를 넣었을 때 "이게 어디인지" 를 알려주기 위한 별도 경로.
 *
 * ```ts
 * await findOffices("2920083000");  // 광주 광산구 임곡출장소 (1995~1998 말소)
 * await findOffices("28265");       // 인천 서구 검단출장소
 * await findOffices("영종");         // 이름으로
 * ```
 */
export async function findOffices(
  query: string,
  options: FindOfficesOptions = {},
): Promise<OfficeRow[]> {
  if (typeof query !== "string") {
    throw new TypeError(`query must be string, got ${typeof query}`);
  }
  const { exact = false, by, baseUrl, fetch: fetchFn, signal } = options;
  if (by !== undefined && by !== "name" && by !== "code") {
    throw new Error(
      `by must be "name", "code", or undefined, got ${JSON.stringify(by)}`,
    );
  }

  const q = nfc(query).trim().split(/\s+/).filter(Boolean).join("");
  if (!q) return [];

  const codeMode = by === undefined ? /^[0-9]+$/.test(q) : by === "code";

  let rows: OfficeIndexRow[];
  try {
    rows = await loadIndexFile<OfficeIndexRow>(OFFICES_FILE, {
      baseUrl,
      fetch: fetchFn,
      signal,
    });
  } catch (e) {
    // 구버전 dist/data 에는 _offices.parquet 이 없다. 검색 자체를 깨뜨리지 않는다.
    if (signal?.aborted) throw e;
    return [];
  }

  const needle = q.toLowerCase();
  const matched = rows.filter((r) => {
    if (codeMode) {
      const c = r.code ?? "";
      return exact ? c === q : c.startsWith(q);
    }
    if (exact) return nfc(r.name ?? "").toLowerCase() === needle;
    return (r._fullpath ?? "").includes(needle);
  });

  matched.sort((a, b) => {
    if (a.code !== b.code) return a.code < b.code ? -1 : 1;
    return (a.created ?? "") < (b.created ?? "") ? -1 : 1;
  });

  return matched.map(({ _fullpath: _omit, ...rest }) => rest);
}
