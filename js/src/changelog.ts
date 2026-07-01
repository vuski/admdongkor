/** 인덱스 parquet 수정 이력 + 현재 data_version 조회. */

import { DEFAULT_INDEX_BASE, type LoaderOptions } from "./_index-loader.js";

export interface ChangelogEntry {
  /** YYYY.MM.DD 형식의 data_version 태그. */
  version: string;
  /** 사람이 읽는 한 줄 요약. */
  changes: string;
}

export interface Manifest {
  data_version: string;
  schema_version: string;
  min_lib_version: string;
  created_at: string;
  /** 사용 가능한 버전 키 목록 (오름차순). 옛 배포에는 없을 수 있음 (optional). */
  versions?: string[];
  history: ChangelogEntry[];
  files: Record<string, { size: number; sha256: string }>;
}

let _manifestCache: Promise<Manifest> | null = null;

export function clearManifestCache(): void {
  _manifestCache = null;
}

/** 원격 manifest.json 을 받아 파싱. 프로세스 내 메모리 캐시. */
export async function fetchManifest(
  opts: LoaderOptions = {},
): Promise<Manifest> {
  const base = opts.baseUrl ?? DEFAULT_INDEX_BASE;
  if (_manifestCache) return _manifestCache;
  const fetchFn = opts.fetch ?? fetch;
  const p = (async () => {
    const res = await fetchFn(`${base}/manifest.json`, { signal: opts.signal });
    if (!res.ok) {
      throw new Error(
        `failed to fetch manifest: ${res.status} ${res.statusText}`,
      );
    }
    return (await res.json()) as Manifest;
  })();
  _manifestCache = p;
  try {
    return await p;
  } catch (e) {
    _manifestCache = null;
    throw e;
  }
}

/** 현재 원격 data_version (예: "2026.04.25"). 네트워크 실패 시 throw. */
export async function dataVersion(opts: LoaderOptions = {}): Promise<string> {
  const m = await fetchManifest(opts);
  return m.data_version;
}

/** 인덱스 수정 이력. 최신이 [0]. */
export async function changelog(
  opts: LoaderOptions = {},
): Promise<ChangelogEntry[]> {
  const m = await fetchManifest(opts);
  return m.history ?? [];
}
