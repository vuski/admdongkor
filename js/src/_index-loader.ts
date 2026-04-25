import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

// 인덱스 parquet 의 canonical URL. Python lib 과 동일 경로를 공유.
// 이 URL 은 **영구 stable** — 옮기면 이미 배포된 번들이 전부 깨진다.
// manifest.json 도 같은 경로에서 받는다 (changelog()).
export const DEFAULT_INDEX_BASE =
  "https://raw.githubusercontent.com/vuski/admdongkor/master/dist/data";

export interface LoaderOptions {
  baseUrl?: string;
  fetch?: typeof fetch;
  signal?: AbortSignal;
}

const cache = new Map<string, Promise<unknown[]>>();

export function clearLoaderCache(): void {
  cache.clear();
}

export async function loadIndexFile<T>(
  filename: string,
  opts: LoaderOptions = {},
): Promise<T[]> {
  const base = opts.baseUrl ?? DEFAULT_INDEX_BASE;
  const url = `${base}/${filename}`;
  const existing = cache.get(url);
  if (existing) return existing as Promise<T[]>;
  const fetchFn = opts.fetch ?? fetch;
  const p = (async () => {
    const res = await fetchFn(url, { signal: opts.signal });
    if (!res.ok) {
      throw new Error(
        `failed to fetch ${url}: ${res.status} ${res.statusText}`,
      );
    }
    const buffer = await res.arrayBuffer();
    const rows = (await parquetReadObjects({
      file: buffer,
      compressors,
    })) as T[];
    return rows;
  })();
  cache.set(url, p);
  try {
    return await p;
  } catch (e) {
    cache.delete(url);
    throw e;
  }
}
