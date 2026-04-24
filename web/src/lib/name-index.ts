/** admdongkor 의 `_index.parquet` 를 직접 읽어 (version, level, code) → 이름·상위 이름 맵 제공.
 *  타임라인 뷰의 라벨/파스텔 색 할당 등에서 이름·sidocd 가 필요하지만
 *  timeline/ meta 엔 이름이 없으므로 여기서 조달한다.
 *
 *  _index.parquet 는 admdongkor 라이브러리가 이미 `find()` 에서 쓰고 있다.
 *  같은 URL 을 그대로 fetch (브라우저 캐시 공유). */

import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

const INDEX_URL =
  "https://raw.githubusercontent.com/vuski/admdongkor/master/lib/src/admdongkor/data/_index.parquet";

export interface NameRow {
  version_key: string;
  level: "sido" | "sgg" | "emd";
  code: string;
  name: string;
  sidonm?: string;
  sggnm?: string;
  sidocd?: string;
  sggcd?: string;
}

let rowsPromise: Promise<NameRow[]> | null = null;

export function loadNameIndex(): Promise<NameRow[]> {
  if (rowsPromise) return rowsPromise;
  rowsPromise = (async () => {
    const res = await fetch(INDEX_URL);
    if (!res.ok) throw new Error(`_index.parquet ${res.status}`);
    const buf = await res.arrayBuffer();
    const rows = (await parquetReadObjects({
      file: buf,
      compressors,
    })) as unknown as Array<Record<string, unknown>>;
    return rows.map((r) => ({
      version_key: String(r.version_key),
      level: String(r.level) as "sido" | "sgg" | "emd",
      code: r.code == null ? "" : String(r.code),
      name: String(r.name ?? ""),
      sidonm: r.sidonm == null ? undefined : String(r.sidonm),
      sggnm: r.sggnm == null ? undefined : String(r.sggnm),
      sidocd: r.sidocd == null ? undefined : String(r.sidocd),
      sggcd: r.sggcd == null ? undefined : String(r.sggcd),
    }));
  })().catch((e) => {
    rowsPromise = null;
    throw e;
  });
  return rowsPromise;
}

/** 한 버전의 (level, code) -> NameRow 맵. version 이 _index 에 없으면 빈 맵. */
export async function loadVersionNames(
  version: string,
): Promise<{
  sido: Map<string, NameRow>;
  sgg: Map<string, NameRow>;
  emd: Map<string, NameRow>;
}> {
  const rows = await loadNameIndex();
  const sido = new Map<string, NameRow>();
  const sgg = new Map<string, NameRow>();
  const emd = new Map<string, NameRow>();
  for (const r of rows) {
    if (r.version_key !== version) continue;
    if (!r.code) continue;
    if (r.level === "sido") sido.set(r.code, r);
    else if (r.level === "sgg") sgg.set(r.code, r);
    else emd.set(r.code, r);
  }
  return { sido, sgg, emd };
}
