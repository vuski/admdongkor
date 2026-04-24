/** timeline/ v2 포맷 클라이언트.
 *  설계: adk-master/.readme/admdongkor/2026-04-25-timeline-meta-size-analysis.md
 *
 *  파일 레이아웃:
 *    timeline/versions.json
 *    timeline/v/<version>/meta.parquet   # columns: code(str), length(u32), level(dict<str>)
 *    timeline/v/<version>/geom.bin       # WKB concat, 같은 순서
 *
 *  meta 엔 이름·area 등이 없다. 이름 검색/라벨은 admdongkor 의 find()(_index.parquet) 사용.
 */

import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { decodeWKB, type DecodedGeometry } from "@/lib/wkb";

export type TimelineLevel = "sido" | "sgg" | "emd";

const TIMELINE_BASE = "/timeline";

export interface VersionsJson {
  versions: string[];
  generated_at: string;
  format_version: number;
}

export interface Range {
  offset: number;
  length: number;
}

/** meta.parquet row. */
interface MetaRow {
  code: string;
  length: number;
  level: TimelineLevel;
}

/** 한 연도의 meta — 코드 → offset/length 매핑 (레벨별).
 *  offset 은 length 누적합으로 로드 시 계산. */
export interface VersionMeta {
  version: string;
  sido: Map<string, Range>;
  sgg: Map<string, Range>;
  emd: Map<string, Range>;
}

/** 조회된 feature. 이름은 meta 에 없으므로 여기에 담지 않는다.
 *  (이름이 필요한 단계 — 라벨/리스트 — 에선 admdongkor find() 로 따로 조달한다.) */
export interface TimelineFeature {
  code: string;
  level: TimelineLevel;
  geometry: DecodedGeometry;
}

/** 매칭된 parent feature 1 개 + 그 자식들. */
export interface TimelineGroup {
  parent: TimelineFeature;
  children: TimelineFeature[];
  /** adm_match 가 준 parent 의 weight (0~1). */
  weight?: number;
  /** children 각자의 weight (동일 순서). */
  childWeights?: number[];
}

/** 한 연도 × 여러 지역의 결과. */
export interface TimelineSlice {
  version: string;
  targetLevel: "sido" | "sgg";
  exists: boolean;
  groups: TimelineGroup[];
  bbox?: [number, number, number, number];
}

// ---------------------------------------------------------------------------
// 캐시
// ---------------------------------------------------------------------------
const versionsCache: { promise?: Promise<VersionsJson> } = {};
const metaCache = new Map<string, Promise<VersionMeta>>();
/** key = `${version}|${offset}-${length}` → ArrayBuffer (geom.bin range). */
const binCache = new Map<string, Promise<ArrayBuffer>>();

export function fetchVersions(): Promise<VersionsJson> {
  if (!versionsCache.promise) {
    versionsCache.promise = fetch(`${TIMELINE_BASE}/versions.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`versions.json ${r.status}`);
        return r.json() as Promise<VersionsJson>;
      })
      .catch((e) => {
        versionsCache.promise = undefined;
        throw e;
      });
  }
  return versionsCache.promise;
}

export function fetchMeta(version: string): Promise<VersionMeta> {
  let p = metaCache.get(version);
  if (p) return p;
  p = (async () => {
    const res = await fetch(`${TIMELINE_BASE}/v/${version}/meta.parquet`);
    if (!res.ok) throw new Error(`meta ${version} ${res.status}`);
    const buffer = await res.arrayBuffer();
    const rows = (await parquetReadObjects({
      file: buffer,
      compressors,
    })) as MetaRow[];

    // offset = 앞 row 들의 length 누적합.
    const sido = new Map<string, Range>();
    const sgg = new Map<string, Range>();
    const emd = new Map<string, Range>();
    let offset = 0;
    for (const r of rows) {
      const len = Number(r.length);
      const rng: Range = { offset, length: len };
      if (r.level === "sido") sido.set(r.code, rng);
      else if (r.level === "sgg") sgg.set(r.code, rng);
      else emd.set(r.code, rng);
      offset += len;
    }
    return { version, sido, sgg, emd };
  })().catch((e) => {
    metaCache.delete(version);
    throw e;
  });
  metaCache.set(version, p);
  return p;
}

function binKey(version: string, range: Range): string {
  return `${version}|${range.offset}-${range.length}`;
}

export function fetchBinRange(
  version: string,
  range: Range,
): Promise<ArrayBuffer> {
  if (range.length === 0) return Promise.resolve(new ArrayBuffer(0));
  const key = binKey(version, range);
  let p = binCache.get(key);
  if (p) return p;
  const url = `${TIMELINE_BASE}/v/${version}/geom.bin`;
  const end = range.offset + range.length - 1;
  p = fetch(url, { headers: { Range: `bytes=${range.offset}-${end}` } })
    .then(async (r) => {
      if (r.status !== 206 && r.status !== 200) {
        throw new Error(`geom ${version} ${r.status}`);
      }
      const buf = await r.arrayBuffer();
      // 200 으로 전체가 내려온 경우 잘라냄 (hosting 이 Range 지원 안 할 때).
      if (r.status === 200 && buf.byteLength > range.length) {
        return buf.slice(range.offset, range.offset + range.length);
      }
      return buf;
    })
    .catch((e) => {
      binCache.delete(key);
      throw e;
    });
  binCache.set(key, p);
  return p;
}

/** level 의 code 하나를 geometry 로. */
async function fetchFeature(
  version: string,
  meta: VersionMeta,
  level: TimelineLevel,
  code: string,
): Promise<TimelineFeature | null> {
  const rng =
    level === "sido"
      ? meta.sido.get(code)
      : level === "sgg"
        ? meta.sgg.get(code)
        : meta.emd.get(code);
  if (!rng) return null;
  const buf = await fetchBinRange(version, rng);
  return { code, level, geometry: decodeWKB(buf, 0) };
}

/** 여러 code 를 batch range (min offset ~ max offset+length) 로 한 번에 fetch.
 *  반환은 입력 codes 순서. */
async function fetchFeaturesBatch(
  version: string,
  meta: VersionMeta,
  level: TimelineLevel,
  codes: string[],
): Promise<TimelineFeature[]> {
  if (codes.length === 0) return [];
  const table =
    level === "sido" ? meta.sido : level === "sgg" ? meta.sgg : meta.emd;
  const ranges: (Range | undefined)[] = codes.map((c) => table.get(c));
  const valid = ranges.filter((r): r is Range => r !== undefined);
  if (valid.length === 0) return [];
  let minOff = Infinity;
  let maxEnd = -Infinity;
  for (const r of valid) {
    if (r.offset < minOff) minOff = r.offset;
    const end = r.offset + r.length;
    if (end > maxEnd) maxEnd = end;
  }
  const blob = await fetchBinRange(version, {
    offset: minOff,
    length: maxEnd - minOff,
  });
  const out: TimelineFeature[] = [];
  for (let i = 0; i < codes.length; i++) {
    const r = ranges[i];
    if (!r) continue;
    const geom = decodeWKB(blob, r.offset - minOff);
    out.push({ code: codes[i]!, level, geometry: geom });
  }
  return out;
}

/** 주어진 sgg 코드 하나의 하위 emd 코드 목록. meta 의 emd keys 에서 prefix 매칭.
 *  - surrogate("name:...") 인 경우: prefix 가 `${sggcode}|` 로 시작해야 한다.
 *  - 일반 숫자 코드: emd 10자리가 sgg 5자리로 시작해야 한다. */
function childEmdCodes(meta: VersionMeta, sggcode: string): string[] {
  const out: string[] = [];
  if (sggcode.startsWith("name:")) {
    const prefix = `${sggcode}|`;
    for (const k of meta.emd.keys()) {
      if (k.startsWith(prefix)) out.push(k);
    }
  } else {
    for (const k of meta.emd.keys()) {
      if (k.length >= 5 && k.startsWith(sggcode)) out.push(k);
    }
  }
  return out;
}

/** 주어진 sido 코드 하나의 하위 sgg 코드 목록. */
function childSggCodes(meta: VersionMeta, sidocode: string): string[] {
  const out: string[] = [];
  if (sidocode.startsWith("name:")) {
    const prefix = `${sidocode}|`;
    for (const k of meta.sgg.keys()) {
      if (k.startsWith(prefix)) out.push(k);
    }
  } else {
    for (const k of meta.sgg.keys()) {
      if (k.length >= 2 && k.startsWith(sidocode)) out.push(k);
    }
  }
  return out;
}

/** 한 (version, level) 에서 여러 code 를 동시에 조회. 각 code 가 하나의 group. */
export async function fetchTimelineSlice(
  version: string,
  level: "sido" | "sgg",
  codes: string[],
  weights?: number[],
  childWeightByCode?: Map<string, number>,
): Promise<TimelineSlice> {
  const meta = await fetchMeta(version);
  if (codes.length === 0) {
    return { version, targetLevel: level, exists: false, groups: [] };
  }

  const groups = await Promise.all(
    codes.map(async (code, idx): Promise<TimelineGroup | null> => {
      if (level === "sido") {
        // parent: sido 하나. children: 그 sido 의 모든 sgg (렌더는 sgg 레벨).
        const parent = await fetchFeature(version, meta, "sido", code);
        if (!parent) return null;
        const childCodes = childSggCodes(meta, code);
        const childFeats = await fetchFeaturesBatch(
          version,
          meta,
          "sgg",
          childCodes,
        );
        const childWeights = childWeightByCode
          ? childFeats.map((c) => childWeightByCode.get(c.code) ?? 0)
          : undefined;
        return { parent, children: childFeats, weight: weights?.[idx], childWeights };
      }
      // sgg: parent = sgg 하나, children = 그 sgg 의 emd 들.
      const parent = await fetchFeature(version, meta, "sgg", code);
      if (!parent) return null;
      const childCodes = childEmdCodes(meta, code);
      const childFeats = await fetchFeaturesBatch(
        version,
        meta,
        "emd",
        childCodes,
      );
      const childWeights = childWeightByCode
        ? childFeats.map((c) => childWeightByCode.get(c.code) ?? 0)
        : undefined;
      return { parent, children: childFeats, weight: weights?.[idx], childWeights };
    }),
  );

  const validGroups = groups.filter((g): g is TimelineGroup => g !== null);
  if (validGroups.length === 0) {
    return { version, targetLevel: level, exists: false, groups: [] };
  }
  const allFeats: TimelineFeature[] = [];
  for (const g of validGroups) {
    allFeats.push(g.parent);
    allFeats.push(...g.children);
  }
  return {
    version,
    targetLevel: level,
    exists: true,
    groups: validGroups,
    bbox: computeBBox(allFeats),
  };
}

function computeBBox(
  feats: TimelineFeature[],
): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const f of feats) {
    const polys =
      f.geometry.type === "Polygon"
        ? [f.geometry.coordinates]
        : f.geometry.coordinates;
    for (const poly of polys) {
      const ring = poly[0];
      if (!ring) continue;
      for (const [x, y] of ring) {
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  return [minX, minY, maxX, maxY];
}
