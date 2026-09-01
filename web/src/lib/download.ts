/**
 * 다운로드 파이프라인 — 전부 브라우저에서 처리한다 (서버 없음).
 *
 * 세 축이 서로 독립이다:
 *   포맷   parquet / geojson / gpkg
 *   해상도 원본(detail) / 단순화(light)
 *   좌표계 EPSG:4326(원본) 외 한국 좌표계 + 사용자 proj4
 *
 * parquet 을 **원본 좌표계 그대로** 받을 때만 바이트를 그대로 통과시키고,
 * 그 외에는 파싱 → (변환) → 재직렬화한다.
 */

import { zip } from "fflate";
import { get, getParquet } from "admdongkor";
import type { Level } from "admdongkor";
import type { FeatureCollection, Geometry, Position } from "geojson";

import { connectorFeatures, pullInIslands } from "./island-move";
import {
  CUSTOM_CRS,
  SOURCE_CRS,
  crsSrsId,
  crsSuffix,
  makeConverter,
  type Converter,
} from "./crs";

export type DownloadFormat = "parquet" | "geojson" | "gpkg";

/**
 * 해상도 3단계.
 *   detail  원본
 *   light   기본 단순화 — 읍면동 18.7% 후 dissolve (배포된 parquet)
 *   super   많이 단순화 — 시군구 2.7%, 시도는 그걸 dissolve. 브라우저에서 계산.
 *           읍면동은 만들 수 없다 (시군구부터 다시 단순화하므로).
 */
export type Resolution = "detail" | "light" | "super";

/**
 * 섬 당겨오기를 수행하는 좌표계.
 * 평행이동은 좌표계마다 결과가 달라 하나로 고정해야 한다. 5179(UTM-K) 가
 * 한국 영역에서 면적 왜곡이 가장 작다 (island-move.ts 주석의 실측 비교).
 */
const MOVE_CRS = "EPSG:5179";

export const FORMAT_LABEL: Record<DownloadFormat, string> = {
  parquet: "Parquet",
  geojson: "GeoJSON",
  gpkg: "GeoPackage",
};

export const FORMAT_NOTE: Record<DownloadFormat, string> = {
  parquet: "geo-parquet. 분석·재가공에 가장 가볍다.",
  geojson: "어디서나 열리지만 용량이 가장 크다.",
  gpkg: "QGIS·ArcGIS 에서 바로 열림. 변환에 시간이 걸린다.",
};

const EXT: Record<DownloadFormat, string> = {
  parquet: "parquet",
  geojson: "geojson",
  gpkg: "gpkg",
};

export interface DownloadProgress {
  ratio: number;
  message: string;
}

export interface DownloadRequest {
  versionKey: string;
  levels: Level[];
  format: DownloadFormat;
  /** true = 원본 해상도, false = 단순화(light). */
  detail: boolean;
  /** "많이 단순화" — 시군구 2.7% 재단순화. 읍면동 미선택 시에만 가능. */
  superSimplify?: boolean;
  /** 대상 좌표계 키 (`EPSG:5179` 또는 CUSTOM_CRS). */
  crs: string;
  /** crs === CUSTOM_CRS 일 때 쓸 proj4 문자열. */
  customProj4?: string;
  /** 섬 지역(백령·연평·흑산·제주·울릉·독도) 을 육지 가까이 당겨온다. */
  pullIslands?: boolean;
  onProgress?: (p: DownloadProgress) => void;
  signal?: AbortSignal;
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw new DOMException("aborted", "AbortError");
}

/** GeoJSON geometry 의 모든 좌표를 in-place 변환. */
function transformGeometry(g: Geometry, conv: Converter): void {
  const walk = (c: unknown): void => {
    if (!Array.isArray(c)) return;
    if (typeof c[0] === "number" && typeof c[1] === "number") {
      const [x, y] = conv([c[0] as number, c[1] as number]);
      (c as Position)[0] = x;
      (c as Position)[1] = y;
      return;
    }
    for (const sub of c) walk(sub);
  };
  if ("coordinates" in g) walk(g.coordinates);
  else if (g.type === "GeometryCollection") {
    for (const sub of g.geometries) transformGeometry(sub, conv);
  }
}

function transformFeatureCollection(
  fc: FeatureCollection,
  conv: Converter,
): void {
  for (const f of fc.features) {
    if (f.geometry) transformGeometry(f.geometry, conv);
  }
}

/**
 * 변환된 FeatureCollection 을 geo-parquet 으로 쓰는 대신,
 * **좌표계를 바꾼 parquet 요청은 GeoJSON 으로 유도**한다.
 *
 * 이유: 브라우저에서 parquet 을 *쓰는* 것은 hyparquet 범위 밖이다
 * (hyparquet 는 reader 전용). 잘못된 파일을 만들어 내보내느니
 * 명확히 막고 대안을 안내한다.
 */
export function parquetNeedsSourceCrs(
  format: DownloadFormat,
  crs: string,
  detail: boolean,
): boolean {
  // parquet 은 바이트 통과라 저장된 좌표계 그대로만 나간다.
  // 원본은 EPSG:5179, light 는 EPSG:4326 으로 저장돼 있다.
  return format === "parquet" && crs !== nativeParquetCrs(detail);
}

/**
 * parquet + 섬 당겨오기는 불가능하다.
 *
 * 섬 이동은 지오메트리를 고쳐 **다시 써야** 하는데, 브라우저에서 parquet 을
 * 쓰는 수단이 없다 (hyparquet 은 reader 전용). 바이트 통과 경로로는 이동된
 * 결과를 낼 수 없으므로 UI 에서 미리 막고 대안을 안내한다.
 */
export function parquetBlocksIslandPull(
  format: DownloadFormat,
  pullIslands: boolean,
): boolean {
  return format === "parquet" && pullIslands;
}

/**
 * "많이 단순화" 는 **시군구부터 다시 단순화**하므로 읍면동을 만들 수 없다.
 * (기본 light 는 읍면동을 18.7% 로 줄인 뒤 dissolve 한 것이고, 여기서 더
 *  줄이려면 읍면동 경계 자체를 버려야 한다.)
 */
export function superBlocksEmd(
  superSimplify: boolean,
  levels: Level[],
): boolean {
  return superSimplify && levels.includes("emd");
}

/** 많이 단순화는 지오메트리를 다시 써야 하므로 parquet 통과 경로를 못 쓴다. */
export function parquetBlocksSuper(
  format: DownloadFormat,
  superSimplify: boolean,
): boolean {
  return format === "parquet" && superSimplify;
}

/** parquet 파일이 실제로 저장하고 있는 좌표계. */
export function nativeParquetCrs(detail: boolean): string {
  return detail ? "EPSG:5179" : "EPSG:4326";
}

/**
 * FeatureCollection 을 목표 좌표계로 옮기고(필요 시 섬 당겨오기 포함)
 * 선택한 포맷 바이트로 직렬화한다. buildOne 과 buildSuper 가 공유한다.
 */
async function serializeFc(
  fc: FeatureCollection,
  level: Level,
  versionKey: string,
  format: DownloadFormat,
  crs: string,
  customProj4: string | undefined,
  pullIslands: boolean,
  sourceCrs: string,
  signal?: AbortSignal,
  /** "단순화(많이)" 여부 — 사라지는 섬의 지시선을 빼기 위해 필요. */
  superSimplify = false,
): Promise<Uint8Array> {
  // 섬 당겨오기는 **EPSG:5179 기준 이동량**이라 5179 인 상태에서 해야 한다.
  // 평행이동 결과는 좌표계마다 다르고, 투영이 비선형이라 한 좌표계에서 잰
  // (dx,dy) 를 다른 좌표계에 그대로 쓸 수 없다. 세 후보 실측 비교 결과
  // 5179 가 면적 왜곡이 가장 작아 채택했다 (island-move.ts 주석 참조).
  //
  // 비용: 원본(detail)은 이미 5179 라 변환이 없고, light(4326) 만 왕복한다.
  let connectors: FeatureCollection | null = null;
  if (pullIslands) {
    const to5179 = await makeConverter(MOVE_CRS, undefined, sourceCrs);
    if (to5179) transformFeatureCollection(fc, to5179);
    pullInIslands(fc);
    connectors = {
      type: "FeatureCollection",
      features: connectorFeatures(superSimplify),
    };
    // 이제 둘 다 5179 다. 목표 좌표계 변환은 5179 에서 출발.
    const conv = await makeConverter(crs, customProj4, MOVE_CRS);
    if (conv) {
      transformFeatureCollection(fc, conv);
      transformFeatureCollection(connectors, conv);
    }
    // GeoJSON 은 레이어 개념이 없으므로 한 컬렉션에 합친다.
    // (properties.kind === "island_connector" 로 구분 가능)
    if (format === "geojson") {
      fc.features.push(...connectors.features);
      connectors = null;
    }
  } else {
    const conv = await makeConverter(crs, customProj4, sourceCrs);
    if (conv) transformFeatureCollection(fc, conv);
  }
  throwIfAborted(signal);

  if (format === "geojson") {
    return new TextEncoder().encode(JSON.stringify(fc));
  }

  // GeoPackage — sql.js(WASM) 를 이 시점에 처음 로드한다.
  const { featureCollectionToGpkg } = await import("./gpkg");
  throwIfAborted(signal);
  return featureCollectionToGpkg(fc, {
    tableName: `${level}_${versionKey}`,
    srsId: crsSrsId(crs),
    // 지시선은 별도 레이어로 — QGIS 에서 따로 일점쇄선 스타일을 줄 수 있다.
    extraLayers: connectors
      ? [{ tableName: "island_connector", fc: connectors }]
      : undefined,
  });
}


async function buildOne(
  level: Level,
  versionKey: string,
  format: DownloadFormat,
  detail: boolean,
  crs: string,
  customProj4: string | undefined,
  pullIslands: boolean,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  if (format === "parquet" && !pullIslands) {
    // 좌표 변환도 섬 이동도 없을 때만 도달한다. 바이트 그대로 통과.
    const buf = await getParquet(versionKey, level, { detail, signal });
    return new Uint8Array(buf);
  }

  const fc = (await get(versionKey, level, {
    detail,
    signal,
  })) as unknown as FeatureCollection & { crs?: string };
  throwIfAborted(signal);

  // 출발 좌표계는 파일마다 다르다: light=4326, 원본=5179.
  // get() 이 알려주는 값을 그대로 믿되, 없으면 detail 로 추정한다.
  const sourceCrs = fc.crs ?? (detail ? "EPSG:5179" : SOURCE_CRS);

  return serializeFc(fc, level, versionKey, format, crs, customProj4,
    pullIslands, sourceCrs, signal);
}

/**
 * "많이 단순화" 경로.
 *
 * 시군구를 2.7% 로 한 번만 단순화하고, 시도가 필요하면 **그 결과를 dissolve**
 * 한다. 시도를 따로 단순화하면 시군구와 경계가 어긋나 겹쳤을 때 틈이 생긴다.
 *
 * 반환: [level, 직렬화된 바이트] 목록.
 */
async function buildSuper(
  levels: Level[],
  versionKey: string,
  format: DownloadFormat,
  crs: string,
  customProj4: string | undefined,
  pullIslands: boolean,
  onProgress: ((p: DownloadProgress) => void) | undefined,
  signal?: AbortSignal,
): Promise<[Level, Uint8Array][]> {
  const { supersimplifySgg, dissolveToSido } = await import("./supersimplify");
  throwIfAborted(signal);

  // 항상 light 시군구에서 출발한다 (원본은 무겁고, 어차피 크게 줄일 것이라
  // 출발점 해상도가 결과에 거의 영향을 주지 않는다).
  const src = (await get(versionKey, "sgg", {
    detail: false,
    signal,
  })) as unknown as FeatureCollection;
  throwIfAborted(signal);

  onProgress?.({ ratio: 0.2, message: "시군구 단순화 중…" });
  const sgg = await supersimplifySgg(src);
  throwIfAborted(signal);

  const out: [Level, FeatureCollection][] = [];
  if (levels.includes("sgg")) out.push(["sgg", sgg]);
  if (levels.includes("sido")) {
    onProgress?.({ ratio: 0.5, message: "시도 병합 중…" });
    out.push(["sido", await dissolveToSido(sgg)]);
  }
  throwIfAborted(signal);

  // 좌표 변환·섬 당겨오기는 buildOne 과 같은 순서로 (5179 에서 이동).
  const res: [Level, Uint8Array][] = [];
  for (const [lv, fc] of out) {
    throwIfAborted(signal);
    onProgress?.({ ratio: 0.7, message: `${lv} 내보내는 중…` });
    res.push([
      lv,
      await serializeFc(fc, lv, versionKey, format, crs, customProj4,
        pullIslands, SOURCE_CRS, signal, true),
    ]);
  }
  return res;
}


function zipAsync(files: Record<string, Uint8Array>): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    // parquet 은 이미 snappy 압축이라 재압축 이득이 거의 없다 → store.
    // GeoJSON 텍스트·GeoPackage(SQLite) 는 잘 줄어든다 → level 6.
    const opts = Object.fromEntries(
      Object.keys(files).map((name) => [
        name,
        { level: name.endsWith(".parquet") ? (0 as const) : (6 as const) },
      ]),
    );
    zip(files, opts, (err, data) => (err ? reject(err) : resolve(data)));
  });
}

function fileNameFor(
  level: Level,
  versionKey: string,
  format: DownloadFormat,
  detail: boolean,
  crs: string,
  pullIslands: boolean,
  superSimplify: boolean,
): string {
  const res = superSimplify ? "_super" : detail ? "" : "_light";
  const proj = crs === SOURCE_CRS ? "" : `_${crsSuffix(crs)}`;
  // 원본과 헷갈리면 안 되므로 파일명에 명시한다.
  const isl = pullIslands ? "_pulled" : "";
  return `${level}_${versionKey}${res}${proj}${isl}.${EXT[format]}`;
}

export async function buildDownload({
  versionKey,
  levels,
  format,
  detail,
  crs,
  customProj4,
  pullIslands = false,
  superSimplify = false,
  onProgress,
  signal,
}: DownloadRequest): Promise<{ blob: Blob; filename: string }> {
  if (levels.length === 0) throw new Error("경계 단위를 하나 이상 선택하세요.");
  if (parquetBlocksIslandPull(format, pullIslands)) {
    throw new Error(
      "Parquet 은 섬 지역 당겨오기와 함께 받을 수 없습니다 " +
        "(브라우저에서 parquet 을 다시 쓸 수 없음). " +
        "GeoJSON 또는 GeoPackage 를 선택하세요.",
    );
  }
  if (parquetBlocksSuper(format, superSimplify)) {
    throw new Error(
      "Parquet 은 '단순화(많이)' 와 함께 받을 수 없습니다 " +
        "(브라우저에서 parquet 을 다시 쓸 수 없음). " +
        "GeoJSON 또는 GeoPackage 를 선택하세요.",
    );
  }
  if (superBlocksEmd(superSimplify, levels)) {
    throw new Error(
      "'단순화(많이)' 는 시군구부터 다시 단순화하므로 읍면동을 만들 수 없습니다. " +
        "읍면동을 빼거나 '단순화(보통)' 을 선택하세요.",
    );
  }
  if (!superSimplify && parquetNeedsSourceCrs(format, crs, detail)) {
    throw new Error(
      `Parquet 은 저장된 좌표계(${nativeParquetCrs(detail)})로만 받을 수 있습니다. ` +
        "다른 좌표계가 필요하면 GeoJSON 또는 GeoPackage 를 선택하세요.",
    );
  }

  onProgress?.({ ratio: 0, message: "준비 중…" });
  throwIfAborted(signal);

  const files: Record<string, Uint8Array> = {};
  const steps = levels.length + 1;

  if (superSimplify) {
    // 시군구를 한 번만 단순화하고, 시도는 **그 결과를 dissolve** 한다.
    // 각자 단순화하면 두 레이어 경계가 어긋나 겹쳤을 때 틈이 생긴다.
    onProgress?.({ ratio: 0, message: "시군구 단순화 중…" });
    const built = await buildSuper(
      levels,
      versionKey,
      format,
      crs,
      customProj4,
      pullIslands,
      onProgress,
      signal,
    );
    for (const [lv, bytes] of built) {
      files[fileNameFor(lv, versionKey, format, false, crs, pullIslands, true)] =
        bytes;
    }
  } else {
    for (let i = 0; i < levels.length; i++) {
      const level = levels[i]!;
      throwIfAborted(signal);
      onProgress?.({ ratio: i / steps, message: `${level} 처리 중…` });
      files[
        fileNameFor(level, versionKey, format, detail, crs, pullIslands, false)
      ] = await buildOne(
        level,
        versionKey,
        format,
        detail,
        crs,
        customProj4,
        pullIslands,
        signal,
      );
    }
  }

  throwIfAborted(signal);
  onProgress?.({ ratio: levels.length / steps, message: "zip 만드는 중…" });
  const zipped = await zipAsync(files);

  onProgress?.({ ratio: 1, message: "완료" });
  const res = detail ? "" : "_light";
  const proj = crs === SOURCE_CRS ? "" : `_${crsSuffix(crs)}`;
  return {
    blob: new Blob([zipped.slice().buffer as ArrayBuffer], {
      type: "application/zip",
    }),
    filename: `admdongkor_${versionKey}${res}${proj}_${EXT[format]}.zip`,
  };
}

export function saveBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

/** 대략적 용량 안내 (MB). 최신 시점 관측치 기준 — 정확한 값이 아니다. */
const SIZE_DETAIL: Record<DownloadFormat, Record<Level, number>> = {
  parquet: { emd: 11.2, sgg: 3.6, sido: 1.8 },
  geojson: { emd: 48.0, sgg: 20.0, sido: 10.0 },
  gpkg: { emd: 30.0, sgg: 13.0, sido: 6.5 },
};
const SIZE_LIGHT: Record<DownloadFormat, Record<Level, number>> = {
  parquet: { emd: 2.4, sgg: 1.0, sido: 0.5 },
  geojson: { emd: 12.0, sgg: 5.0, sido: 2.5 },
  gpkg: { emd: 3.6, sgg: 1.5, sido: 0.6 },
};

/**
 * "많이 단순화" 산출물 크기 (실측, GeoJSON 기준 MB).
 * 정점이 5.6% 로 줄어 light 대비 sgg 2.73 → 0.18 MB.
 * gpkg 는 SQLite 오버헤드로 조금 크고, parquet 은 이 모드에서 불가.
 */
const SIZE_SUPER: Record<DownloadFormat, Partial<Record<Level, number>>> = {
  geojson: { sgg: 0.18, sido: 0.05 },
  gpkg: { sgg: 0.25, sido: 0.1 },
  parquet: {},
};

export function estimateMb(
  format: DownloadFormat,
  detail: boolean,
  levels: Level[],
  superSimplify = false,
): number {
  const table = superSimplify ? SIZE_SUPER : detail ? SIZE_DETAIL : SIZE_LIGHT;
  return levels.reduce((sum, l) => sum + (table[format][l] ?? 0), 0);
}

export { CUSTOM_CRS, SOURCE_CRS };
