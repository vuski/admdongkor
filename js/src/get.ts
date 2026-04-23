import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

import type {
  AdmFeature,
  AdmFeatureCollection,
  AdmProperties,
  GetOptions,
  Level,
} from "./types.js";
import { VERSIONS } from "./versions.js";

const DEFAULT_BASE_URL =
  "https://raw.githubusercontent.com/vuski/admdongkor/master/parquet";

const LEVELS: readonly Level[] = ["emd", "sgg", "sido"] as const;

function validateArgs(key: string, level: Level): void {
  if (typeof key !== "string") {
    throw new TypeError(
      `key must be a version key string like '20250401', got ${typeof key}. ` +
        "Use versions() to see available keys.",
    );
  }
  if (!(VERSIONS as readonly string[]).includes(key)) {
    throw new Error(
      `unknown version key: ${JSON.stringify(key)}. ` +
        "Use versions() to see available keys.",
    );
  }
  if (!LEVELS.includes(level)) {
    throw new Error(
      `level must be one of ${JSON.stringify(LEVELS)}, got ${JSON.stringify(level)}`,
    );
  }
}

function urlFor(key: string, level: Level, detail: boolean, baseUrl: string): string {
  return detail
    ? `${baseUrl}/${level}_${key}.parquet`
    : `${baseUrl}/simplified/${level}_${key}_light.parquet`;
}

/** 원본 parquet 파일을 ArrayBuffer 로 반환. 파싱하지 않음.
 *
 * 용도:
 * - Web Worker 로 transferable 전달해서 main thread 부담 최소화
 * - IndexedDB / Cache API 에 바이트 그대로 저장
 * - parquet-wasm, @geoarrow/deck.gl-layers 등에 직접 투입
 *
 * Geometry 는 WKB, CRS 는 EPSG:4326, 압축은 snappy. geo-parquet spec 호환.
 */
export async function getParquet(
  key: string,
  level: Level = "emd",
  options: GetOptions = {},
): Promise<ArrayBuffer> {
  validateArgs(key, level);
  const detail = options.detail ?? false;
  const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
  const fetchFn = options.fetch ?? fetch;
  const url = urlFor(key, level, detail, baseUrl);

  const response = await fetchFn(url, { signal: options.signal });
  if (!response.ok) {
    throw new Error(
      `failed to fetch ${url}: ${response.status} ${response.statusText}`,
    );
  }
  return response.arrayBuffer();
}

/** 지도 데이터를 GeoJSON `FeatureCollection` 으로 반환.
 *
 * 내부적으로 `getParquet()` 로 받은 parquet 바이트를 파싱해서 GeoJSON 으로 변환.
 * deck.gl `GeoJsonLayer`, Leaflet `L.geoJSON`, MapLibre `GeoJSONSource` 에 바로 투입 가능.
 *
 * CRS 는 EPSG:4326 (WGS84). geometry 는 `Polygon` 또는 `MultiPolygon`.
 */
export async function get(
  key: string,
  level: Level = "emd",
  options: GetOptions = {},
): Promise<AdmFeatureCollection> {
  const buffer = await getParquet(key, level, options);

  const rows = await parquetReadObjects({
    file: buffer,
    compressors,
  });

  const features: AdmFeature[] = rows.map((row) => {
    const { geometry, ...rest } = row as Record<string, unknown>;
    return {
      type: "Feature",
      properties: rest as unknown as AdmProperties,
      geometry: geometry as GeoJSON.Polygon | GeoJSON.MultiPolygon,
    };
  });

  return { type: "FeatureCollection", features };
}
