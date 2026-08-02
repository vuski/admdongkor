export type Level = "emd" | "sgg" | "sido";

export interface GetOptions {
  /** 기본 false (light, 단순화). true 면 원본 해상도 parquet. */
  detail?: boolean;
  /** fetch 기본 URL 오버라이드. 테스트 / 자체 호스팅용. */
  baseUrl?: string;
  /** 커스텀 fetch (Node 18+ 는 글로벌 fetch 사용 가능). */
  fetch?: typeof fetch;
  /** AbortSignal. */
  signal?: AbortSignal;
}

export interface EmdProperties {
  emd7: string | null;
  emd8: string | null;
  emdcd: string | null;
  emdnm: string;
  sggcd: string | null;
  sggnm: string | null;
  sidocd: string | null;
  sidonm: string;
  area: number;
}

export interface SggProperties {
  sggcd: string | null;
  sggnm: string;
  sidocd: string | null;
  sidonm: string;
  area: number;
}

export interface SidoProperties {
  sidocd: string | null;
  sidonm: string;
  area: number;
}

export type AdmProperties = EmdProperties | SggProperties | SidoProperties;

export interface AdmFeature<P = AdmProperties> {
  type: "Feature";
  properties: P;
  geometry: GeoJSON.Polygon | GeoJSON.MultiPolygon;
}

export interface AdmFeatureCollection<P = AdmProperties> {
  type: "FeatureCollection";
  /**
   * 이 데이터의 좌표계.
   * - `"EPSG:4326"` — `detail: false` (light). Leaflet/MapLibre 에 바로 사용 가능.
   * - `"EPSG:5179"` — `detail: true` (원본). UTM-K 미터 좌표라 재투영이 필요하다.
   */
  crs?: "EPSG:4326" | "EPSG:5179";
  features: AdmFeature<P>[];
}
