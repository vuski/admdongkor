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
  /** 저장 CRS 는 EPSG:4326. Leaflet/MapLibre 에서 바로 사용 가능. */
  features: AdmFeature<P>[];
}
