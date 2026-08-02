/**
 * 한국에서 쓰이는 좌표계 정의 + 변환.
 *
 * 출처: https://www.osgeo.kr/17 (OSGeo 한국어 지부, 한국 좌표계 정리)
 *
 * proj4 문자열을 그대로 싣는다 — EPSG 코드만으로는 **보정 원점**(5173~5178)
 * 처럼 같은 이름이 여러 정의를 갖는 경우를 구분할 수 없고, Bessel 계열은
 * `towgs84` 7-파라미터가 붙어야 WGS84 와 맞물린다.
 *
 * proj4js 는 이 모듈을 실제로 쓸 때 dynamic import 한다 (초기 번들 제외).
 */

export interface CrsDef {
  /** 표시용 라벨. */
  label: string;
  /** proj4 정의 문자열. */
  proj4: string;
  /** 파일명·GPKG srs_id 에 쓸 EPSG 코드. 비표준이면 null. */
  epsg: number | null;
  /** 묶음 (UI 그룹핑용). */
  group: string;
  /** 도 단위면 true (m 아님). */
  degrees?: boolean;
}

/**
 * light parquet 의 저장 좌표계. UI 기본값이자 "변환 없음" 기준.
 *
 * **원본(detail) parquet 은 EPSG:5179 로 저장돼 있다** — `get()` 이 돌려주는
 * `crs` 필드로 판별해서 변환 출발점을 정해야 한다.
 */
export const SOURCE_CRS = "EPSG:4326";

export const CRS_DEFS: Record<string, CrsDef> = {
  // ── 경위도 ──
  "EPSG:4326": {
    label: "WGS84 경위도",
    proj4: "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
    epsg: 4326,
    group: "경위도",
    degrees: true,
  },
  "EPSG:4737": {
    label: "Korean 2000 경위도 (GRS80)",
    proj4: "+proj=longlat +ellps=GRS80 +no_defs",
    epsg: 4737,
    group: "경위도",
    degrees: true,
  },
  "EPSG:4162": {
    label: "Korean 1985 경위도 (Bessel)",
    proj4:
      "+proj=longlat +ellps=bessel +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 4162,
    group: "경위도",
    degrees: true,
  },

  // ── UTM-K / 전국 단일 ──
  "EPSG:5179": {
    label: "UTM-K (GRS80) — 국토지리정보원 표준",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127.5 +k=0.9996 +x_0=1000000 +y_0=2000000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5179,
    group: "전국 단일 (UTM-K)",
  },
  "EPSG:5178": {
    label: "UTM-K (Bessel)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127.5 +k=0.9996 +x_0=1000000 +y_0=2000000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 5178,
    group: "전국 단일 (UTM-K)",
  },
  KATEC: {
    label: "KATEC (내비게이션용 카텍)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=128 +k=0.9999 +x_0=400000 +y_0=600000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: null,
    group: "전국 단일 (UTM-K)",
  },

  // ── GRS80 원점 (falseY 600000) — 현행 지적·공간정보 표준 ──
  "EPSG:5185": {
    label: "서부원점 (GRS80, falseY 600000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=125 +k=1 +x_0=200000 +y_0=600000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5185,
    group: "GRS80 원점 (falseY 600000)",
  },
  "EPSG:5186": {
    label: "중부원점 (GRS80, falseY 600000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=600000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5186,
    group: "GRS80 원점 (falseY 600000)",
  },
  "EPSG:5187": {
    label: "동부원점 (GRS80, falseY 600000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=129 +k=1 +x_0=200000 +y_0=600000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5187,
    group: "GRS80 원점 (falseY 600000)",
  },
  "EPSG:5188": {
    label: "동해(울릉)원점 (GRS80, falseY 600000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=131 +k=1 +x_0=200000 +y_0=600000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5188,
    group: "GRS80 원점 (falseY 600000)",
  },

  // ── GRS80 원점 (falseY 500000) ──
  "EPSG:5180": {
    label: "서부원점 (GRS80, falseY 500000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=125 +k=1 +x_0=200000 +y_0=500000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5180,
    group: "GRS80 원점 (falseY 500000)",
  },
  "EPSG:5181": {
    label: "중부원점 (GRS80, falseY 500000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=500000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5181,
    group: "GRS80 원점 (falseY 500000)",
  },
  "EPSG:5182": {
    label: "제주원점 (GRS80, falseY 550000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=550000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5182,
    group: "GRS80 원점 (falseY 500000)",
  },
  "EPSG:5183": {
    label: "동부원점 (GRS80, falseY 500000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=129 +k=1 +x_0=200000 +y_0=500000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5183,
    group: "GRS80 원점 (falseY 500000)",
  },
  "EPSG:5184": {
    label: "동해(울릉)원점 (GRS80, falseY 500000)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=131 +k=1 +x_0=200000 +y_0=500000 +ellps=GRS80 +units=m +no_defs",
    epsg: 5184,
    group: "GRS80 원점 (falseY 500000)",
  },

  // ── Bessel 보정 원점 — 옛 지적도 (경도에 +0.0028902777778° 보정) ──
  "EPSG:5173": {
    label: "서부원점 (Bessel 보정)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=125.0028902777778 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 5173,
    group: "Bessel 보정 원점 (옛 지적)",
  },
  "EPSG:5174": {
    label: "중부원점 (Bessel 보정)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127.0028902777778 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 5174,
    group: "Bessel 보정 원점 (옛 지적)",
  },
  "EPSG:5175": {
    label: "제주원점 (Bessel 보정)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127.0028902777778 +k=1 +x_0=200000 +y_0=550000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 5175,
    group: "Bessel 보정 원점 (옛 지적)",
  },
  "EPSG:5176": {
    label: "동부원점 (Bessel 보정)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=129.0028902777778 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 5176,
    group: "Bessel 보정 원점 (옛 지적)",
  },
  "EPSG:5177": {
    label: "동해(울릉)원점 (Bessel 보정)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=131.0028902777778 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 5177,
    group: "Bessel 보정 원점 (옛 지적)",
  },

  // ── Bessel 무보정 원점 ──
  "EPSG:2098": {
    label: "서부원점 (Bessel)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=125 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 2098,
    group: "Bessel 원점",
  },
  "EPSG:2097": {
    label: "중부원점 (Bessel)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 2097,
    group: "Bessel 원점",
  },
  "EPSG:2096": {
    label: "동부원점 (Bessel)",
    proj4:
      "+proj=tmerc +lat_0=38 +lon_0=129 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43",
    epsg: 2096,
    group: "Bessel 원점",
  },

  // ── 기타 ──
  "EPSG:3857": {
    label: "Web Mercator (Google/OSM)",
    proj4:
      "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs",
    epsg: 3857,
    group: "기타",
  },
  "EPSG:32652": {
    label: "UTM 52N (WGS84)",
    proj4: "+proj=utm +zone=52 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
    epsg: 32652,
    group: "기타",
  },
  "EPSG:32651": {
    label: "UTM 51N (WGS84)",
    proj4: "+proj=utm +zone=51 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
    epsg: 32651,
    group: "기타",
  },
};

/** UI 드롭다운용 그룹 순서. */
export const CRS_GROUPS = [
  "경위도",
  "전국 단일 (UTM-K)",
  "GRS80 원점 (falseY 600000)",
  "GRS80 원점 (falseY 500000)",
  "Bessel 보정 원점 (옛 지적)",
  "Bessel 원점",
  "기타",
] as const;

/** 사용자가 직접 넣은 proj4 문자열을 쓰겠다는 표식. */
export const CUSTOM_CRS = "__custom__";

export type Converter = (xy: [number, number]) => [number, number];

/**
 * 변환 함수 생성. `target` 이 소스와 같으면 `null` (변환 불필요).
 *
 * proj4js 는 여기서 처음 로드된다.
 */
export async function makeConverter(
  target: string,
  customProj4?: string,
  /** 출발 좌표계. light 는 4326, 원본(detail) 은 5179. */
  source: string = SOURCE_CRS,
): Promise<Converter | null> {
  const targetDef =
    target === CUSTOM_CRS ? customProj4?.trim() : CRS_DEFS[target]?.proj4;
  if (!targetDef) throw new Error("좌표계 정의를 찾을 수 없습니다.");

  const sourceDef = CRS_DEFS[source]?.proj4;
  if (!sourceDef) throw new Error(`출발 좌표계를 알 수 없습니다: ${source}`);

  // 출발과 도착이 같으면 변환 자체를 건너뛴다 (부동소수 오차 방지).
  if (target === source) return null;

  const proj4 = (await import("proj4")).default;
  // proj4(from, to) 는 변환기를 돌려주지만, 타입 정의가 좌표 인자를 받는
  // 오버로드로 좁혀버려 forward 가 안 보인다. 최소 형태로 좁혀 쓴다.
  type Fwd = { forward(c: [number, number]): number[] };
  let fwd: Fwd;
  try {
    fwd = proj4(sourceDef, targetDef) as unknown as Fwd;
  } catch (e) {
    throw new Error(
      `proj4 정의가 올바르지 않습니다: ${e instanceof Error ? e.message : String(e)}`,
    );
  }
  return (xy) => {
    const r = fwd.forward(xy);
    return [r[0]!, r[1]!];
  };
}

/** 파일명 꼬리표. `EPSG:5179` → `5179`, custom → `custom`. */
export function crsSuffix(target: string): string {
  if (target === CUSTOM_CRS) return "custom";
  const def = CRS_DEFS[target];
  if (def?.epsg != null) return String(def.epsg);
  return target.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

/** GeoPackage srs_id. 비표준 정의는 0(undefined geographic) 대신 -1 을 쓴다. */
export function crsSrsId(target: string): number {
  if (target === CUSTOM_CRS) return -1;
  return CRS_DEFS[target]?.epsg ?? -1;
}
