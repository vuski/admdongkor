/**
 * "섬 지역 당겨오기" — 멀리 떨어진 섬 여섯 덩어리를 육지 가까이 평행이동한다.
 *
 * 한국 지도는 백령도·연평도·흑산도·제주도·울릉도·독도 때문에 실제로 그리면
 * 본토가 화면 구석으로 밀린다. 통계지도에서 흔히 쓰는 관용 표현대로 섬을
 * 육지 쪽으로 당겨 붙이고, 원래 자리가 아님을 알리는 지시선을 함께 넣는다.
 *
 * ## 좌표계
 * 이동량·박스·지시선 좌표는 전부 **EPSG:4326(경위도)** 기준이다.
 * 따라서 **좌표 변환 전에** 적용해야 한다 (download.ts 가 그 순서를 지킨다).
 *
 * ## 이동 단위 = polygon part
 * feature(행) 가 아니라 MultiPolygon 안의 **part** 단위로 옮긴다.
 * 예: 인천광역시는 본토 part 는 그대로 두고 백령도/연평도 part 만 당겨온다.
 * 판정은 `box.contains(part)` — 박스에 **완전히** 들어간 part 만 대상이라
 * 걸쳐 있는 폴리곤이 잘리거나 어긋날 일이 없다.
 *
 * ## 출처
 * 박스와 이동량은 사용자가 QGIS 에서 직접 배치한 결과
 * (`experiments/movebox/`) 에서 역산했다. 237개 시군구 전체에 대해
 * 재현 검증했고 최대 Hausdorff 편차 0.0001 m 로 일치한다.
 * 지시선은 `experiments/movebox/line.gpkg` 의 좌표를 그대로 옮겨 적었다.
 */

import type { FeatureCollection, Feature, Position } from "geojson";

/** 섬 한 덩어리: 판정용 bbox(원래 위치) + 이동량. 전부 EPSG:4326. */
interface IslandBox {
  name: string;
  /** [minLon, minLat, maxLon, maxLat] — 원래 위치 기준. */
  bbox: [number, number, number, number];
  /** [dLon, dLat] — 이 만큼 평행이동. */
  offset: [number, number];
}

/**
 * 여섯 덩어리. bbox 는 원래 위치, offset 은 당겨올 양.
 * 울릉도와 독도가 **따로**인 이유: 둘은 실제 거리가 멀어 한 덩어리로 묶으면
 * 사이 빈 바다까지 통째로 끌려와 배치가 어색해진다. 이동량도 서로 다르다.
 */
const ISLANDS: IslandBox[] = [
  {
    name: "백령도",
    bbox: [124.58093, 37.71742, 124.81836, 38.03249],
    offset: [1.006894246, -0.222519381],
  },
  {
    name: "연평도",
    bbox: [125.60078, 37.54513, 125.77615, 37.79089],
    offset: [0.279843938, -0.041196164],
  },
  {
    name: "흑산도",
    bbox: [124.98698, 33.99801, 125.53333, 34.81604],
    offset: [0.211606177, 0.003106404],
  },
  {
    name: "제주도",
    bbox: [126.08103, 33.06588, 127.05771, 33.67332],
    offset: [2.029566586, 0.798281135],
  },
  {
    name: "울릉도",
    bbox: [130.73916, 37.38895, 130.98063, 37.62366],
    offset: [-1.245885101, -0.124700519],
  },
  {
    name: "독도",
    bbox: [131.79543, 37.20331, 131.94652, 37.29998],
    offset: [-2.084790851, -0.000649327],
  },
];

/**
 * 당겨온 표시 지시선 — 둔각 꺾은선(3점) 6개. EPSG:4326.
 * `experiments/movebox/line.gpkg` 그대로. 이동된 섬 **옆**에 놓여
 * "여기 있는 건 원래 자리가 아니다" 를 알린다.
 *
 * 일점쇄선(dash-dot)은 지오메트리가 아니라 **표현(스타일)** 이라 데이터
 * 포맷에는 담기지 않는다. QGIS 등에서 이 레이어에 일점쇄선 스타일을 주면 된다.
 */
const CONNECTOR_LINES: Position[][] = [
  [[125.74513, 37.84950], [125.85035, 37.63401], [125.73973, 37.42647]],
  [[126.03112, 37.77703], [126.11206, 37.59768], [126.02842, 37.43932]],
  [[125.67768, 34.87139], [125.82067, 34.40746], [125.68037, 33.93423]],
  [[127.91838, 34.19349], [128.50115, 34.47866], [129.28358, 34.41414]],
  [[129.84746, 37.37610], [129.71256, 37.29029], [129.75034, 37.13236]],
  [[129.65456, 37.57844], [129.47649, 37.43718], [129.52505, 37.22694]],
];

/** ring 전체가 bbox 안에 들어오는가. 하나라도 벗어나면 false. */
function ringInside(
  ring: Position[],
  [minX, minY, maxX, maxY]: [number, number, number, number],
): boolean {
  for (const p of ring) {
    const x = p[0] as number;
    const y = p[1] as number;
    if (x < minX || x > maxX || y < minY || y > maxY) return false;
  }
  return true;
}

/** polygon(= ring 배열) 을 통째로 평행이동. in-place. */
function shiftPolygon(poly: Position[][], dx: number, dy: number): void {
  for (const ring of poly) {
    for (const p of ring) {
      (p as Position)[0] = (p[0] as number) + dx;
      (p as Position)[1] = (p[1] as number) + dy;
    }
  }
}

/**
 * FeatureCollection 의 섬 part 들을 당겨온다 (in-place).
 * 입력은 **EPSG:4326** 이어야 한다.
 * 반환: 이동한 part 개수 (검증·로깅용).
 */
export function pullInIslands(fc: FeatureCollection): number {
  let moved = 0;
  for (const f of fc.features) {
    const g = f.geometry;
    if (!g) continue;
    // Polygon 은 part 가 하나, MultiPolygon 은 여럿. 그 외 타입은 대상 아님.
    const polys: Position[][][] =
      g.type === "Polygon"
        ? [g.coordinates as Position[][]]
        : g.type === "MultiPolygon"
          ? (g.coordinates as Position[][][])
          : [];
    for (const poly of polys) {
      const outer = poly[0];
      if (!outer || outer.length === 0) continue;
      for (const isl of ISLANDS) {
        // 외곽 ring 이 박스에 완전히 들어가면 그 part 는 이 섬에 속한다.
        if (!ringInside(outer, isl.bbox)) continue;
        shiftPolygon(poly, isl.offset[0], isl.offset[1]);
        moved++;
        break; // 박스끼리 겹치지 않으므로 첫 매치에서 확정.
      }
    }
  }
  return moved;
}

/** 지시선 6개를 LineString feature 로. 좌표계는 EPSG:4326. */
export function connectorFeatures(): Feature[] {
  return CONNECTOR_LINES.map((coords, i) => ({
    type: "Feature" as const,
    properties: { kind: "island_connector", seq: i + 1 },
    // 매번 새 배열로 복사 — 이후 좌표 변환이 in-place 라 상수를 오염시키면 안 된다.
    geometry: {
      type: "LineString" as const,
      coordinates: coords.map((p) => [p[0], p[1]] as Position),
    },
  }));
}

/** 지시선만 담은 FeatureCollection (별도 레이어/파일용). */
export function connectorFeatureCollection(): FeatureCollection {
  return { type: "FeatureCollection", features: connectorFeatures() };
}
