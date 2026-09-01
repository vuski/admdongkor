/**
 * "섬 지역 당겨오기" — 멀리 떨어진 섬 여섯 덩어리를 육지 가까이 평행이동한다.
 *
 * 한국 지도는 백령도·연평도·흑산도·제주도·울릉도·독도 때문에 실제로 그리면
 * 본토가 화면 구석으로 밀린다. 통계지도에서 흔히 쓰는 관용 표현대로 섬을
 * 육지 쪽으로 당겨 붙이고, 원래 자리가 아님을 알리는 지시선을 함께 넣는다.
 *
 * ## 좌표계 — EPSG:5179 에서 옮긴다
 * 이동량·박스·지시선 좌표는 전부 **EPSG:5179(UTM-K, m)** 기준이다.
 *
 * 왜 5179 인가: 평행이동은 좌표계마다 결과가 다르고, 투영이 비선형이라
 * 어느 한 좌표계에서 잰 (dx,dy) 를 다른 좌표계에 그대로 못 쓴다.
 * 세 후보를 실측 비교한 결과 5179(등각·한국 최적화) 가 면적 왜곡이 가장 작다:
 *
 *     4326 이동   면적변화 중앙값 0.0545%  최대 0.9167%
 *     5179 이동   면적변화 중앙값 0.0205%  최대 0.2679%   ← 채택
 *     3857 이동   면적변화 중앙값 0.1250%  최대 1.7528%
 *
 * **옮기는 순간 면적·형상은 어차피 조금 틀어진다.** 지도 표현을 위한 기능이지
 * 면적 계산용이 아니며, UI 가 그 점을 경고한다.
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

/** 섬 이름 — 지시선을 이름으로 묶어 index 어긋남을 막는다. */
type IslandName =
  | "백령도"
  | "연평도"
  | "흑산도"
  | "제주도"
  | "울릉도"
  | "독도";

/** 섬 한 덩어리: 판정용 bbox(원래 위치) + 이동량. 전부 EPSG:5179(m). */
interface IslandBox {
  name: IslandName;
  /** [minX, minY, maxX, maxY] — 원래 위치 기준. */
  bbox: [number, number, number, number];
  /** [dx, dy] — 이 만큼 평행이동 (m). */
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
    bbox: [743663.9, 1972033.3, 763680.9, 2007627.4],
    offset: [87624.0, -26613.9],
  },
  {
    name: "연평도",
    bbox: [832771.3, 1951048.6, 847717.1, 1978379.4],
    offset: [25139.9, -5655.3],
  },
  {
    name: "흑산도",
    bbox: [768767.2, 1557987.9, 819135.6, 1649706.5],
    offset: [21704.2, -331.8],
  },
  {
    name: "제주도",
    bbox: [868415.8, 1453650.5, 958718.1, 1520209.8],
    offset: [184130.2, 84861.5],
  },
  {
    name: "울릉도",
    bbox: [1286352.2, 1937131.9, 1308127.7, 1963347.4],
    offset: [-109445.0, -15271.7],
  },
  {
    name: "독도",
    bbox: [1380937.2, 1920261.8, 1394640.7, 1931525.0],
    offset: [-183660.7, -8965.5],
  },
];

/**
 * 당겨온 표시 지시선 — 둔각 꺾은선(3점) 6개. EPSG:4326.
 * `experiments/movebox/line.gpkg` 를 5179 로 변환한 값. 이동된 섬 **옆**에 놓여
 * "여기 있는 건 원래 자리가 아니다" 를 알린다.
 *
 * 일점쇄선(dash-dot)은 지오메트리가 아니라 **표현(스타일)** 이라 데이터
 * 포맷에는 담기지 않는다. QGIS 등에서 이 레이어에 일점쇄선 스타일을 주면 된다.
 */
const CONNECTOR_LINES: { island: IslandName; coords: Position[] }[] = [
  {
    island: "백령도",
    coords: [[845606.4, 1984752.7], [854443.1, 1960673.7], [844250.8, 1937822.9]],
  },
  {
    island: "연평도",
    coords: [[870642.9, 1976277.1], [877476.5, 1956268.8], [869817.9, 1938811.2]],
  },
  {
    island: "흑산도",
    coords: [[833440.4, 1654480.9], [845654.0, 1602799.3], [831821.4, 1550539.3]],
  },
  {
    island: "제주도",
    coords: [[1038548.9, 1577874.7], [1091934.2, 1609870.8], [1163915.6, 1603703.6]],
  },
  {
    island: "독도",
    coords: [[1207850.3, 1933366.4], [1196127.3, 1923556.0], [1199892.5, 1906110.9]],
  },
  {
    island: "울릉도",
    coords: [[1190253.6, 1955410.6], [1174857.2, 1939390.8], [1179654.3, 1916154.5]],
  },
];

/**
 * "단순화(많이)"(시군구 2.7%) 에서 **완전히 사라지는** 섬.
 * 섬이 없는데 지시선만 남으면 바다에 선이 떠 있게 되므로 함께 뺀다.
 *
 * 실측 (원본 light → 2.7%):
 *   백령도 3→1   연평도 5→0   흑산도 20→0
 *   제주도 13→2  울릉도 3→1   독도 2→2 (되붙이므로 유지)
 */
const GONE_WHEN_SUPER: IslandName[] = ["연평도", "흑산도"];

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
 * 입력은 **EPSG:5179** 여야 한다.
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

/**
 * 지시선을 LineString feature 로. 좌표계는 EPSG:5179.
 *
 * @param superSimplify "단순화(많이)" 여부. true 면 그 해상도에서 사라지는
 *   섬(연평도·흑산도) 의 지시선을 뺀다 — 섬 없이 선만 남으면 안 되므로.
 */
export function connectorFeatures(superSimplify = false): Feature[] {
  const lines = superSimplify
    ? CONNECTOR_LINES.filter((l) => !GONE_WHEN_SUPER.includes(l.island))
    : CONNECTOR_LINES;
  return lines.map(({ island, coords }, i) => ({
    type: "Feature" as const,
    properties: { kind: "island_connector", island, seq: i + 1 },
    // 매번 새 배열로 복사 — 이후 좌표 변환이 in-place 라 상수를 오염시키면 안 된다.
    geometry: {
      type: "LineString" as const,
      coordinates: coords.map((p) => [p[0], p[1]] as Position),
    },
  }));
}

/** 지시선만 담은 FeatureCollection (별도 레이어/파일용). */
export function connectorFeatureCollection(
  superSimplify = false,
): FeatureCollection {
  return {
    type: "FeatureCollection",
    features: connectorFeatures(superSimplify),
  };
}
