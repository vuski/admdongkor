import type { GeoJSONSource, Map as MapLibreMap } from "maplibre-gl";
import type { Level } from "admdongkor";
import type { LabelDatum } from "./label-data";
import type { LabelData } from "./label-layers";

// 이 이상 줌에서만 emd 라벨. 그 전엔 sgg 라벨로 fallback.
const EMD_MIN_ZOOM = 10;

// 레벨별 라벨 크기 (위계 유지).
const SIZE_BY_LEVEL: Record<Level, number> = { sido: 18, sgg: 15, emd: 12 };

// ⚠️ text-font 는 Carto glyph 서버(Positron)에 라틴 PBF 가 실재하는 폰트여야 한다.
//   없는 폰트(예: "NanumBarunGothic Regular", "Noto Sans Bold")를 지정하면 glyph
//   404 로 라벨이 통째로 안 뜬다. "Open Sans Bold" 는 Positron 원본이 쓰는 폰트라 안전.
//   한글은 MapLibre 가 glyph PBF 없이 로컬 렌더(TinySDF)하므로 라틴 폰트만 실재하면 됨.
//   이름의 "Bold" 로 한글도 700 굵기로 렌더. (popuKrei 검증 패턴)
const TEXT_FONT = ["Open Sans Bold"];

// 독도 라벨은 **행정구역이 아니라 지명(섬)** 이므로 행정구역 라벨과 구분되게
// 그린다. 같은 폰트로 쓰면 "경상북도" 옆의 "독도" 가 시도 레벨처럼 읽힌다.
//
// ⚠️ 한글은 glyph PBF 가 아니라 `localIdeographFontFamily`(브라우저 canvas) 로
//    렌더되므로 **text-font 의 italic/bold 는 한글에 적용되지 않는다.**
//    따라서 구분은 폰트가 아니라 **크기·색·halo 없음**으로 준다.
//    (text-font 자체는 라틴 fallback 용으로 실재하는 값이어야 함 — 404 나면
//     라벨이 통째로 사라진다. "Open Sans Italic" 은 HTTP 200 확인.)
const PLACE_FONT = ["Open Sans Italic"];
const PLACE_SIZE = 12;

/** 독도 라벨. 행정구역 경계와 무관하게 항상 같은 자리에 찍는다.
 *  동도·서도 사이 지점 (WGS84). */
const DOKDO_LABEL = {
  text: "독도",
  position: [131.8664, 37.2429] as [number, number],
};

const LEVELS: Level[] = ["sido", "sgg", "emd"];

/** deck.gl(경계선/diff) 레이어를 이 anchor "아래"에 삽입시키기 위한 빈 레이어 id.
 *  라벨 심볼은 anchor 위(스택 최상단)에 두므로 라벨이 항상 deck 경계선 위에 그려진다.
 *  deck.gl MapboxOverlay 는 각 레이어 props 의 beforeId 로 삽입 위치를 지정할 수 있다. */
export const DECK_ANCHOR_ID = "adm-deck-anchor";

/** deck 레이어 삽입 anchor(빈 symbol 레이어)를 스택 최상단 근처에 1회 보장.
 *  실제로 아무것도 안 그리는 placeholder — beforeId 대상용.
 *  ⚠️ deck.gl MapboxOverlay 는 beforeId 레이어가 없으면 addLayer 가 throw 하므로,
 *     deck 레이어가 처음 삽입되기 전(onLoad)에 반드시 먼저 만들어져 있어야 한다. */
export function ensureDeckAnchor(map: MapLibreMap): void {
  if (map.getLayer(DECK_ANCHOR_ID)) return;
  if (!map.getSource(DECK_ANCHOR_ID)) {
    map.addSource(DECK_ANCHOR_ID, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });
  }
  map.addLayer({
    id: DECK_ANCHOR_ID,
    type: "symbol",
    source: DECK_ANCHOR_ID,
  });
}

/** side + level 별 source id. */
function srcId(side: "A" | "B", lv: Level): string {
  return `adm-labels-${side}-${lv}`;
}
/** side + level 별 symbol layer id. */
function layerId(side: "A" | "B", lv: Level): string {
  return `adm-labels-${side}-${lv}-sym`;
}

/** 지명(독도) 라벨 source/layer id — 레벨과 무관하게 side 당 하나. */
function placeSrcId(side: "A" | "B"): string {
  return `adm-place-${side}`;
}
function placeLayerId(side: "A" | "B"): string {
  return `adm-place-${side}-sym`;
}

/** 독도 라벨 레이어를 보장/제거. 행정구역 라벨과 독립적으로 동작한다. */
function applyPlaceLabels(map: MapLibreMap, side: "A" | "B", show: boolean): void {
  const lid = placeLayerId(side);
  const sid = placeSrcId(side);

  if (!show) {
    if (map.getLayer(lid)) map.removeLayer(lid);
    if (map.getSource(sid)) map.removeSource(sid);
    return;
  }

  const data: GeoJSON.FeatureCollection<GeoJSON.Point> = {
    type: "FeatureCollection",
    features: [{
      type: "Feature",
      geometry: { type: "Point", coordinates: DOKDO_LABEL.position },
      properties: { label: DOKDO_LABEL.text },
    }],
  };

  const src = map.getSource(sid) as GeoJSONSource | undefined;
  if (src) src.setData(data);
  else map.addSource(sid, { type: "geojson", data });

  if (!map.getLayer(lid)) {
    map.addLayer({
      id: lid,
      type: "symbol",
      source: sid,
      layout: {
        "text-field": ["get", "label"],
        "text-size": PLACE_SIZE,
        "text-font": PLACE_FONT,
        // 주변에 겹칠 행정구역 라벨이 없는 먼바다라 항상 표시해도 안전하고,
        // "항상 독도라고 표시" 가 요구사항이므로 충돌로 사라지지 않게 한다.
        "text-allow-overlap": true,
        "text-ignore-placement": true,
        "text-anchor": "top",
        "text-offset": [0, 0.5],
      },
      paint: {
        // halo 없이 — 행정구역 라벨(흰 버퍼) 과 시각적으로 구분.
        // 색도 행정구역(#111 진한 검정) 보다 흐리게 해서 위계를 낮춘다.
        "text-color": "#4a4a4a",
      },
    });
  }
  // 생성 직후에도 반드시 최상단으로. deck.gl 은 beforeId=DECK_ANCHOR_ID 로
  // anchor "아래"에 삽입되지만, 그건 anchor 기준일 뿐이라 이 레이어가 anchor
  // 아래에 남아 있으면 deck 의 fill 에 덮인다. 멱등이므로 매번 호출해도 안전.
  map.moveLayer(lid);
}

/** LabelDatum[] → MapLibre symbol 용 GeoJSON Point FeatureCollection.
 *  properties.label = text(줄바꿈 포함), sortKey = -priority
 *  (symbol-sort-key 는 작을수록 우선 → 면적 큰 라벨이 충돌 시 살아남도록 부호 반전). */
function labelsToGeoJSON(
  labels: LabelDatum[],
): GeoJSON.FeatureCollection<GeoJSON.Point> {
  return {
    type: "FeatureCollection",
    features: labels.map((d) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: d.position },
      properties: { label: d.text, sortKey: -d.priority },
    })),
  };
}

/** 마지막으로 적용한 라벨 상태 (side 별). idle 마다 재적용을 피하기 위한 dirty-check 키.
 *  labelData 는 map-pane 에서 useMemo 로 안정 참조이므로 === 비교가 유효하다. */
interface AppliedState {
  show: boolean;
  activeLevel: Level;
  labelData: LabelData;
}
const lastApplied = new WeakMap<MapLibreMap, Record<"A" | "B", AppliedState | null>>();

/** 이 map 에 라벨 심볼 레이어를 (재)적용. 멱등 — source 있으면 setData, layer 없으면 addLayer.
 *  z-순서: deck.gl(경계선/diff) 은 DECK_ANCHOR_ID 아래에 삽입되고, 라벨은 anchor 위
 *  (스택 최상단)로 moveLayer → 라벨이 항상 경계선 위에 그려진다.
 *  스타일 미완료면 조용히 return (호출부의 idle/style.load 가 재시도).
 *
 *  ⚠️ idle 마다 호출되므로, 상태(show/activeLevel/labelData)가 직전과 같으면 early return.
 *     안 그러면 이동/줌 멈출 때마다 setData(source 재파싱) + moveLayer 가 돌아 지도가 무거워진다. */
export function applyLabels(
  map: MapLibreMap,
  side: "A" | "B",
  show: boolean,
  level: Level,
  zoomInt: number,
  labelData: LabelData,
): void {
  if (!map.isStyleLoaded()) return;

  // 배경 경계 level 에 따라 실제로 그릴 라벨 레벨:
  // sido → sido / sgg → sgg / emd → 줌<10 이면 sgg fallback, 아니면 emd.
  let activeLevel: Level = level;
  if (level === "emd" && zoomInt < EMD_MIN_ZOOM) activeLevel = "sgg";

  // dirty-check: 직전 적용과 동일하고 anchor·레이어도 살아있으면 아무것도 안 함.
  let sideMap = lastApplied.get(map);
  if (!sideMap) {
    sideMap = { A: null, B: null };
    lastApplied.set(map, sideMap);
  }
  const prev = sideMap[side];
  const anchorAlive = !!map.getLayer(DECK_ANCHOR_ID);
  // 독도 라벨은 dirty-check **앞**에서 처리한다. 뒤에 두면 show/level/labelData 가
  // 그대로인 idle 재호출에서 early-return 에 걸려 영영 생성되지 않는다.
  // (style.load 로 레이어가 통째로 날아간 뒤 복구도 이 경로로 이뤄진다.)
  applyPlaceLabels(map, side, show);
  if (
    prev &&
    anchorAlive &&
    prev.show === show &&
    prev.activeLevel === activeLevel &&
    prev.labelData === labelData
  ) {
    return; // 변경 없음 — idle 재호출 무시.
  }
  sideMap[side] = { show, activeLevel, labelData };

  // deck 레이어 삽입 anchor 보장 (deck 가 이 아래로 들어가도록).
  ensureDeckAnchor(map);

  for (const lv of LEVELS) {
    const on = show && lv === activeLevel;

    if (on && labelData[lv].length > 0) {
      const data = labelsToGeoJSON(labelData[lv]);
      const src = map.getSource(srcId(side, lv)) as GeoJSONSource | undefined;
      if (src) src.setData(data);
      else map.addSource(srcId(side, lv), { type: "geojson", data });

      if (!map.getLayer(layerId(side, lv))) {
        map.addLayer({
          id: layerId(side, lv),
          type: "symbol",
          source: srcId(side, lv),
          layout: {
            "text-field": ["get", "label"],
            "text-size": SIZE_BY_LEVEL[lv],
            "text-line-height": 1.05,
            "text-font": TEXT_FONT,
            "text-allow-overlap": false,
            "text-padding": 2,
            // 작을수록 우선(면적 큰 게 먼저) → 겹칠 때 큰 행정구역 라벨이 살아남음.
            "symbol-sort-key": ["get", "sortKey"],
          },
          paint: {
            "text-color": "#111111",
            "text-halo-color": "#ffffff",
            "text-halo-width": 2.5, // 두꺼운 흰 halo 로 경계선 위에서도 글자 식별
            "text-halo-blur": 0,
          },
        });
      } else {
        // 안전망: 라벨 위에 뭔가 쌓였으면 맨 위로 복귀. 멱등.
        map.moveLayer(layerId(side, lv));
      }
    } else {
      if (map.getLayer(layerId(side, lv))) map.removeLayer(layerId(side, lv));
      if (map.getSource(srcId(side, lv))) map.removeSource(srcId(side, lv));
    }
  }

  // 행정구역 라벨을 새로 얹었으므로 독도 라벨을 다시 최상단으로.
  if (map.getLayer(placeLayerId(side))) map.moveLayer(placeLayerId(side));
}
