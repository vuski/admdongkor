import type { Map as MapLibreMap } from "maplibre-gl";

/** CARTO Positron — 벡터 타일 + 한국어 레이블 적용용.
 *  style URL 을 직접 넣으면 maplibre 가 내부적으로 vector 소스를 로드한다. */
export const POSITRON_STYLE_URL =
  "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

export const INITIAL_VIEW = {
  longitude: 127.8,
  latitude: 36.3,
  zoom: 6.3,
  pitch: 0,
  bearing: 0,
};

/** basemap 의 모든 레이어 가시성을 토글. deck.gl 오버레이 레이어 (id 가
 *  `adm-` 로 시작) 는 건드리지 않는다. */
export function setBasemapVisible(map: MapLibreMap, visible: boolean): void {
  const style = map.getStyle();
  if (!style?.layers) return;
  const vis = visible ? "visible" : "none";
  for (const layer of style.layers) {
    if (layer.id.startsWith("adm-")) continue;
    try {
      map.setLayoutProperty(layer.id, "visibility", vis);
    } catch {
      /* 일부 레이어는 거부 — 무시 */
    }
  }
}

/** 스타일 로드 후 symbol 레이어의 text-field 를 한국어로 전환.
 *
 *  ⚠️ **우리가 얹는 라벨 레이어는 반드시 제외 목록에 넣어야 한다.**
 *  이 함수는 스타일의 모든 symbol 레이어를 돌며 text-field 를
 *  `coalesce(name:ko, name_ko, name)` 로 덮어쓴다. 우리 레이어의 property 는
 *  `label` 이라 덮이면 빈 텍스트가 되어 **레이어는 살아있는데 글자만 사라진다**
 *  (getLayer 는 true, queryRenderedFeatures 는 0 — 원인 찾기 어려움).
 *
 *  새 라벨 레이어를 추가하면 여기 prefix 를 등록할 것.
 *    adm-labels-  행정구역 라벨
 *    adm-place-   지명 라벨 (독도 등)
 *  → .readme/admdongkor/20260826_dokdo_label.md */
export function applyKoreanLabels(map: MapLibreMap): void {
  const style = map.getStyle();
  if (!style?.layers) return;
  for (const layer of style.layers) {
    if (layer.type !== "symbol") continue;
    // 우리 라벨 레이어는 제외 (위 주석 참조).
    if (layer.id.startsWith("adm-labels-")) continue;
    if (layer.id.startsWith("adm-place-")) continue;
    const layout = (layer as { layout?: Record<string, unknown> }).layout;
    if (!layout || !layout["text-field"]) continue;
    try {
      map.setLayoutProperty(layer.id, "text-field", [
        "coalesce",
        ["get", "name:ko"],
        ["get", "name_ko"],
        ["get", "name"],
      ]);
    } catch {
      // 일부 레이어는 expression 형식을 거부 — 무시.
    }
  }
}
