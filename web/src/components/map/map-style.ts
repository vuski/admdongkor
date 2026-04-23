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

/** 스타일 로드 후 symbol 레이어의 text-field 를 한국어로 전환. */
export function applyKoreanLabels(map: MapLibreMap): void {
  const style = map.getStyle();
  if (!style?.layers) return;
  for (const layer of style.layers) {
    if (layer.type !== "symbol") continue;
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
