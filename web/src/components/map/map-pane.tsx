"use client";

import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
import { Map, NavigationControl } from "react-map-gl/maplibre";
import type { MapRef, ViewStateChangeEvent } from "react-map-gl/maplibre";
import { GeoJsonLayer } from "@deck.gl/layers";
import type { Level } from "admdongkor";
import { DeckOverlay } from "./deck-overlay";
import {
  POSITRON_STYLE_URL,
  INITIAL_VIEW,
  applyKoreanLabels,
  setBasemapVisible,
} from "./map-style";
import type { LayerData } from "@/hooks/use-admdongkor-geojson";
import { buildLabels } from "./label-data";
import {
  A_FILL_BY_LEVEL,
  A_LINE_BY_LEVEL,
  B_FILL_BY_LEVEL,
  B_LINE_BY_LEVEL,
  lineWidthForLevel,
} from "./layer-colors";
import { buildLabelLayers, type LabelData } from "./label-layers";
import type { HoverInfo } from "./hover-types";

export interface MapPaneHandle {
  map: MapRef | null;
}

interface Props {
  /** emd → sgg → sido 순서로 전달 (배열 앞일수록 deck.gl 에서 먼저 그려져 아래에 깔림). */
  dataLayers: LayerData[];
  side: "A" | "B";
  level: Level;
  showControls: boolean;
  showBasemap: boolean;
  showLabels: boolean;
  /** compare() 결과 diff 하이라이트 레이어. 외부에서 빌드해서 주입. */
  extraLayers?: import("@deck.gl/layers").GeoJsonLayer[];
  /** split drag 등으로 hover 비활성화 시 pickable/autoHighlight 전부 끔. */
  pickingDisabled?: boolean;
  onMove?: (e: ViewStateChangeEvent) => void;
  onHover?: (info: HoverInfo | null) => void;
  interactive: boolean;
}

export const MapPane = forwardRef<MapPaneHandle, Props>(function MapPane(
  {
    dataLayers,
    side,
    level,
    showControls,
    showBasemap,
    showLabels,
    extraLayers = [],
    pickingDisabled = false,
    onMove,
    onHover,
    interactive,
  },
  ref,
) {
  const mapRef = useRef<MapRef>(null);
  useImperativeHandle(ref, () => ({ get map() { return mapRef.current; } }), []);

  const [zoomInt, setZoomInt] = useState(Math.floor(INITIAL_VIEW.zoom));

  const fillPalette = side === "A" ? A_FILL_BY_LEVEL : B_FILL_BY_LEVEL;
  const linePalette = side === "A" ? A_LINE_BY_LEVEL : B_LINE_BY_LEVEL;

  // 경계선 레이어.
  // - sido 만 옅은 fill (시각적 구분)
  // - 현재 선택된 level 레이어만 pickable + autoHighlight → hover 시 하이라이트
  //   다른 레이어들은 pickable:false 라 hit-test 에서 스킵됨
  // - pickable 레이어는 fill 이 있어야 hit-test 가 잡히므로 현재 level 은 filled:true
  //   (선만 있으면 hover 영역이 선 위로만 제한돼 잡기 어려움)
  const boundaryLayers = useMemo(
    () =>
      dataLayers.map(({ level: lv, data }) => {
        const isCurrent = lv === level;
        const pickable = isCurrent && !pickingDisabled;
        return new GeoJsonLayer({
          id: `adm-${side}-${lv}`,
          data,
          stroked: true,
          filled: isCurrent || lv === "sido",
          getFillColor:
            isCurrent && lv !== "sido" ? [0, 0, 0, 1] : fillPalette[lv],
          getLineColor: linePalette[lv],
          lineWidthUnits: "pixels",
          getLineWidth: lineWidthForLevel(lv),
          pickable,
          autoHighlight: pickable,
          highlightColor:
            side === "A" ? [28, 112, 87, 80] : [194, 80, 15, 80],
          onHover: pickable
            ? (info) => {
                if (!onHover) return;
                if (!info.object) {
                  onHover(null);
                  return;
                }
                const props =
                  (info.object as { properties?: Record<string, unknown> })
                    .properties ?? {};
                onHover({
                  side,
                  level: lv,
                  x: info.x,
                  y: info.y,
                  sidonm: (props.sidonm as string) ?? null,
                  sidocd: (props.sidocd as string) ?? null,
                  sggnm: (props.sggnm as string) ?? null,
                  sggcd: (props.sggcd as string) ?? null,
                  emdnm: (props.emdnm as string) ?? null,
                  emdcd: (props.emdcd as string) ?? null,
                  emd7: (props.emd7 as string) ?? null,
                  emd8: (props.emd8 as string) ?? null,
                  area:
                    typeof props.area === "number"
                      ? (props.area as number)
                      : undefined,
                });
              }
            : undefined,
        });
      }),
    [dataLayers, level, pickingDisabled, side, fillPalette, linePalette, onHover],
  );

  // buildLabels (polylabel 포함) 는 data/level 변경 시에만 재계산.
  // zoom 변화마다 호출되지 않도록 useMemo 로 캐싱.
  const labelData = useMemo<LabelData>(() => ({
    sido: buildLabels(dataLayers.find((l) => l.level === "sido")?.data ?? null, "sido"),
    sgg:  buildLabels(dataLayers.find((l) => l.level === "sgg")?.data  ?? null, "sgg"),
    emd:  buildLabels(dataLayers.find((l) => l.level === "emd")?.data  ?? null, "emd"),
  }), [dataLayers]);

  // TextLayer 생성은 zoomInt(정수) + labelData 변경 시에만.
  const labelLayers = useMemo(
    () =>
      showLabels
        ? buildLabelLayers({ side, zoomInt, level, labelData })
        : [],
    [showLabels, side, zoomInt, level, labelData],
  );

  // diff 레이어는 경계선 위, 레이블 아래에 위치.
  // split 드래그처럼 parent 가 자주 리렌더될 때 참조를 안정화 → DeckOverlay 의 setProps 가
  // 실제로 바뀐 경우에만 발생.
  const layers = useMemo(
    () => [...boundaryLayers, ...extraLayers, ...labelLayers],
    [boundaryLayers, extraLayers, labelLayers],
  );

  const handleMove = useCallback(
    (e: ViewStateChangeEvent) => {
      // 정수 단위로만 state 갱신 → TextLayer 재생성 횟수 최소화.
      const newZoomInt = Math.floor(e.viewState.zoom);
      setZoomInt((prev) => (prev !== newZoomInt ? newZoomInt : prev));
      onMove?.(e);
    },
    [onMove],
  );

  const onLoad = useCallback(() => {
    const m = mapRef.current?.getMap();
    if (!m) return;
    applyKoreanLabels(m);
    setBasemapVisible(m, showBasemap);
    setZoomInt(Math.floor(m.getZoom()));
  }, [showBasemap]);

  useEffect(() => {
    const m = mapRef.current?.getMap();
    if (!m) return;
    if (!m.isStyleLoaded()) return;
    setBasemapVisible(m, showBasemap);
  }, [showBasemap]);

  return (
    <Map
      ref={mapRef}
      initialViewState={INITIAL_VIEW}
      mapStyle={POSITRON_STYLE_URL}
      onMove={handleMove}
      onLoad={onLoad}
      onStyleData={onLoad}
      interactive={interactive}
      attributionControl={side === "A" ? undefined : false}
      style={{ width: "100%", height: "100%" }}
    >
      {showControls && <NavigationControl position="bottom-right" />}
      <DeckOverlay layers={layers} />
    </Map>
  );
});
