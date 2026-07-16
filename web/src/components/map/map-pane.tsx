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
import type { LabelData } from "./label-layers";
import { applyLabels, ensureDeckAnchor, DECK_ANCHOR_ID } from "./label-symbol";
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

  // emd 모드의 줌<10 sgg fallback 판단에 필요. 정수 줌만 state 로 (TextLayer 재생성 X).
  const [zoomInt, setZoomInt] = useState(Math.floor(INITIAL_VIEW.zoom));
  // map 인스턴스 준비 완료 여부. onLoad 에서 true → 라벨 effect 재실행 트리거.
  const [mapReady, setMapReady] = useState(false);

  const fillPalette = side === "A" ? A_FILL_BY_LEVEL : B_FILL_BY_LEVEL;
  const linePalette = side === "A" ? A_LINE_BY_LEVEL : B_LINE_BY_LEVEL;

  // 경계선 레이어.
  // - sido 만 옅은 fill (시각적 구분)
  // - 현재 선택된 level 레이어만 pickable + autoHighlight → hover 시 하이라이트
  //   다른 레이어들은 pickable:false 라 hit-test 에서 스킵됨
  // - pickable 레이어는 fill 이 있어야 hit-test 가 잡히므로 현재 level 은 filled:true
  //   (선만 있으면 hover 영역이 선 위로만 제한돼 잡기 어려움)
  const boundaryLayers = useMemo(
    () => {
      // anchor 가 실재할 때만 beforeId 지정 (없는데 지정하면 deck.gl addLayer 가 throw).
      // anchor 는 onLoad 의 ensureDeckAnchor 로 생성되고 onLoad 가 mapReady=true 로 만든다.
      // → mapReady 이면 anchor 존재. (mapRef 를 useMemo 안에서 읽지 말 것: deps 미추적 →
      //   매 렌더 새 레이어 → deck setProps → hover 재발화 → setState 루프.)
      const anchor = mapReady ? DECK_ANCHOR_ID : undefined;
      return dataLayers.map(({ level: lv, data }) => {
        const isCurrent = lv === level;
        const pickable = isCurrent && !pickingDisabled;
        return new GeoJsonLayer({
          id: `adm-${side}-${lv}`,
          // deck 레이어를 라벨 anchor 아래에 삽입 → 라벨이 경계선 위에 뜬다.
          beforeId: anchor,
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
      });
    },
    [dataLayers, level, pickingDisabled, side, fillPalette, linePalette, onHover, mapReady],
  );

  // buildLabels (polylabel 포함) 는 data/level 변경 시에만 재계산.
  // 라벨은 MapLibre native symbol 레이어가 그린다 (collision 자동 회피 + SDF halo).
  // 줌별 sgg↔emd 전환은 symbol layer 의 min/maxzoom 이 선언적으로 처리하므로
  // 여기서는 정수 zoom state 를 둘 필요가 없다.
  const labelData = useMemo<LabelData>(() => ({
    sido: buildLabels(dataLayers.find((l) => l.level === "sido")?.data ?? null, "sido"),
    sgg:  buildLabels(dataLayers.find((l) => l.level === "sgg")?.data  ?? null, "sgg"),
    emd:  buildLabels(dataLayers.find((l) => l.level === "emd")?.data  ?? null, "emd"),
  }), [dataLayers]);

  // 라벨 심볼 (재)적용 — MapLibre native symbol (collision 자동 회피 + halo).
  // show/level/zoomInt/labelData 변경 시, 그리고 idle/style.load 마다 멱등 재적용.
  // idle 을 거는 이유: 초기 로드 때 onLoad 시점에 isStyleLoaded 가 아직 false 라
  // applyLabels 가 return 되는 경우가 있어 idle 에서 보정. style.load 는 베이스맵 교체 대비.
  useEffect(() => {
    const m = mapRef.current?.getMap();
    if (!m) return;
    const run = () => applyLabels(m, side, showLabels, level, zoomInt, labelData);
    run();
    m.on("idle", run);
    m.on("style.load", run);
    return () => {
      m.off("idle", run);
      m.off("style.load", run);
    };
    // mapReady: onLoad 에서 map 준비되면 true → 첫 렌더에 놓친 effect 를 재실행.
  }, [mapReady, side, showLabels, level, zoomInt, labelData]);

  // diff 레이어는 경계선 위. 라벨은 deck.gl 이 아니라 MapLibre symbol 이므로 여기서 제외.
  // split 드래그처럼 parent 가 자주 리렌더될 때 참조를 안정화 → DeckOverlay 의 setProps 가
  // 실제로 바뀐 경우에만 발생.
  const layers = useMemo(
    () => [...boundaryLayers, ...extraLayers],
    [boundaryLayers, extraLayers],
  );

  const handleMove = useCallback(
    (e: ViewStateChangeEvent) => {
      // 정수 줌만 state 갱신 → emd↔sgg fallback 판단용. 재적용 횟수 최소화.
      const newZoomInt = Math.floor(e.viewState.zoom);
      setZoomInt((prev) => (prev !== newZoomInt ? newZoomInt : prev));
      onMove?.(e);
    },
    [onMove],
  );

  const onLoad = useCallback(() => {
    const m = mapRef.current?.getMap();
    if (!m) return;
    // deck 레이어가 beforeId=anchor 로 삽입되기 전에 anchor 를 먼저 보장.
    // (없으면 deck.gl 의 addLayer 가 "before non-existing layer" 로 throw.)
    ensureDeckAnchor(m);
    applyKoreanLabels(m);
    setBasemapVisible(m, showBasemap);
    setZoomInt(Math.floor(m.getZoom()));
    // map 준비 완료를 알림 → 아래 라벨 useEffect 가 재실행돼 심볼을 적용.
    // (첫 렌더엔 mapRef.current 가 null 이라 effect 가 놓치므로 이 flag 로 트리거.)
    setMapReady(true);
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
      // 한글(CJK) 라벨은 glyph PBF 가 아니라 브라우저 로컬 폰트로 canvas 렌더된다.
      // 이 옵션이 그 로컬 폰트 패밀리를 지정 → 시스템 한글 폰트로 라벨이 그려짐.
      // 없으면 symbol layer 는 생성돼도 한글이 빈 글자로 렌더(renderedCount:0).
      localIdeographFontFamily={
        '"Noto Sans KR", "Malgun Gothic", "Apple SD Gothic Neo", sans-serif'
      }
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
