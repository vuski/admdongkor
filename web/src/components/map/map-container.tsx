"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { ViewStateChangeEvent } from "react-map-gl/maplibre";
import { MapPane, type MapPaneHandle } from "./map-pane";
import { useAppStore } from "@/stores/app-store";
import { useAdmdongkorGeoJSON } from "@/hooks/use-admdongkor-geojson";
import { CompareDivider } from "./compare-divider";
import { getFeatureBbox } from "@/hooks/use-admdongkor-geojson";
import { VersionBadge, VersionBadgeA, VersionBadgeB } from "./version-badge";
import { HoverTooltip } from "./hover-tooltip";
import type { HoverInfo } from "./hover-types";

// globals.css 의 --side-a / --side-b 와 맞춤. 지도 sido 선 색상과 동일.
const COLOR_A = "#1c7057";
const COLOR_B = "#c2500f";

export function MapContainer() {
  const versionKey = useAppStore((s) => s.versionKey);
  const versionKeyB = useAppStore((s) => s.versionKeyB);
  const level = useAppStore((s) => s.level);
  const compareMode = useAppStore((s) => s.compareMode);
  const split = useAppStore((s) => s.compareSplit);
  const showBasemap = useAppStore((s) => s.showBasemap);
  const showLabels = useAppStore((s) => s.showLabels);
  const flyToRequest = useAppStore((s) => s.flyToRequest);
  const clearFlyTo = useAppStore((s) => s.clearFlyTo);

  const a = useAdmdongkorGeoJSON(versionKey, level);
  const b = useAdmdongkorGeoJSON(versionKeyB, level);

  const [hover, setHover] = useState<HoverInfo | null>(null);

  // flyToRequest 가 pending 이고 지도 데이터가 로드됐으면 bbox 찾아서 실행.
  useEffect(() => {
    if (!flyToRequest || flyToRequest.status !== "pending") return;
    if (a.loading) return; // 아직 로딩 중 — 완료 후 재실행

    const map = paneA.current?.map?.getMap();
    if (!map) return;

    const execute = async () => {
      let bbox = flyToRequest.bbox;

      // bbox 가 없으면 code 로 feature 를 찾아 계산.
      if (!bbox && flyToRequest.level) {
        bbox = await getFeatureBbox(
          versionKey,
          flyToRequest.level as import("admdongkor").Level,
          flyToRequest.codeField ?? null,
          flyToRequest.codeValue ?? null,
          flyToRequest.nameFields,
        ) ?? undefined;
      }

      if (bbox) {
        const [minLon, minLat, maxLon, maxLat] = bbox;
        map.fitBounds([[minLon, minLat], [maxLon, maxLat]], {
          padding: 60,
          maxZoom: 14,
          duration: 800,
        });
      } else if (flyToRequest.center) {
        map.flyTo({ center: flyToRequest.center, zoom: flyToRequest.zoom ?? 10, duration: 800 });
      }

      clearFlyTo();
    };

    execute();
  }, [flyToRequest, a.loading, versionKey, clearFlyTo]);

  const paneA = useRef<MapPaneHandle>(null);
  const paneB = useRef<MapPaneHandle>(null);
  // 어느 쪽이 "원본 이벤트" 인지 기록 — 상대방 jumpTo 가 onMove 를 다시 트리거해서
  // 양방향 피드백 루프가 돌지 않도록.
  const syncingRef = useRef<"A" | "B" | null>(null);

  const sync = useCallback(
    (from: "A" | "B", e: ViewStateChangeEvent) => {
      if (!compareMode) return;
      if (syncingRef.current && syncingRef.current !== from) return;
      const target = from === "A" ? paneB.current?.map?.getMap() : paneA.current?.map?.getMap();
      if (!target) return;
      syncingRef.current = from;
      target.jumpTo({
        center: [e.viewState.longitude, e.viewState.latitude],
        zoom: e.viewState.zoom,
        bearing: e.viewState.bearing,
        pitch: e.viewState.pitch,
      });
      // 다음 tick 에 lock 해제 — 상대 지도의 render cycle 이 끝난 뒤.
      queueMicrotask(() => {
        syncingRef.current = null;
      });
    },
    [compareMode],
  );

  const onMoveA = useCallback((e: ViewStateChangeEvent) => sync("A", e), [sync]);
  const onMoveB = useCallback((e: ViewStateChangeEvent) => sync("B", e), [sync]);

  // compare 모드 진입 시 B 를 A 로 한 번 맞춤.
  useEffect(() => {
    if (!compareMode) return;
    const mapA = paneA.current?.map?.getMap();
    const mapB = paneB.current?.map?.getMap();
    if (!mapA || !mapB) return;
    syncingRef.current = "A";
    mapB.jumpTo({
      center: mapA.getCenter(),
      zoom: mapA.getZoom(),
      bearing: mapA.getBearing(),
      pitch: mapA.getPitch(),
    });
    queueMicrotask(() => {
      syncingRef.current = null;
    });
  }, [compareMode]);

  // 각 pane 은 전체 container 를 꽉 채우게 렌더 → 두 지도의 절대 좌표가 일치.
  // 시각적으로는 clip-path 로 자르고, pointer event 는 래퍼의 width/left 로 분리.
  const aWidth = compareMode ? `${split * 100}%` : "100%";
  const bLeft = compareMode ? `${split * 100}%` : "0";
  const bWidth = compareMode ? `${(1 - split) * 100}%` : "0";

  return (
    <div className="relative w-full h-full bg-muted overflow-hidden">
      {/* A 래퍼: 좌측 split 폭만 hit-test 수신. 내부 지도는 전체 폭. */}
      <div
        className="absolute top-0 left-0 bottom-0 overflow-hidden"
        style={{ width: aWidth }}
      >
        <div
          className="absolute top-0 left-0 h-full"
          style={{ width: compareMode ? `calc(100% / ${split || 0.0001})` : "100%" }}
        >
          <MapPane
            ref={paneA}
            dataLayers={a.layers}
            side="A"
            level={level}
            showControls={!compareMode}
            showBasemap={showBasemap}
            showLabels={showLabels}
            onMove={onMoveA}
            onHover={setHover}
            interactive={true}
          />
        </div>
      </div>

      {/* B 래퍼: split 우측부터 끝까지만 hit-test. 내부 지도는 전체 폭 + 왼쪽으로 offset. */}
      {compareMode && (
        <div
          className="absolute top-0 bottom-0 overflow-hidden"
          style={{ left: bLeft, width: bWidth }}
        >
          <div
            className="absolute top-0 h-full"
            style={{
              width: `calc(100% / ${(1 - split) || 0.0001})`,
              right: 0,
            }}
          >
            <MapPane
              ref={paneB}
              dataLayers={b.layers}
              side="B"
              level={level}
              showControls={true}
              showBasemap={showBasemap}
              showLabels={showLabels}
              onMove={onMoveB}
              onHover={setHover}
              interactive={true}
            />
          </div>
        </div>
      )}

      {compareMode && <CompareDivider />}

      <VersionBadgeA>
        <VersionBadge
          label={versionKey}
          color={COLOR_A}
          loading={a.loading}
          error={a.error}
        />
      </VersionBadgeA>
      {compareMode && (
        <VersionBadgeB>
          <VersionBadge
            label={versionKeyB}
            color={COLOR_B}
            loading={b.loading}
            error={b.error}
          />
        </VersionBadgeB>
      )}

      <HoverTooltip info={hover} />
    </div>
  );
}
