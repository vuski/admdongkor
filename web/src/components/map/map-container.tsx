"use client";

import { useCallback, useEffect, useRef } from "react";
import type { ViewStateChangeEvent } from "react-map-gl/maplibre";
import { MapPane, type MapPaneHandle } from "./map-pane";
import { useAppStore } from "@/stores/app-store";
import { useAdmdongkorGeoJSON } from "@/hooks/use-admdongkor-geojson";
import { CompareDivider } from "./compare-divider";
import { VersionBadge } from "./version-badge";

export function MapContainer() {
  const versionKey = useAppStore((s) => s.versionKey);
  const versionKeyB = useAppStore((s) => s.versionKeyB);
  const level = useAppStore((s) => s.level);
  const detail = useAppStore((s) => s.detail);
  const compareMode = useAppStore((s) => s.compareMode);
  const split = useAppStore((s) => s.compareSplit);

  const a = useAdmdongkorGeoJSON(versionKey, level, detail);
  const b = useAdmdongkorGeoJSON(versionKeyB, level, detail);

  const paneA = useRef<MapPaneHandle>(null);
  const paneB = useRef<MapPaneHandle>(null);
  const syncingRef = useRef<"A" | "B" | null>(null);

  // A 움직이면 B 따라옴 (compare 모드에서).
  const onMoveA = useCallback(
    (e: ViewStateChangeEvent) => {
      if (!compareMode) return;
      if (syncingRef.current === "B") return;
      const mapB = paneB.current?.map?.getMap();
      if (!mapB) return;
      syncingRef.current = "A";
      mapB.jumpTo({
        center: [e.viewState.longitude, e.viewState.latitude],
        zoom: e.viewState.zoom,
        bearing: e.viewState.bearing,
        pitch: e.viewState.pitch,
      });
      queueMicrotask(() => {
        syncingRef.current = null;
      });
    },
    [compareMode],
  );

  // compare 모드 진입 시 B 를 A 로 한 번 맞춤.
  useEffect(() => {
    if (!compareMode) return;
    const mapA = paneA.current?.map?.getMap();
    const mapB = paneB.current?.map?.getMap();
    if (!mapA || !mapB) return;
    mapB.jumpTo({
      center: mapA.getCenter(),
      zoom: mapA.getZoom(),
      bearing: mapA.getBearing(),
      pitch: mapA.getPitch(),
    });
  }, [compareMode]);

  const clipB = compareMode
    ? `inset(0 0 0 ${split * 100}%)`
    : "inset(0 0 0 100%)";

  return (
    <div className="relative w-full h-full bg-[var(--muted)]">
      <div className="absolute inset-0">
        <MapPane
          ref={paneA}
          data={a.data}
          level={level}
          side="A"
          showControls={!compareMode}
          onMove={onMoveA}
          interactive={true}
        />
      </div>
      <div
        className="absolute inset-0 pointer-events-none"
        style={{ clipPath: clipB, WebkitClipPath: clipB }}
      >
        <div
          className={compareMode ? "absolute inset-0 pointer-events-auto" : "absolute inset-0"}
        >
          <MapPane
            ref={paneB}
            data={b.data}
            level={level}
            side="B"
            showControls={false}
            interactive={false}
          />
        </div>
      </div>

      {compareMode && <CompareDivider />}

      <VersionBadge
        position="top-left"
        label={versionKey}
        color="#2563eb"
        loading={a.loading}
        error={a.error}
      />
      {compareMode && (
        <VersionBadge
          position="top-right"
          label={versionKeyB}
          color="#f97316"
          loading={b.loading}
          error={b.error}
        />
      )}
    </div>
  );
}
