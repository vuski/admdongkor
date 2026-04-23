"use client";

import { forwardRef, useImperativeHandle, useRef } from "react";
import { Map, NavigationControl } from "react-map-gl/maplibre";
import type { MapRef, ViewStateChangeEvent } from "react-map-gl/maplibre";
import { GeoJsonLayer } from "@deck.gl/layers";
import type { AdmFeatureCollection, Level } from "admdongkor";
import { DeckOverlay } from "./deck-overlay";
import { OSM_STYLE, INITIAL_VIEW } from "./map-style";
import {
  lineWidthForLevel,
  SIDE_A_FILL,
  SIDE_A_LINE,
  SIDE_B_FILL,
  SIDE_B_LINE,
} from "./layer-colors";

export interface MapPaneHandle {
  map: MapRef | null;
}

interface Props {
  data: AdmFeatureCollection | null;
  level: Level;
  side: "A" | "B";
  showControls: boolean;
  onMove?: (e: ViewStateChangeEvent) => void;
  interactive: boolean;
}

export const MapPane = forwardRef<MapPaneHandle, Props>(function MapPane(
  { data, level, side, showControls, onMove, interactive },
  ref,
) {
  const mapRef = useRef<MapRef>(null);
  useImperativeHandle(ref, () => ({ get map() { return mapRef.current; } }), []);

  const fill = side === "A" ? SIDE_A_FILL : SIDE_B_FILL;
  const line = side === "A" ? SIDE_A_LINE : SIDE_B_LINE;

  const layers = data
    ? [
        new GeoJsonLayer({
          id: `adm-${side}-${level}`,
          data,
          stroked: true,
          filled: true,
          getFillColor: fill,
          getLineColor: line,
          lineWidthUnits: "pixels",
          getLineWidth: lineWidthForLevel(level),
          pickable: false,
        }),
      ]
    : [];

  return (
    <Map
      ref={mapRef}
      initialViewState={INITIAL_VIEW}
      mapStyle={OSM_STYLE}
      onMove={onMove}
      interactive={interactive}
      attributionControl={side === "A" ? undefined : false}
      style={{ width: "100%", height: "100%" }}
    >
      {showControls && <NavigationControl position="bottom-right" />}
      <DeckOverlay layers={layers} />
    </Map>
  );
});
