"use client";

import { useEffect, useState } from "react";
import type { AdmFeatureCollection, Level } from "admdongkor";
import { get } from "admdongkor";

export interface LayerData {
  level: Level;
  data: AdmFeatureCollection;
}

interface State {
  /** 요청한 levels 중 로드된 것들. emd → sgg → sido 순으로 정렬. */
  layers: LayerData[];
  loading: boolean;
  error: Error | null;
}

export const geojsonCache = new Map<string, Promise<AdmFeatureCollection>>();

function keyFor(versionKey: string, level: Level) {
  return `${level}|${versionKey}|light`;
}

/** 캐시에서 feature 를 찾아 bbox 를 반환. 캐시 miss 시 null.
 *  codeField/codeValue 로 먼저 찾고, 없으면 nameFields 로 fallback. */
export async function getFeatureBbox(
  versionKey: string,
  level: Level,
  codeField: string | null,
  codeValue: string | null,
  nameFields?: Record<string, string>, // 예: { sidonm: "경기도", sggnm: "수원시" }
): Promise<[number, number, number, number] | null> {
  const key = keyFor(versionKey, level);
  const promise = geojsonCache.get(key);
  if (!promise) return null;
  try {
    const fc = await promise;
    let feature = codeField && codeValue
      ? fc.features.find(
          (f) => (f.properties as unknown as Record<string, unknown>)[codeField] === codeValue,
        )
      : undefined;

    // 코드로 못 찾으면 이름으로 fallback
    if (!feature && nameFields) {
      feature = fc.features.find((f) => {
        const p = f.properties as unknown as Record<string, unknown>;
        return Object.entries(nameFields).every(([k, v]) => p[k] === v);
      });
    }

    if (!feature) return null;
    return geometryBbox(feature.geometry);
  } catch {
    return null;
  }
}

function geometryBbox(
  geom: GeoJSON.Polygon | GeoJSON.MultiPolygon,
): [number, number, number, number] {
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
  const rings =
    geom.type === "Polygon" ? geom.coordinates : geom.coordinates.flatMap((p) => p);
  for (const ring of rings) {
    for (const [lon, lat] of ring) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
  }
  return [minLon, minLat, maxLon, maxLat];
}

/** 선택한 level 과 그 상위 레벨까지 한 번에 로드 (항상 light).
 *  level=emd → [emd, sgg, sido]
 *  level=sgg → [sgg, sido]
 *  level=sido → [sido]
 *  배열 순서는 emd → sgg → sido (그리는 순서). */
export function useAdmdongkorGeoJSON(versionKey: string, level: Level): State {
  const [state, setState] = useState<State>({
    layers: [],
    loading: true,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;
    const levels = levelsUpTo(level);

    setState({ layers: [], loading: true, error: null });

    const promises = levels.map((lvl) => {
      const k = keyFor(versionKey, lvl);
      let p = geojsonCache.get(k);
      if (!p) {
        p = get(versionKey, lvl, { detail: false });
        geojsonCache.set(k, p);
      }
      return p.then((data) => ({ level: lvl, data }));
    });

    Promise.all(promises).then(
      (layers) => {
        if (cancelled) return;
        setState({ layers, loading: false, error: null });
      },
      (error: unknown) => {
        if (cancelled) return;
        // 실패한 key 만 캐시에서 제거
        for (const lvl of levels) geojsonCache.delete(keyFor(versionKey, lvl));
        setState({
          layers: [],
          loading: false,
          error: error instanceof Error ? error : new Error(String(error)),
        });
      },
    );

    return () => {
      cancelled = true;
    };
  }, [versionKey, level]);

  return state;
}

function levelsUpTo(level: Level): Level[] {
  if (level === "sido") return ["sido"];
  if (level === "sgg") return ["sgg", "sido"];
  return ["emd", "sgg", "sido"];
}
