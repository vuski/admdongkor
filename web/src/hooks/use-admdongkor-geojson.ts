"use client";

import { useEffect, useState } from "react";
import type { AdmFeatureCollection, Level } from "admdongkor";
import { get } from "admdongkor";

interface State {
  data: AdmFeatureCollection | null;
  loading: boolean;
  error: Error | null;
}

const cache = new Map<string, Promise<AdmFeatureCollection>>();

function keyFor(versionKey: string, level: Level, detail: boolean) {
  return `${level}|${versionKey}|${detail ? "detail" : "light"}`;
}

export function useAdmdongkorGeoJSON(
  versionKey: string,
  level: Level,
  detail: boolean,
): State {
  const [state, setState] = useState<State>({ data: null, loading: true, error: null });

  useEffect(() => {
    let cancelled = false;
    const k = keyFor(versionKey, level, detail);
    let promise = cache.get(k);
    if (!promise) {
      promise = get(versionKey, level, { detail });
      cache.set(k, promise);
    }
    setState({ data: null, loading: true, error: null });
    promise.then(
      (data) => {
        if (cancelled) return;
        setState({ data, loading: false, error: null });
      },
      (error: unknown) => {
        if (cancelled) return;
        cache.delete(k);
        setState({
          data: null,
          loading: false,
          error: error instanceof Error ? error : new Error(String(error)),
        });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [versionKey, level, detail]);

  return state;
}
