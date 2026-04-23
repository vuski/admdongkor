"use client";

import { useEffect, useState } from "react";
import type { CompareResult } from "admdongkor";
import { compare } from "admdongkor";

interface State {
  result: CompareResult | null;
  loading: boolean;
  error: Error | null;
  /** 지원되지 않는 시점 조합 (1990 이전 포함). */
  unsupported: boolean;
}

const cache = new Map<string, Promise<CompareResult>>();

function cacheKey(va: string, vb: string) {
  return `${va}|${vb}`;
}

/** timeline_v3_emd.parquet 에 1975/1980/1985 데이터가 없어서 비교 불가. */
function isUnsupported(v: string): boolean {
  return v < "19901231";
}

export function useCompare(
  va: string,
  vb: string,
  enabled: boolean,
): State {
  const [state, setState] = useState<State>({
    result: null,
    loading: false,
    error: null,
    unsupported: false,
  });

  useEffect(() => {
    if (!enabled || va === vb) {
      setState({ result: null, loading: false, error: null, unsupported: false });
      return;
    }
    if (isUnsupported(va) || isUnsupported(vb)) {
      setState({ result: null, loading: false, error: null, unsupported: true });
      return;
    }
    let cancelled = false;
    const k = cacheKey(va, vb);
    let promise = cache.get(k);
    if (!promise) {
      promise = compare([va, vb]);
      cache.set(k, promise);
    }
    setState({ result: null, loading: true, error: null, unsupported: false });
    promise.then(
      (result) => {
        if (cancelled) return;
        setState({ result, loading: false, error: null, unsupported: false });
      },
      (err: unknown) => {
        if (cancelled) return;
        cache.delete(k);
        setState({
          result: null,
          loading: false,
          error: err instanceof Error ? err : new Error(String(err)),
          unsupported: false,
        });
      },
    );
    return () => { cancelled = true; };
  }, [va, vb, enabled]);

  return state;
}
