"use client";

import { useEffect, useState } from "react";
import type { FindRow } from "admdongkor";
import { find } from "admdongkor";

interface State {
  rows: FindRow[];
  loading: boolean;
  error: Error | null;
}

export function useFind(query: string, debounceMs = 250): State {
  const [state, setState] = useState<State>({ rows: [], loading: false, error: null });

  useEffect(() => {
    const trimmed = query.trim();
    if (!trimmed) {
      setState({ rows: [], loading: false, error: null });
      return;
    }
    const controller = new AbortController();
    const handle = window.setTimeout(async () => {
      setState((s) => ({ ...s, loading: true, error: null }));
      try {
        const rows = await find(trimmed, { signal: controller.signal });
        setState({ rows, loading: false, error: null });
      } catch (e) {
        if (controller.signal.aborted) return;
        setState({
          rows: [],
          loading: false,
          error: e instanceof Error ? e : new Error(String(e)),
        });
      }
    }, debounceMs);
    return () => {
      controller.abort();
      window.clearTimeout(handle);
    };
  }, [query, debounceMs]);

  return state;
}
