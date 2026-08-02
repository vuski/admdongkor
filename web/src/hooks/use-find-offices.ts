"use client";

import { useEffect, useState } from "react";
import type { OfficeRow } from "admdongkor";
import { findOffices } from "admdongkor";

interface State {
  rows: OfficeRow[];
  loading: boolean;
}

/**
 * 출장소 검색. 출장소는 경계 지도가 없어 `find()` 로는 안 나오므로 별도 조회.
 *
 * 에러는 삼키고 빈 배열로 둔다 — 출장소는 부가 정보라, 이것 때문에 일반 검색
 * 결과까지 에러 화면으로 가려지면 안 된다.
 */
export function useFindOffices(query: string, debounceMs = 250): State {
  const [state, setState] = useState<State>({ rows: [], loading: false });

  useEffect(() => {
    const trimmed = query.trim();
    if (!trimmed) {
      setState({ rows: [], loading: false });
      return;
    }
    const controller = new AbortController();
    const handle = window.setTimeout(async () => {
      setState((s) => ({ ...s, loading: true }));
      try {
        const rows = await findOffices(trimmed, { signal: controller.signal });
        if (controller.signal.aborted) return;
        setState({ rows, loading: false });
      } catch {
        if (controller.signal.aborted) return;
        setState({ rows: [], loading: false });
      }
    }, debounceMs);
    return () => {
      controller.abort();
      window.clearTimeout(handle);
    };
  }, [query, debounceMs]);

  return state;
}
