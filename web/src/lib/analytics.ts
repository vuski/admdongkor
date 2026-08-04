"use client";

/**
 * Google Analytics 4 커스텀 이벤트 전송.
 *
 * GA 스크립트는 `NEXT_PUBLIC_GA_ID` 가 있을 때만 로드된다 (layout.tsx).
 * 그래서 fork·로컬 개발에서는 `gtag` 가 없고, 여기서 조용히 무시한다 —
 * 분석 코드 때문에 기능이 깨지는 일은 없어야 한다.
 *
 * ## GA4 파라미터 제약 (등록 안 하면 리포트에 안 보임)
 *
 * 커스텀 파라미터는 GA4 콘솔에서 **맞춤 측정기준**으로 등록해야 표준
 * 리포트에 나타난다. 등록 이전에 쌓인 데이터는 소급 적용되지 않는다.
 * (DebugView / BigQuery export 에서는 등록 없이도 보인다.)
 *
 * - 이벤트 이름: 40자 이내, 영문·숫자·`_`
 * - 파라미터 이름: 40자 이내 / 값: 100자 이내
 * - 이벤트당 파라미터 25개까지
 */

type GtagFn = (
  command: "event" | "config" | "js",
  target: string | Date,
  params?: Record<string, unknown>,
) => void;

declare global {
  interface Window {
    gtag?: GtagFn;
  }
}

/** 이벤트 이름 — 오타로 조용히 유실되지 않게 한곳에 모은다. */
export const GA_EVENT = {
  /** 시점 슬라이더를 놓았을 때 (드래그 중이 아니라 확정 시점). */
  versionChange: "version_change",
  /** 우측 패널에서 행정구역을 검색했을 때 (디바운스 후 결과 확정). */
  searchQuery: "search_query",
  /** 검색 결과를 클릭해 지도를 이동했을 때. */
  searchResultPick: "search_result_pick",
  /** 시계열 추적을 시작했을 때. */
  timelineTrack: "timeline_track",
  /** 다운로드를 시작했을 때 (옵션 조합 포함). */
  downloadStart: "download_start",
  /** 다운로드가 끝났을 때 (성공/실패/취소 + 소요 시간). */
  downloadResult: "download_result",
} as const;

/**
 * 커스텀 이벤트 전송. gtag 가 없으면(GA 미설정) 아무 일도 하지 않는다.
 *
 * 값은 GA4 제한에 맞춰 문자열 100자로 자른다.
 */
export function track(
  event: string,
  params: Record<string, string | number | boolean | undefined> = {},
): void {
  if (typeof window === "undefined" || typeof window.gtag !== "function") return;

  const clean: Record<string, string | number | boolean> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    clean[k] = typeof v === "string" && v.length > 100 ? v.slice(0, 100) : v;
  }

  try {
    window.gtag("event", event, clean);
  } catch {
    // 분석 실패가 사용자 기능을 막지 않도록 삼킨다.
  }
}
