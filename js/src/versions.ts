// prettier-ignore
// === AUTO-GENERATED VERSIONS START (rebuild_all.py 가 parquet/ 스캔으로 갱신. 이 블록은 손대지 말 것) ===
export const VERSIONS = [
  "19751231", "19801231", "19851231", "19901231", "19951231", "20001231",
  "20011231", "20021231", "20031231", "20041231", "20051231", "20061231",
  "20071231", "20081231", "20091231", "20101231", "20111231", "20121210",
  "20121231", "20131231", "20141231", "20151231", "20160201", "20170418",
  "20170801", "20171016", "20180301", "20180401", "20180724", "20181106",
  "20190403", "20190908", "20191001", "20191231", "20200101", "20200701",
  "20201001", "20210101", "20210401", "20210701", "20220101", "20220309",
  "20220401", "20220701", "20221001", "20230101", "20230401", "20230701",
  "20231001", "20231231", "20240101", "20240401", "20240701", "20241001",
  "20241231", "20250101", "20250401", "20250701", "20251001", "20251231",
  "20260201", "20260401", "20260701",
] as const;
// === AUTO-GENERATED VERSIONS END ===

export type VersionKey = typeof VERSIONS[number];

import { fetchManifest } from "./changelog.js";
import type { LoaderOptions } from "./_index-loader.js";

function filterByYear(keys: readonly string[], year?: number): string[] {
  if (year === undefined) return [...keys];
  if (!Number.isInteger(year)) {
    throw new TypeError(`year must be an integer, got ${typeof year}`);
  }
  const prefix = String(year).padStart(4, "0");
  return keys.filter((k) => k.startsWith(prefix));
}

/**
 * 버전 키 목록 (동기).
 *
 * 이 배열은 **패키지 배포 시점의 스냅샷**이다. 새 시점 데이터가 dist/data/ 에
 * 추가돼도 이 상수는 재배포 전까지 갱신되지 않는다. 최신 목록이 필요하면
 * {@link versionsAsync} 를 쓸 것 (manifest.json 을 런타임에 읽어 항상 최신).
 *
 * get/compare/matchAdm 의 입력 검증에도 이 동기 상수를 쓴다 (하위호환 유지).
 */
export function versions(year?: number): string[] {
  return filterByYear(VERSIONS, year);
}

/**
 * 버전 키 목록 (비동기, 런타임 최신).
 *
 * 원격 `manifest.json` 의 `versions` 배열을 읽으므로, 라이브러리 재배포 없이
 * dist/data/ 데이터만 갱신돼도 최신 시점까지 반영된다. manifest 에 versions 가
 * 없는 옛 배포이거나 네트워크 실패 시 동기 {@link VERSIONS} 스냅샷으로 fallback.
 *
 * @param year  4자리 연도로 필터 (예: 2025). 생략 시 전체.
 * @param opts  baseUrl / fetch / signal (LoaderOptions).
 */
export async function versionsAsync(
  year?: number,
  opts: LoaderOptions = {},
): Promise<string[]> {
  let keys: readonly string[] = VERSIONS;
  try {
    const m = await fetchManifest(opts);
    if (Array.isArray(m.versions) && m.versions.length > 0) {
      keys = m.versions;
    }
  } catch {
    // 네트워크 실패 등 → 동기 스냅샷으로 fallback
  }
  return filterByYear(keys, year);
}
