/** 기본 추적 연도 선택 로직.
 *  "1975 부터 5년마다 + 최근 버전" — 한 해에 여러 버전이 있으면 그 해의 마지막 것.
 *  해당 연도에 데이터 없으면 skip. */

export function yearOf(versionKey: string): number {
  return Number(versionKey.slice(0, 4));
}

export function pickDefaultVersions(
  allVersions: string[],
  step = 5,
  baseVersion?: string,
): string[] {
  if (allVersions.length === 0) return [];
  const sorted = [...allVersions].sort();
  const byYear = new Map<number, string[]>();
  for (const v of sorted) {
    const y = yearOf(v);
    const arr = byYear.get(y) ?? [];
    arr.push(v);
    byYear.set(y, arr);
  }
  // 한 해의 '마지막' 버전.
  const lastOfYear = new Map<number, string>();
  for (const [y, arr] of byYear) {
    lastOfYear.set(y, arr[arr.length - 1]!);
  }

  const minYear = yearOf(sorted[0]!);
  const maxYear = yearOf(sorted[sorted.length - 1]!);
  const origin = minYear;
  const picked = new Set<string>();

  // 1975 부터 step 년 간격.
  for (let y = origin; y <= maxYear; y += step) {
    const v = lastOfYear.get(y);
    if (v) picked.add(v);
  }

  // 최근 버전 (전체 배열의 마지막) 도 강제 포함.
  picked.add(sorted[sorted.length - 1]!);

  // 기준 버전은 반드시 포함.
  if (baseVersion && sorted.includes(baseVersion)) picked.add(baseVersion);

  return [...picked].sort();
}

export function fmtVersionLabel(v: string): string {
  return `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}`;
}
