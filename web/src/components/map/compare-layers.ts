import { GeoJsonLayer } from "@deck.gl/layers";
import type { CompareResult } from "admdongkor";
import type { AdmFeatureCollection } from "admdongkor";

// 변화 유형별 색상 (side 기준이 아닌 시간 기준)
const COLOR_CHANGED: [number, number, number, number] = [234, 179, 8, 180];    // 노랑 (경계변경)
const COLOR_ABOLISHED: [number, number, number, number] = [220, 38, 38, 180];  // 빨강 (폐지 = 과거에만)
const COLOR_CREATED: [number, number, number, number] = [34, 197, 94, 180];    // 초록 (신설 = 최근에만)

const FILL_CHANGED: [number, number, number, number] = [234, 179, 8, 60];
const FILL_ABOLISHED: [number, number, number, number] = [220, 38, 38, 60];
const FILL_CREATED: [number, number, number, number] = [34, 197, 94, 60];

/** va/vb 중 어느 쪽이 과거인지 판단. version_key 문자열 (YYYYMMDD) 비교. */
function getTimeOrientation(result: CompareResult): {
  pastVersion: string;
  recentVersion: string;
  /** A 가 과거인가? */
  aIsPast: boolean;
} {
  const aIsPast = result.va < result.vb;
  return {
    pastVersion: aIsPast ? result.va : result.vb,
    recentVersion: aIsPast ? result.vb : result.va,
    aIsPast,
  };
}

/** compare 결과 + GeoJSON 데이터 → diff 하이라이트 레이어들 반환.
 *  시간 순서에 맞춰 표시:
 *   - 과거에만 있던 emd (폐지) → 빨강, 과거 시점 pane 에 표시
 *   - 최근에만 있던 emd (신설) → 초록, 최근 시점 pane 에 표시
 *   - 경계변경 (changed) → 노랑, 양쪽 pane 모두 해당 시점 기준으로 표시 */
export function buildCompareLayers(
  result: CompareResult,
  side: "A" | "B",
  emdData: AdmFeatureCollection | null,
): GeoJsonLayer[] {
  if (!emdData) return [];

  const { aIsPast } = getTimeOrientation(result);
  const sideIsPast = (side === "A") === aIsPast;
  const sideVersion = side === "A" ? result.va : result.vb;

  // changed: 이 side 의 시점 기준 feature 표시
  const changedIds = new Set(
    result.diff
      .filter((r) => r.status === "changed" && r.version_key === sideVersion)
      .map((r) => r.emdcd),
  );

  // 과거 pane: only_in_{past} → 폐지
  // 최근 pane: only_in_{recent} → 신설
  const statusForThisSide = sideIsPast
    ? (aIsPast ? "only_in_a" : "only_in_b")
    : (aIsPast ? "only_in_b" : "only_in_a");
  const onlyIds = new Set(
    result.diff
      .filter((r) => r.status === statusForThisSide)
      .map((r) => r.emdcd),
  );

  if (changedIds.size === 0 && onlyIds.size === 0) return [];

  const changedFeatures = emdData.features.filter(
    (f) => changedIds.has((f.properties as { emdcd?: string }).emdcd ?? ""),
  );
  const onlyFeatures = emdData.features.filter(
    (f) => onlyIds.has((f.properties as { emdcd?: string }).emdcd ?? ""),
  );

  const layers: GeoJsonLayer[] = [];

  if (changedFeatures.length > 0) {
    layers.push(
      new GeoJsonLayer({
        id: `diff-changed-${side}`,
        data: { type: "FeatureCollection", features: changedFeatures },
        stroked: true,
        filled: true,
        getFillColor: FILL_CHANGED,
        getLineColor: COLOR_CHANGED,
        lineWidthUnits: "pixels",
        getLineWidth: 2,
        pickable: false,
      }),
    );
  }

  if (onlyFeatures.length > 0) {
    // 과거 side 면 폐지(빨강), 최근 side 면 신설(초록)
    const fillColor = sideIsPast ? FILL_ABOLISHED : FILL_CREATED;
    const lineColor = sideIsPast ? COLOR_ABOLISHED : COLOR_CREATED;
    layers.push(
      new GeoJsonLayer({
        id: `diff-only-${side}`,
        data: { type: "FeatureCollection", features: onlyFeatures },
        stroked: true,
        filled: true,
        getFillColor: fillColor,
        getLineColor: lineColor,
        lineWidthUnits: "pixels",
        getLineWidth: 2,
        pickable: false,
      }),
    );
  }

  return layers;
}

export interface DiffSummary {
  /** 경계변경 된 emd 수 (동일 emdcd 양쪽 시점). */
  changed: number;
  /** 과거에만 있던 emd 수 (폐지). */
  abolished: number;
  /** 최근에만 있던 emd 수 (신설). */
  created: number;
}

export function getDiffSummary(result: CompareResult): DiffSummary {
  const { aIsPast } = getTimeOrientation(result);
  const changed = new Set(
    result.diff.filter((r) => r.status === "changed").map((r) => r.emdcd),
  ).size;
  const abolishedStatus = aIsPast ? "only_in_a" : "only_in_b";
  const createdStatus = aIsPast ? "only_in_b" : "only_in_a";
  const abolished = result.diff.filter((r) => r.status === abolishedStatus).length;
  const created = result.diff.filter((r) => r.status === createdStatus).length;
  return { changed, abolished, created };
}
