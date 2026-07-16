import polylabel from "@mapbox/polylabel";
import type {
  AdmFeature,
  AdmFeatureCollection,
  EmdProperties,
  Level,
  SggProperties,
  SidoProperties,
} from "admdongkor";
import { shortSido } from "@/lib/sido-short";

export interface LabelDatum {
  /** 화면 위치 (lon, lat). polygon 의 근사 중심. */
  position: [number, number];
  /** 실제 그릴 텍스트 (레벨 규칙대로 줄바꿈 포함). */
  text: string;
  /** 충돌 우선순위 — 면적 큰 애가 살아남음. */
  priority: number;
  /** 디버그 키. */
  id: string;
}

/** Ring bbox 의 면적 (경위도 단위). MultiPolygon 에서 가장 큰 조각 선택용. */
function ringBboxArea(ring: number[][]): number {
  if (ring.length === 0) return 0;
  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity;
  for (const p of ring) {
    const x = p[0];
    const y = p[1];
    if (typeof x !== "number" || typeof y !== "number") continue;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }
  return Math.max(0, (maxX - minX) * (maxY - minY));
}

/** @mapbox/polylabel: polygon 내부의 "가장 안쪽 깊숙한 점" — concave 도 항상 내부에 찍힘. */
function featurePosition(feature: AdmFeature): [number, number] {
  const g = feature.geometry;
  const precision = 0.001; // 경위도 단위 — 약 100m 해상도, 지도 레이블엔 충분
  if (g.type === "Polygon") {
    const p = polylabel(g.coordinates as number[][][], precision);
    return [p[0], p[1]];
  }
  // MultiPolygon: bbox 면적 최대 polygon 기준 (본토 vs 섬).
  let best: number[][][] | null = null;
  let bestArea = -1;
  for (const poly of g.coordinates) {
    const outer = poly[0] ?? [];
    const area = ringBboxArea(outer);
    if (area > bestArea) {
      bestArea = area;
      best = poly as number[][][];
    }
  }
  if (!best) return [0, 0];
  const p = polylabel(best, precision);
  return [p[0], p[1]];
}

export function buildLabels(
  fc: AdmFeatureCollection | null,
  level: Level,
): LabelDatum[] {
  if (!fc) return [];
  const out: LabelDatum[] = [];
  for (const f of fc.features) {
    const pos = featurePosition(f);
    const props = f.properties;
    let text = "";
    let id = "";
    if (level === "sido") {
      const p = props as SidoProperties;
      text = p.sidonm;
      id = `sido|${p.sidocd ?? p.sidonm}`;
    } else if (level === "sgg") {
      const p = props as SggProperties;
      text = `${shortSido(p.sidonm)}\n${p.sggnm}`;
      id = `sgg|${p.sggcd ?? `${p.sidonm}${p.sggnm}`}`;
    } else {
      const p = props as EmdProperties;
      text = `${p.sggnm ?? ""}\n${p.emdnm}`.trim();
      id = `emd|${p.emdcd ?? `${p.sggnm}${p.emdnm}`}`;
    }
    // priority 는 CollisionFilterExtension 에서 GPU float 로 다뤄진다. m² 단위
    // 원본 area 는 수억까지 커서 내부 연산이 불안정 → log10 으로 0-10 범위로 정규화.
    const pr = props.area > 0 ? Math.log10(props.area) : 0;
    out.push({
      position: pos,
      text,
      priority: pr,
      id,
    });
  }
  return out;
}
