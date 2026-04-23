import { loadIndexFile, type LoaderOptions } from "./_index-loader.js";
import { VERSIONS } from "./versions.js";

interface TimelineEmdRow {
  version_key: string;
  element_id: string;
  shape_id: bigint;
  name: string;
  emd7: string | null;
  emd8: string | null;
  sggcd: string | null;
  sggnm: string | null;
  sidocd: string | null;
  sidonm: string | null;
  area: number;
}

interface TimelineLevelRow {
  version_key: string;
  element_id: string;
  name: string;
  area: number;
  sggcd?: string | null;
  sggnm?: string | null;
  sidocd?: string | null;
  sidonm?: string | null;
}

interface ShapePairRow {
  shape_id_a: bigint;
  shape_id_b: bigint;
  w_forward: number;
  w_backward: number;
}

export interface MatchEmdRow {
  version_key: string;
  emdcd: string;
  emdnm: string;
  sggcd: string | null;
  sggnm: string | null;
  sidocd: string | null;
  sidonm: string | null;
  area: number;
  weight: number;
}

export interface MatchSggRow {
  version_key: string;
  sggcd: string;
  sggnm: string;
  sidocd: string | null;
  sidonm: string | null;
  area: number;
  weight: number;
}

export interface MatchSidoRow {
  version_key: string;
  sidocd: string;
  sidonm: string;
  area: number;
  weight: number;
}

export interface MatchOptions extends LoaderOptions {
  base: string;
  region: string;
  target: string | string[];
  /** weight 가 이 값 미만이면 결과에서 제외. 기본 0. */
  minWeight?: number;
}

export interface MatchResult {
  base: string;
  region: string;
  targets: string[];
  emd: MatchEmdRow[];
  sgg: () => Promise<MatchSggRow[]>;
  sido: () => Promise<MatchSidoRow[]>;
}

function clamp01(x: number): number {
  return x > 1 ? 1 : x < 0 ? 0 : x;
}

function resolveRegionMask(
  tl: TimelineEmdRow[],
  base: string,
  region: string,
): TimelineEmdRow[] {
  const L = region.length;
  switch (L) {
    case 2:
      return tl.filter((r) => r.version_key === base && r.sidocd === region);
    case 5:
      return tl.filter((r) => r.version_key === base && r.sggcd === region);
    case 7:
      return tl.filter((r) => r.version_key === base && r.emd7 === region);
    case 10:
      return tl.filter((r) => r.version_key === base && r.element_id === region);
    default:
      throw new Error(
        `region must be 2/5/7/10 digit code, got length=${L}: ${JSON.stringify(region)}`,
      );
  }
}

export async function matchAdm(opts: MatchOptions): Promise<MatchResult> {
  const { base, region, target, minWeight = 0 } = opts;

  if (typeof base !== "string") throw new TypeError("base must be string");
  if (typeof region !== "string") throw new TypeError("region must be string");
  if (!(VERSIONS as readonly string[]).includes(base)) {
    throw new Error(`unknown base version key: ${JSON.stringify(base)}`);
  }
  const targets = Array.isArray(target) ? target : [target];
  if (targets.length === 0) throw new Error("target must have at least one version key");
  for (const t of targets) {
    if (typeof t !== "string") throw new TypeError("target items must be strings");
    if (!(VERSIONS as readonly string[]).includes(t)) {
      throw new Error(`unknown target version key: ${JSON.stringify(t)}`);
    }
  }

  const [tl, sp] = await Promise.all([
    loadIndexFile<TimelineEmdRow>("timeline_v3_emd.parquet", opts),
    loadIndexFile<ShapePairRow>("shape_pairs_v3_emd.parquet", opts),
  ]);

  const baseRows = resolveRegionMask(tl, base, region);
  if (baseRows.length === 0) {
    return emptyResult(base, region, targets, opts);
  }

  const baseShapes = new Set(baseRows.map((r) => r.shape_id));

  // shape_pairs → {otherShape: summed weight toward target}
  // base shape 가 A 측이면 w_backward 사용 (base ∩ other) / area(other)
  // base shape 가 B 측이면 w_forward 사용
  const relatedWeight = new Map<bigint, number>();
  const addRelated = (other: bigint, w: number) => {
    if (w < minWeight) return;
    relatedWeight.set(other, (relatedWeight.get(other) ?? 0) + w);
  };
  for (const p of sp) {
    if (baseShapes.has(p.shape_id_a)) addRelated(p.shape_id_b, p.w_backward);
    if (baseShapes.has(p.shape_id_b)) addRelated(p.shape_id_a, p.w_forward);
  }

  const byVersion = new Map<string, TimelineEmdRow[]>();
  for (const t of targets) byVersion.set(t, []);
  for (const r of tl) {
    const bucket = byVersion.get(r.version_key);
    if (bucket) bucket.push(r);
  }

  const emdOut: MatchEmdRow[] = [];
  for (const t of targets) {
    const rows = byVersion.get(t) ?? [];
    // (element_id) → accumulated weight
    const accum = new Map<string, { row: TimelineEmdRow; weight: number }>();
    for (const r of rows) {
      let w = 0;
      if (baseShapes.has(r.shape_id)) {
        w = 1;
      } else {
        const rel = relatedWeight.get(r.shape_id);
        if (rel !== undefined) w = rel;
      }
      if (w === 0) continue;
      const existing = accum.get(r.element_id);
      if (existing) existing.weight += w;
      else accum.set(r.element_id, { row: r, weight: w });
    }
    for (const { row, weight } of accum.values()) {
      const clamped = clamp01(weight);
      if (clamped < minWeight) continue;
      emdOut.push({
        version_key: row.version_key,
        emdcd: row.element_id,
        emdnm: row.name,
        sggcd: row.sggcd,
        sggnm: row.sggnm,
        sidocd: row.sidocd,
        sidonm: row.sidonm,
        area: row.area,
        weight: clamped,
      });
    }
  }

  emdOut.sort((a, b) => {
    if (a.version_key !== b.version_key)
      return a.version_key < b.version_key ? -1 : 1;
    return b.weight - a.weight;
  });

  return {
    base,
    region,
    targets,
    emd: emdOut,
    sgg: () => aggregateToLevel(emdOut, "sgg", opts),
    sido: () => aggregateToLevel(emdOut, "sido", opts),
  };
}

function emptyResult(
  base: string,
  region: string,
  targets: string[],
  opts: LoaderOptions,
): MatchResult {
  return {
    base,
    region,
    targets,
    emd: [],
    sgg: async () => [],
    sido: async () => {
      void opts;
      return [];
    },
  };
}

async function aggregateToLevel(
  emd: MatchEmdRow[],
  level: "sgg",
  opts: LoaderOptions,
): Promise<MatchSggRow[]>;
async function aggregateToLevel(
  emd: MatchEmdRow[],
  level: "sido",
  opts: LoaderOptions,
): Promise<MatchSidoRow[]>;
async function aggregateToLevel(
  emd: MatchEmdRow[],
  level: "sgg" | "sido",
  opts: LoaderOptions,
): Promise<MatchSggRow[] | MatchSidoRow[]> {
  if (emd.length === 0) return [] as MatchSggRow[] | MatchSidoRow[];

  const codeKey = level === "sgg" ? "sggcd" : "sidocd";
  const nameKey = level === "sgg" ? "sggnm" : "sidonm";

  // 분자: Σ(weight × area) per (version, code)
  interface Acc {
    version_key: string;
    code: string;
    name: string;
    sidocd: string | null;
    sidonm: string | null;
    wa: number;
  }
  const accum = new Map<string, Acc>();
  for (const r of emd) {
    const code = r[codeKey];
    if (code === null) continue;
    const key = `${r.version_key}|${code}`;
    const existing = accum.get(key);
    const wa = r.weight * r.area;
    if (existing) {
      existing.wa += wa;
    } else {
      accum.set(key, {
        version_key: r.version_key,
        code,
        name: (r[nameKey] as string | null) ?? "",
        sidocd: r.sidocd,
        sidonm: r.sidonm,
        wa,
      });
    }
  }

  // 분모: 해당 level 의 total area
  const levelTl = await loadIndexFile<TimelineLevelRow>(
    `timeline_v3_${level}.parquet`,
    opts,
  );
  const totalArea = new Map<string, number>();
  for (const r of levelTl) {
    totalArea.set(`${r.version_key}|${r.element_id}`, r.area);
  }

  if (level === "sgg") {
    const out: MatchSggRow[] = [];
    for (const a of accum.values()) {
      const total = totalArea.get(`${a.version_key}|${a.code}`) ?? a.wa;
      out.push({
        version_key: a.version_key,
        sggcd: a.code,
        sggnm: a.name,
        sidocd: a.sidocd,
        sidonm: a.sidonm,
        area: total,
        weight: clamp01(a.wa / total),
      });
    }
    out.sort((x, y) => {
      if (x.version_key !== y.version_key)
        return x.version_key < y.version_key ? -1 : 1;
      return y.weight - x.weight;
    });
    return out;
  } else {
    const out: MatchSidoRow[] = [];
    for (const a of accum.values()) {
      const total = totalArea.get(`${a.version_key}|${a.code}`) ?? a.wa;
      out.push({
        version_key: a.version_key,
        sidocd: a.code,
        sidonm: a.name,
        area: total,
        weight: clamp01(a.wa / total),
      });
    }
    out.sort((x, y) => {
      if (x.version_key !== y.version_key)
        return x.version_key < y.version_key ? -1 : 1;
      return y.weight - x.weight;
    });
    return out;
  }
}
