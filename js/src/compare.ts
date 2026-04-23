import { loadIndexFile, type LoaderOptions } from "./_index-loader.js";
import { VERSIONS } from "./versions.js";

interface TimelineRow {
  level: string;
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

interface ShapePairRow {
  shape_id_a: bigint;
  shape_id_b: bigint;
  iou: number;
}

export interface CompareRow {
  version_key: string;
  emdcd: string;
  emdnm: string;
  sggcd: string | null;
  sggnm: string | null;
  sidocd: string | null;
  sidonm: string | null;
  shape_id: number;
  /** diff rows only: "changed" | "only_in_a" | "only_in_b". same rows omit. */
  status?: "changed" | "only_in_a" | "only_in_b";
  /** diff rows only. changed → 0–1, only_* → null. */
  iou?: number | null;
}

export interface CompareResult {
  va: string;
  vb: string;
  threshold: number;
  same: CompareRow[];
  diff: CompareRow[];
}

export interface CompareOptions extends LoaderOptions {
  /** shape_id 다를 때 iou >= threshold 면 same 으로 승격. 기본 0.99. */
  threshold?: number;
}

function project(r: TimelineRow): CompareRow {
  return {
    version_key: r.version_key,
    emdcd: r.element_id,
    emdnm: r.name,
    sggcd: r.sggcd,
    sggnm: r.sggnm,
    sidocd: r.sidocd,
    sidonm: r.sidonm,
    shape_id: Number(r.shape_id),
  };
}

function pairKey(a: bigint, b: bigint): string {
  const [lo, hi] = a < b ? [a, b] : [b, a];
  return `${lo}|${hi}`;
}

export async function compare(
  versions: [string, string],
  options: CompareOptions = {},
): Promise<CompareResult> {
  if (!Array.isArray(versions) || versions.length !== 2) {
    throw new Error("versions must be a tuple of exactly 2 version keys");
  }
  const [va, vb] = versions;
  if (typeof va !== "string" || typeof vb !== "string") {
    throw new TypeError("versions items must be strings");
  }
  for (const v of [va, vb]) {
    if (!(VERSIONS as readonly string[]).includes(v)) {
      throw new Error(`unknown version key: ${JSON.stringify(v)}`);
    }
  }
  const threshold = options.threshold ?? 0.99;
  if (!(threshold >= 0 && threshold <= 1)) {
    throw new Error(`threshold must be in [0, 1], got ${threshold}`);
  }

  const [tl, sp] = await Promise.all([
    loadIndexFile<TimelineRow>("timeline_v3_emd.parquet", options),
    loadIndexFile<ShapePairRow>("shape_pairs_v3_emd.parquet", options),
  ]);

  const tlA = tl.filter((r) => r.version_key === va);
  const tlB = tl.filter((r) => r.version_key === vb);
  if (tlA.length === 0) throw new Error(`no timeline rows for version: ${va}`);
  if (tlB.length === 0) throw new Error(`no timeline rows for version: ${vb}`);

  const byIdA = new Map(tlA.map((r) => [r.element_id, r]));
  const byIdB = new Map(tlB.map((r) => [r.element_id, r]));
  const pairIou = new Map<string, number>();
  for (const p of sp) {
    pairIou.set(pairKey(p.shape_id_a, p.shape_id_b), p.iou);
  }

  const sameIds = new Set<string>();
  const changedIou = new Map<string, number>();

  for (const id of byIdA.keys()) {
    const rb = byIdB.get(id);
    if (!rb) continue;
    const ra = byIdA.get(id)!;
    if (ra.shape_id === rb.shape_id) {
      sameIds.add(id);
      continue;
    }
    const iou = pairIou.get(pairKey(ra.shape_id, rb.shape_id)) ?? 0;
    if (iou >= threshold) {
      sameIds.add(id);
    } else {
      changedIou.set(id, iou);
    }
  }

  const onlyA: string[] = [];
  const onlyB: string[] = [];
  for (const id of byIdA.keys()) if (!byIdB.has(id)) onlyA.push(id);
  for (const id of byIdB.keys()) if (!byIdA.has(id)) onlyB.push(id);

  const sameRows: CompareRow[] = [];
  for (const id of sameIds) {
    sameRows.push(project(byIdA.get(id)!));
    sameRows.push(project(byIdB.get(id)!));
  }
  sameRows.sort((x, y) => {
    if (x.emdcd !== y.emdcd) return x.emdcd < y.emdcd ? -1 : 1;
    return x.version_key < y.version_key ? -1 : 1;
  });

  const diffRows: CompareRow[] = [];
  for (const [id, iou] of changedIou) {
    const a = project(byIdA.get(id)!);
    const b = project(byIdB.get(id)!);
    a.status = "changed";
    b.status = "changed";
    a.iou = iou;
    b.iou = iou;
    diffRows.push(a, b);
  }
  for (const id of onlyA) {
    const a = project(byIdA.get(id)!);
    a.status = "only_in_a";
    a.iou = null;
    diffRows.push(a);
  }
  for (const id of onlyB) {
    const b = project(byIdB.get(id)!);
    b.status = "only_in_b";
    b.iou = null;
    diffRows.push(b);
  }
  diffRows.sort((x, y) => {
    if (x.status !== y.status) return (x.status ?? "") < (y.status ?? "") ? -1 : 1;
    if (x.emdcd !== y.emdcd) return x.emdcd < y.emdcd ? -1 : 1;
    return x.version_key < y.version_key ? -1 : 1;
  });

  return { va, vb, threshold, same: sameRows, diff: diffRows };
}
