"""v3 step3 — 공간 겹치는 모든 shape 쌍의 intersection 전수 계산.

v2 와 차이:
    - v2: 같은 element_id 내 shape 쌍만
    - v3: global shape_id 기준, 공간적으로 겹친 적 있는 모든 쌍

접근:
    1. v3 step1 결과 (_spatial_iou_<level>.parquet) 에서 element 쌍 수집
    2. v3 step2 timeline 으로 element → shape_id 매핑
    3. IoU < 0.99 인 쌍 (서로 다른 shape) 을 수집 → shape 쌍으로 변환
    4. 각 shape 쌍에 대해 대표 (rep_version, element_id) 로 도형 로드해 정확한 intersection 계산

Output:
    scripts/_shape_pairs_v3_<level>.parquet
    columns:
        shape_id_a, shape_id_b,
        rep_version_a, rep_element_a,
        rep_version_b, rep_element_b,
        area_a, area_b, area_intersect, w_forward, w_backward, iou

실행:
    python scripts/measure_v3_step3_shape_pairs.py --level sido --workers 60
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import LEVEL_COLS, OUT_DIR, load_level


def _pair_task(args: tuple[str, int, int, str, str, str, str]) -> dict:
    """(sid_a, sid_b) 의 대표 도형 두 개 로드해서 intersection."""
    level, sid_a, sid_b, rep_va, rep_ea, rep_vb, rep_eb = args
    id_col = LEVEL_COLS[level]["id"]
    ga_gdf = load_level(rep_va, level)
    gb_gdf = load_level(rep_vb, level)
    ga_gdf[id_col] = ga_gdf[id_col].astype(str)
    gb_gdf[id_col] = gb_gdf[id_col].astype(str)
    ra = ga_gdf[ga_gdf[id_col] == str(rep_ea)]
    rb = gb_gdf[gb_gdf[id_col] == str(rep_eb)]
    if len(ra) == 0 or len(rb) == 0:
        return {}
    ga = ra.geometry.iloc[0]
    gb = rb.geometry.iloc[0]
    if ga is None or gb is None or ga.is_empty or gb.is_empty:
        return {}
    area_a = ga.area
    area_b = gb.area
    inter = ga.intersection(gb)
    area_int = inter.area if not inter.is_empty else 0.0
    uni = area_a + area_b - area_int
    return {
        "shape_id_a": int(sid_a),
        "shape_id_b": int(sid_b),
        "rep_version_a": rep_va,
        "rep_element_a": rep_ea,
        "rep_version_b": rep_vb,
        "rep_element_b": rep_eb,
        "area_a": float(area_a),
        "area_b": float(area_b),
        "area_intersect": float(area_int),
        "w_forward": float(area_int / area_a) if area_a > 0 else 0.0,
        "w_backward": float(area_int / area_b) if area_b > 0 else 0.0,
        "iou": float(area_int / uni) if uni > 0 else 0.0,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--level", choices=["emd", "sgg", "sido"], required=True)
    ap.add_argument("--workers", type=int, default=60)
    args = ap.parse_args()

    iou_path = OUT_DIR / f"_spatial_iou_{args.level}.parquet"
    tl_path = OUT_DIR / f"_timeline_v3_{args.level}.parquet"
    if not iou_path.exists() or not tl_path.exists():
        print("ERROR: run step1/step2 first", flush=True)
        return 1

    iou = pd.read_parquet(iou_path)
    tl = pd.read_parquet(tl_path)
    print(f"loaded iou: {len(iou):,}, timeline: {len(tl):,}", flush=True)

    # (version_key, element_id) -> shape_id
    tl_key = tl.set_index(["version_key", "element_id"])["shape_id"].to_dict()

    # shape_id -> (rep_version, rep_element_id) — 첫 등장
    rep_map: dict[int, tuple[str, str]] = {}
    for r in tl.sort_values("version_key").itertuples(index=False):
        rep_map.setdefault(int(r.shape_id), (r.version_key, r.element_id))
    print(f"unique shapes: {len(rep_map)}", flush=True)

    # step1 의 edge 중 IoU < 0.99 (다른 shape) 쌍에서 shape_id 쌍 추출
    diff = iou[iou.iou < 0.99]
    shape_pairs_raw: set[tuple[int, int]] = set()
    for r in diff.itertuples(index=False):
        sa = tl_key.get((r.va, r.element_id_a))
        sb = tl_key.get((r.vb, r.element_id_b))
        if sa is None or sb is None or sa == sb:
            continue
        pair = tuple(sorted([int(sa), int(sb)]))
        shape_pairs_raw.add(pair)

    # 공간 crosslink 가 없어서 놓친 쌍은 없나? step1 에 없으면 공간적 교차 없음 → weight 0 → 저장 불필요
    print(f"unique shape pairs to compute: {len(shape_pairs_raw):,}", flush=True)

    tasks = []
    for sa, sb in shape_pairs_raw:
        rep_va, rep_ea = rep_map[sa]
        rep_vb, rep_eb = rep_map[sb]
        tasks.append((args.level, sa, sb, rep_va, rep_ea, rep_vb, rep_eb))

    t0 = time.perf_counter()
    rows: list[dict] = []
    done = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(_pair_task, t) for t in tasks]
        for fut in as_completed(futs):
            done += 1
            try:
                r = fut.result()
            except Exception as e:
                print(f"  [{done}/{len(tasks)}] ERROR: {e}", flush=True)
                continue
            if r:
                rows.append(r)
            if done % 500 == 0 or done == len(tasks):
                elapsed = time.perf_counter() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(tasks) - done) / rate if rate > 0 else 0
                print(f"  [{done}/{len(tasks)}] elapsed={elapsed:.1f}s, rate={rate:.1f}/s, eta={eta:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    out_path = OUT_DIR / f"_shape_pairs_v3_{args.level}.parquet"
    df.to_parquet(out_path, index=False)
    total = time.perf_counter() - t0
    print(f"\ndone in {total:.1f}s", flush=True)
    print(f"output: {out_path}  ({len(df):,} rows, {out_path.stat().st_size/1024:.1f} KB)", flush=True)
    if len(df) > 0:
        print(f"iou: min={df.iou.min():.3f} median={df.iou.median():.3f} max={df.iou.max():.3f}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
