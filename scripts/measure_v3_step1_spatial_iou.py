"""v3 step1 — 공간 크로스 pairwise IoU.

v2 와 차이:
    - v2: 같은 element_id 끼리만 IoU 계산 (emdcd 재할당·분동·합동 놓침)
    - v3: STRtree 로 공간적으로 겹치는 모든 쌍에 대해 IoU 계산

출력:
    scripts/_spatial_iou_<level>.parquet
    columns: va, vb, element_id_a, element_id_b, iou

실행:
    python scripts/measure_v3_step1_spatial_iou.py --level sido --workers 60
    python scripts/measure_v3_step1_spatial_iou.py --level sgg --workers 60
    python scripts/measure_v3_step1_spatial_iou.py --level emd --workers 60
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import combinations
from pathlib import Path

import pandas as pd
from shapely.strtree import STRtree

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import LEVEL_COLS, OUT_DIR, list_versions, load_level


def _iou(ga, gb) -> float:
    if ga is None or gb is None:
        return 0.0
    inter = ga.intersection(gb).area
    uni = ga.union(gb).area
    return float(inter / uni) if uni > 0 else 0.0


def _pair_task(args: tuple[str, str, str, float]) -> list[dict]:
    """공간 크로스: (va, vb) 쌍에서 공간적으로 겹치는 모든 요소 쌍에 대해 IoU."""
    va, vb, level, min_iou = args
    id_col = LEVEL_COLS[level]["id"]

    a = load_level(va, level)[[id_col, "geometry"]].dropna(subset=[id_col]).copy()
    b = load_level(vb, level)[[id_col, "geometry"]].dropna(subset=[id_col]).copy()
    a[id_col] = a[id_col].astype(str)
    b[id_col] = b[id_col].astype(str)
    a = a.drop_duplicates(id_col).reset_index(drop=True)
    b = b.drop_duplicates(id_col).reset_index(drop=True)

    b_geoms = b.geometry.values
    b_ids = b[id_col].values
    tree = STRtree(b_geoms)

    rows = []
    for _, ra in a.iterrows():
        ga = ra.geometry
        if ga is None or ga.is_empty:
            continue
        cand = tree.query(ga)
        if len(cand) == 0:
            continue
        for bi in cand:
            gb = b_geoms[bi]
            # 실제 교차 확인 (STRtree 는 bbox 기반이라 false positive 있음)
            if not ga.intersects(gb):
                continue
            iou = _iou(ga, gb)
            if iou < min_iou:
                continue
            rows.append({
                "va": va,
                "vb": vb,
                "element_id_a": str(ra[id_col]),
                "element_id_b": str(b_ids[bi]),
                "iou": iou,
            })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--level", choices=["emd", "sgg", "sido"], required=True)
    ap.add_argument("--workers", type=int, default=60)
    ap.add_argument("--min-iou", type=float, default=0.01,
                    help="이 값 이상의 IoU 만 기록 (슬리버 제거)")
    args = ap.parse_args()

    versions = list_versions()
    pairs = list(combinations(versions, 2))
    print(f"level={args.level}, versions={len(versions)}, pairs={len(pairs)}, "
          f"workers={args.workers}, min_iou={args.min_iou}", flush=True)

    t0 = time.perf_counter()
    all_rows: list[dict] = []
    done = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_pair_task, (va, vb, args.level, args.min_iou)): (va, vb)
                for va, vb in pairs}
        for fut in as_completed(futs):
            va, vb = futs[fut]
            done += 1
            try:
                rows = fut.result()
                all_rows.extend(rows)
            except Exception as e:
                print(f"  [{done}/{len(pairs)}] {va}<>{vb} ERROR: {e}", flush=True)
                continue
            if done % 50 == 0 or done == len(pairs):
                elapsed = time.perf_counter() - t0
                rate = done / elapsed
                eta = (len(pairs) - done) / rate if rate > 0 else 0
                print(f"  [{done}/{len(pairs)}] elapsed={elapsed:.1f}s, rate={rate:.1f}/s, "
                      f"eta={eta:.0f}s, rows={len(all_rows):,}", flush=True)

    df = pd.DataFrame(all_rows)
    out_path = OUT_DIR / f"_spatial_iou_{args.level}.parquet"
    df.to_parquet(out_path, index=False)
    total = time.perf_counter() - t0
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\ndone in {total:.1f}s", flush=True)
    print(f"output: {out_path}  ({len(df):,} rows, {size_mb:.1f} MB)", flush=True)

    # 요약
    if len(df) > 0:
        same_element = (df.element_id_a == df.element_id_b).sum()
        diff_element = len(df) - same_element
        high_iou = (df.iou >= 0.99).sum()
        print(f"  same element_id: {same_element:,}", flush=True)
        print(f"  diff element_id (cross): {diff_element:,}", flush=True)
        print(f"  IoU >= 0.99 (same-shape edges): {high_iou:,}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
