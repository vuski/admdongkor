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

최적화 (2026-04-24):
    - 워커 initializer 로 필요한 버전들만 한 번씩 선로드 (기존: 쌍마다 load_level 2회).
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


# 워커 전역 캐시: version -> dict[element_id_str, geometry]
_CACHE: dict[str, dict[str, object]] = {}


def _init_worker(level: str, versions: list[str]) -> None:
    id_col = LEVEL_COLS[level]["id"]
    for v in versions:
        gdf = load_level(v, level)
        gdf = gdf[[id_col, "geometry"]].dropna(subset=[id_col])
        # 중복 id 가 있을 때 기존 구현(load_level + iloc[0])과 맞추기 위해
        # 첫 등장만 유지. dict(zip) 은 마지막 값이 덮어써서 불일치 발생했음.
        cache: dict[str, object] = {}
        for k, g in zip(gdf[id_col].astype(str).values, gdf.geometry.values):
            if k not in cache:
                cache[k] = g
        _CACHE[v] = cache


def _pair_task(args: tuple[str, int, int, str, str, str, str]) -> dict:
    level, sid_a, sid_b, rep_va, rep_ea, rep_vb, rep_eb = args
    ga = _CACHE[rep_va].get(str(rep_ea))
    gb = _CACHE[rep_vb].get(str(rep_eb))
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
    ap.add_argument("--iou-in", type=str, default=None,
                    help="step1 결과 경로 override (기본: scripts/_spatial_iou_<level>.parquet)")
    ap.add_argument("--timeline-in", type=str, default=None,
                    help="step2 결과 경로 override (기본: scripts/_timeline_v3_<level>.parquet)")
    ap.add_argument("--out", type=str, default=None,
                    help="출력 경로 override (기본: scripts/_shape_pairs_v3_<level>.parquet)")
    args = ap.parse_args()

    iou_path = Path(args.iou_in) if args.iou_in else OUT_DIR / f"_spatial_iou_{args.level}.parquet"
    tl_path = Path(args.timeline_in) if args.timeline_in else OUT_DIR / f"_timeline_v3_{args.level}.parquet"
    if not iou_path.exists() or not tl_path.exists():
        print("ERROR: run step1/step2 first", flush=True)
        return 1

    iou = pd.read_parquet(iou_path)
    tl = pd.read_parquet(tl_path)
    print(f"loaded iou: {len(iou):,}, timeline: {len(tl):,}", flush=True)

    tl_key = tl.set_index(["version_key", "element_id"])["shape_id"].to_dict()

    rep_map: dict[int, tuple[str, str]] = {}
    for r in tl.sort_values("version_key").itertuples(index=False):
        rep_map.setdefault(int(r.shape_id), (r.version_key, r.element_id))
    print(f"unique shapes: {len(rep_map)}", flush=True)

    diff = iou[iou.iou < 0.99]
    shape_pairs_raw: set[tuple[int, int]] = set()
    for r in diff.itertuples(index=False):
        sa = tl_key.get((r.va, r.element_id_a))
        sb = tl_key.get((r.vb, r.element_id_b))
        if sa is None or sb is None or sa == sb:
            continue
        pair = tuple(sorted([int(sa), int(sb)]))
        shape_pairs_raw.add(pair)

    print(f"unique shape pairs to compute: {len(shape_pairs_raw):,}", flush=True)

    tasks = []
    needed_versions: set[str] = set()
    for sa, sb in shape_pairs_raw:
        rep_va, rep_ea = rep_map[sa]
        rep_vb, rep_eb = rep_map[sb]
        tasks.append((args.level, sa, sb, rep_va, rep_ea, rep_vb, rep_eb))
        needed_versions.add(rep_va)
        needed_versions.add(rep_vb)

    versions_list = sorted(needed_versions)
    print(f"versions to preload per worker: {len(versions_list)}", flush=True)

    t0 = time.perf_counter()
    rows: list[dict] = []
    done = 0
    with ProcessPoolExecutor(
        max_workers=args.workers,
        initializer=_init_worker,
        initargs=(args.level, versions_list),
    ) as ex:
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
    out_path = Path(args.out) if args.out else OUT_DIR / f"_shape_pairs_v3_{args.level}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    total = time.perf_counter() - t0
    print(f"\ndone in {total:.1f}s", flush=True)
    print(f"output: {out_path}  ({len(df):,} rows, {out_path.stat().st_size/1024:.1f} KB)", flush=True)
    if len(df) > 0:
        print(f"iou: min={df.iou.min():.3f} median={df.iou.median():.3f} max={df.iou.max():.3f}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
