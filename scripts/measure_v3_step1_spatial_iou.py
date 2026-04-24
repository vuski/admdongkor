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

최적화 (2026-04-24):
    - 워커 초기화 시 해당 level 의 모든 버전을 한 번씩 로드해 모듈 전역 캐시.
      기존: 쌍마다 load_level(va) + load_level(vb) → 버전당 평균 61회 재로드.
    - iterrows 루프 + 객체별 intersection/union 을 shapely 2.x 벡터 API 로 교체.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from shapely.strtree import STRtree

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import LEVEL_COLS, OUT_DIR, PARQUET_DIR, list_versions, load_level


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _compute_hashes(level: str, versions: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for v in versions:
        out[v] = _file_sha256(PARQUET_DIR / f"{level}_{v}.parquet")
    return out


# 워커 프로세스 전역 캐시: version -> (id_array, geom_array, STRtree)
# initializer 로 주입. 메인 프로세스에선 비어 있음.
_CACHE: dict[str, tuple[np.ndarray, np.ndarray, STRtree]] = {}


def _init_worker(level: str, versions: list[str]) -> None:
    """각 워커 프로세스 시작 시 호출 — 해당 level 의 모든 버전을 선로드."""
    id_col = LEVEL_COLS[level]["id"]
    for v in versions:
        gdf = load_level(v, level)[[id_col, "geometry"]].dropna(subset=[id_col])
        gdf = gdf.copy()
        gdf[id_col] = gdf[id_col].astype(str)
        gdf = gdf.drop_duplicates(id_col).reset_index(drop=True)
        geoms = gdf.geometry.values
        ids = gdf[id_col].values
        tree = STRtree(geoms)
        _CACHE[v] = (ids, geoms, tree)


def _pair_task(args: tuple[str, str, str, float]) -> list[dict]:
    """공간 크로스: (va, vb) 쌍에서 공간적으로 겹치는 요소 쌍에 대해 IoU (벡터화)."""
    va, vb, level, min_iou = args
    a_ids, a_geoms, _ = _CACHE[va]
    b_ids, b_geoms, b_tree = _CACHE[vb]

    # STRtree.query(a_geoms) — 각 a_geom 에 대해 후보 b 인덱스.
    # shapely 2.x 는 array 입력 시 (2, N) array 반환: [a_idx_row, b_idx_row].
    if len(a_geoms) == 0 or len(b_geoms) == 0:
        return []
    pairs = b_tree.query(a_geoms, predicate="intersects")
    if pairs.size == 0:
        return []
    ai = pairs[0]
    bi = pairs[1]

    ga = a_geoms[ai]
    gb = b_geoms[bi]

    # 벡터 intersection / union (GEOS C 레벨).
    inter = shapely.intersection(ga, gb)
    inter_area = shapely.area(inter)
    area_a = shapely.area(ga)
    area_b = shapely.area(gb)
    uni_area = area_a + area_b - inter_area
    with np.errstate(divide="ignore", invalid="ignore"):
        iou = np.where(uni_area > 0, inter_area / uni_area, 0.0)

    mask = iou >= min_iou
    if not mask.any():
        return []

    ai_f = ai[mask]
    bi_f = bi[mask]
    iou_f = iou[mask]

    rows = [
        {
            "va": va,
            "vb": vb,
            "element_id_a": str(a_ids[i]),
            "element_id_b": str(b_ids[j]),
            "iou": float(k),
        }
        for i, j, k in zip(ai_f, bi_f, iou_f)
    ]
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--level", choices=["emd", "sgg", "sido"], required=True)
    ap.add_argument("--workers", type=int, default=60)
    ap.add_argument("--min-iou", type=float, default=0.01,
                    help="이 값 이상의 IoU 만 기록 (슬리버 제거)")
    ap.add_argument("--out", type=str, default=None,
                    help="출력 parquet 경로 override (기본: scripts/_spatial_iou_<level>.parquet)")
    ap.add_argument("--no-cache", action="store_true",
                    help="hash 기반 증분 캐시 무시하고 전수 재계산")
    args = ap.parse_args()

    versions = list_versions()
    pairs = list(combinations(versions, 2))
    print(f"level={args.level}, versions={len(versions)}, pairs={len(pairs)}, "
          f"workers={args.workers}, min_iou={args.min_iou}", flush=True)

    out_path = Path(args.out) if args.out else OUT_DIR / f"_spatial_iou_{args.level}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # hash 맵은 출력 parquet 옆에 sidecar 로 저장해 --out 쓸 때도 독립 캐시 유지
    hash_path = out_path.with_name(f"_hashes_v3_{args.level}.parquet")

    # 현재 parquet hash 계산
    t_hash = time.perf_counter()
    new_hashes = _compute_hashes(args.level, versions)
    print(f"hashes computed in {time.perf_counter()-t_hash:.1f}s", flush=True)

    # 이전 실행 결과 + 이전 hash 맵 로드 시도
    old_hashes: dict[str, str] = {}
    old_df: pd.DataFrame | None = None
    reuse_ok = (not args.no_cache) and hash_path.exists() and out_path.exists()
    if reuse_ok:
        try:
            h_df = pd.read_parquet(hash_path)
            old_hashes = dict(zip(h_df["version_key"].astype(str),
                                  h_df["sha256"].astype(str)))
            old_df = pd.read_parquet(out_path)
        except Exception as e:
            print(f"cache load failed, falling back to full recompute: {e}", flush=True)
            old_hashes = {}
            old_df = None

    # 쌍 분류: 재사용 vs 재계산
    pairs_reuse: list[tuple[str, str]] = []
    pairs_todo: list[tuple[str, str]] = []
    for va, vb in pairs:
        if (old_df is not None
                and old_hashes.get(va) == new_hashes.get(va)
                and old_hashes.get(vb) == new_hashes.get(vb)):
            pairs_reuse.append((va, vb))
        else:
            pairs_todo.append((va, vb))
    print(f"pairs to reuse: {len(pairs_reuse):,}, to recompute: {len(pairs_todo):,}", flush=True)

    t0 = time.perf_counter()
    all_rows: list[dict] = []

    # 재사용: 기존 parquet 에서 해당 (va, vb) 행만 필터
    if pairs_reuse and old_df is not None:
        reuse_set = set(pairs_reuse)
        key_iter = zip(old_df["va"].astype(str), old_df["vb"].astype(str))
        mask = np.fromiter((k in reuse_set for k in key_iter),
                           dtype=bool, count=len(old_df))
        reuse_df = old_df[mask].copy()
        print(f"reused rows: {len(reuse_df):,}", flush=True)
    else:
        reuse_df = None

    # 재계산만 워커에 분배. 워커 init 은 어차피 모든 버전 로드 (쌍이 어느 버전을
    # 포함할지 미리 알 수 없고, 로드 자체는 1회성이라 큰 비용 아님).
    if pairs_todo:
        done = 0
        with ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_init_worker,
            initargs=(args.level, versions),
        ) as ex:
            futs = {ex.submit(_pair_task, (va, vb, args.level, args.min_iou)): (va, vb)
                    for va, vb in pairs_todo}
            for fut in as_completed(futs):
                va, vb = futs[fut]
                done += 1
                try:
                    rows = fut.result()
                    all_rows.extend(rows)
                except Exception as e:
                    print(f"  [{done}/{len(pairs_todo)}] {va}<>{vb} ERROR: {e}", flush=True)
                    continue
                if done % 50 == 0 or done == len(pairs_todo):
                    elapsed = time.perf_counter() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (len(pairs_todo) - done) / rate if rate > 0 else 0
                    print(f"  [{done}/{len(pairs_todo)}] elapsed={elapsed:.1f}s, rate={rate:.1f}/s, "
                          f"eta={eta:.0f}s, rows={len(all_rows):,}", flush=True)

    new_df = pd.DataFrame(all_rows, columns=["va", "vb", "element_id_a", "element_id_b", "iou"])
    if reuse_df is not None and len(reuse_df) > 0:
        df = pd.concat([reuse_df, new_df], ignore_index=True)
    else:
        df = new_df
    df.to_parquet(out_path, index=False)

    # 새 hash 맵 저장
    pd.DataFrame(
        {"version_key": list(new_hashes.keys()),
         "sha256": list(new_hashes.values())}
    ).to_parquet(hash_path, index=False)

    total = time.perf_counter() - t0
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\ndone in {total:.1f}s", flush=True)
    print(f"output: {out_path}  ({len(df):,} rows, {size_mb:.1f} MB)", flush=True)
    print(f"hashes: {hash_path}", flush=True)

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
