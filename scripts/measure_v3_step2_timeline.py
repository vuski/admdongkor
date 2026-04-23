"""v3 step2 — global shape_id 부여 + timeline 구성.

v2 와 차이:
    - v2: element_id 별로 local shape_id
    - v3: 공간 크로스 edge (IoU>=0.99) 기반 global union-find → 전역 shape_id

Input:
    scripts/_spatial_iou_<level>.parquet  (v3 step1 산출)

Output:
    scripts/_timeline_v3_<level>.parquet
    columns: level, version_key, element_id, shape_id (global), name, attrs...

실행:
    python scripts/measure_v3_step2_timeline.py --level sido --iou 0.99
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import LEVEL_COLS, OUT_DIR, list_versions, load_level


class UF:
    def __init__(self):
        self.p: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[ra] = rb


def collect_attrs(level: str, versions: list[str]) -> pd.DataFrame:
    id_col = LEVEL_COLS[level]["id"]
    name_col = LEVEL_COLS[level]["name"]
    attr_cols = LEVEL_COLS[level]["attrs"]
    rows: list[pd.DataFrame] = []
    for v in versions:
        cols = [id_col, name_col] + attr_cols
        gdf = load_level(v, level)
        df = gdf[cols].copy()
        df[id_col] = df[id_col].astype("string")
        df = df.dropna(subset=[id_col])
        df["version_key"] = v
        rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    out = out.rename(columns={id_col: "element_id", name_col: "name"})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--level", choices=["emd", "sgg", "sido"], required=True)
    ap.add_argument("--iou", type=float, default=0.99)
    args = ap.parse_args()

    iou_path = OUT_DIR / f"_spatial_iou_{args.level}.parquet"
    if not iou_path.exists():
        print(f"ERROR: {iou_path} not found.", flush=True)
        return 1
    iou = pd.read_parquet(iou_path)
    print(f"loaded {len(iou):,} rows from {iou_path.name}", flush=True)

    # global UF: node = "{version_key}|{element_id}"
    same_shape = iou[iou.iou >= args.iou]
    print(f"same-shape edges (IoU>={args.iou}): {len(same_shape):,}", flush=True)

    uf = UF()
    for r in same_shape.itertuples(index=False):
        ka = f"{r.va}|{r.element_id_a}"
        kb = f"{r.vb}|{r.element_id_b}"
        uf.union(ka, kb)

    versions = list_versions()
    attrs = collect_attrs(args.level, versions)
    print(f"collected attrs: {len(attrs):,} rows", flush=True)

    # root -> shape_id (등장 순)
    root_to_sid: dict[str, int] = {}
    sids: list[int] = []
    for r in attrs.itertuples(index=False):
        key = f"{r.version_key}|{r.element_id}"
        root = uf.find(key)  # 없으면 자기자신
        sid = root_to_sid.setdefault(root, len(root_to_sid))
        sids.append(sid)
    attrs["shape_id"] = sids

    # 출력 컬럼 순서
    level_attrs = LEVEL_COLS[args.level]["attrs"]
    ordered = ["version_key", "element_id", "shape_id", "name"] + level_attrs
    out = attrs[ordered].copy()
    out.insert(0, "level", args.level)
    out = out.sort_values(["element_id", "version_key"]).reset_index(drop=True)

    out_path = OUT_DIR / f"_timeline_v3_{args.level}.parquet"
    out.to_parquet(out_path, index=False)

    # 통계
    n_shapes = out.shape_id.nunique()
    n_elements = out.element_id.nunique()
    # 한 element 당 shape 수
    shapes_per_element = out.groupby("element_id").shape_id.nunique()
    # 한 shape 당 element 수 (재할당 검출)
    elements_per_shape = out.groupby("shape_id").element_id.nunique()
    reused_shapes = (elements_per_shape > 1).sum()

    print(f"\noutput: {out_path}  ({len(out):,} rows, {out_path.stat().st_size/1024:.1f} KB)", flush=True)
    print(f"elements: {n_elements}", flush=True)
    print(f"global unique shapes: {n_shapes}", flush=True)
    print(f"  shapes reused across different element_ids: {reused_shapes}", flush=True)
    print(f"  (= emdcd/sggcd/sidocd 재할당 횟수)", flush=True)
    print(f"shapes/element: mean {shapes_per_element.mean():.2f}, max {shapes_per_element.max()}", flush=True)
    print(f"elements/shape: mean {elements_per_shape.mean():.2f}, max {elements_per_shape.max()}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
