"""parquet/emd_<v>.parquet → parquet/simplified/{emd,sgg,sido}_<v>_light.parquet 배치.

파이프라인 (1 버전당):
    1) emd parquet (EPSG:5179) 읽기 → EPSG:4326 재투영 → 임시 GeoJSON 저장
    2) mapshaper -simplify 18.7% keep-shapes  → simplified GeoJSON
    3) GeoPandas 로 읽어 snappy parquet 저장 (emd_<v>_light.parquet)
    4) dissolve(sggcd)  → 작은 홀 제거(<1km²) → sgg_<v>_light.parquet
    5) dissolve(sidocd) → 작은 홀 제거(<1km²) → sido_<v>_light.parquet
    6) 임시 GeoJSON 정리

병렬: ProcessPoolExecutor. mapshaper 는 npx 로 호출.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon

REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = REPO_ROOT / "parquet"
OUT_DIR = PARQUET_DIR / "simplified"

SIMPLIFY_PERCENT = "18.7%"
HOLE_THRESHOLD_KM2 = 1.0
# 1 km² → 제곱도 (중위도 약 37°N 기준 대략치)
HOLE_THRESHOLD_DEG2 = 1e6 / (111_000**2 * 0.8)


def _remove_small_holes(geom, thresh: float):
    if geom is None or geom.is_empty:
        return geom
    if isinstance(geom, Polygon):
        interiors = [r for r in geom.interiors if Polygon(r).area >= thresh]
        return Polygon(geom.exterior, interiors)
    if isinstance(geom, MultiPolygon):
        return MultiPolygon([
            Polygon(p.exterior, [r for r in p.interiors if Polygon(r).area >= thresh])
            for p in geom.geoms
        ])
    return geom


def process_version(version: str) -> tuple[str, int, int, int, float]:
    """1개 버전 처리. 반환: (version, emd_rows, sgg_rows, sido_rows, elapsed_s)."""
    t0 = time.perf_counter()
    src = PARQUET_DIR / f"emd_{version}.parquet"
    if not src.exists():
        raise FileNotFoundError(src)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"light_{version}_") as td:
        tmp = Path(td)
        raw_geojson = tmp / f"emd_{version}.geojson"
        simp_geojson = tmp / f"emd_{version}_simp.geojson"

        # 1) parquet → WGS84 GeoJSON
        emd = gpd.read_parquet(src).to_crs(4326)
        emd.to_file(raw_geojson, driver="GeoJSON")

        # 2) mapshaper simplify (combine-layers 로 multi-output 방지)
        cmd = ["npx", "--yes", "mapshaper", str(raw_geojson),
               "-simplify", SIMPLIFY_PERCENT, "keep-shapes",
               "-o", "combine-layers", str(simp_geojson)]
        r = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        if r.returncode != 0:
            raise RuntimeError(f"mapshaper failed for {version}: {r.stderr}")
        if not simp_geojson.exists():
            # mapshaper 가 멀티 레이어로 분할 저장한 경우 (simp1.geojson, simp2.geojson ...)
            stem = simp_geojson.stem
            parts = sorted(simp_geojson.parent.glob(f"{stem}*.geojson"))
            if not parts:
                raise RuntimeError(f"mapshaper output missing for {version}: {r.stderr}")
            # 첫 파트를 메인으로 채택 — keep-shapes 로 feature 수 보존
            parts[0].rename(simp_geojson)

        # 3) emd light parquet
        emd_s = gpd.read_file(simp_geojson)
        emd_out = OUT_DIR / f"emd_{version}_light.parquet"
        emd_s.to_parquet(emd_out, compression="snappy")

    # 4) sgg dissolve.
    # 1975/1980/1985 버전은 sggcd/sidocd 가 전부 NULL 이라 이름으로 dissolve.
    # 이 경우 "sidonm + sggnm" 조합을 키로 써서 동명 시군구(예: '중구') 가
    # 서로 다른 시도에서 합쳐지지 않게 한다.
    has_sgg_code = emd_s["sggcd"].notna().any()
    if has_sgg_code:
        sgg = emd_s.dissolve(
            by="sggcd",
            aggfunc={"sggnm": "first", "sidocd": "first",
                     "sidonm": "first", "area": "sum"},
        ).reset_index()
    else:
        # 코드가 없는 옛 버전: 이름으로 dissolve. sggcd/sidocd 는 NaN 으로 둠.
        sgg = emd_s.dissolve(
            by=["sidonm", "sggnm"],
            aggfunc={"area": "sum"},
        ).reset_index()
        sgg["sggcd"] = pd.NA
        sgg["sidocd"] = pd.NA
    sgg = sgg[["sggcd", "sggnm", "sidocd", "sidonm", "area", "geometry"]]
    sgg["geometry"] = sgg.geometry.apply(lambda g: _remove_small_holes(g, HOLE_THRESHOLD_DEG2))
    sgg_out = OUT_DIR / f"sgg_{version}_light.parquet"
    sgg.to_parquet(sgg_out, compression="snappy")

    # 5) sido dissolve
    has_sido_code = emd_s["sidocd"].notna().any()
    if has_sido_code:
        sido = emd_s.dissolve(
            by="sidocd",
            aggfunc={"sidonm": "first", "area": "sum"},
        ).reset_index()
    else:
        sido = emd_s.dissolve(
            by="sidonm",
            aggfunc={"area": "sum"},
        ).reset_index()
        sido["sidocd"] = pd.NA
    sido = sido[["sidocd", "sidonm", "area", "geometry"]]
    sido["geometry"] = sido.geometry.apply(lambda g: _remove_small_holes(g, HOLE_THRESHOLD_DEG2))
    sido_out = OUT_DIR / f"sido_{version}_light.parquet"
    sido.to_parquet(sido_out, compression="snappy")

    return version, len(emd_s), len(sgg), len(sido), time.perf_counter() - t0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", nargs="*", default=None,
                    help="특정 버전만. 생략하면 parquet/emd_*.parquet 전체")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--force", action="store_true",
                    help="이미 <out>/emd_<v>_light.parquet 있어도 재생성")
    args = ap.parse_args()

    if args.version:
        versions = args.version
    else:
        versions = sorted(p.stem.split("_", 1)[1]
                          for p in PARQUET_DIR.glob("emd_*.parquet"))

    if not args.force:
        versions = [v for v in versions
                    if not (OUT_DIR / f"emd_{v}_light.parquet").exists()
                    or not (OUT_DIR / f"sgg_{v}_light.parquet").exists()
                    or not (OUT_DIR / f"sido_{v}_light.parquet").exists()]

    if not versions:
        print("nothing to do (use --force to rebuild)")
        return 0

    print(f"processing {len(versions)} versions with {args.workers} workers...", flush=True)
    t0 = time.perf_counter()

    failed = []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_version, v): v for v in versions}
        for i, fut in enumerate(as_completed(futs), 1):
            v = futs[fut]
            try:
                ver, n_emd, n_sgg, n_sido, sec = fut.result()
                print(f"[{i:3d}/{len(versions)}] {ver}  "
                      f"emd={n_emd}  sgg={n_sgg}  sido={n_sido}  ({sec:.1f}s)",
                      flush=True)
            except Exception as e:
                print(f"[{i:3d}/{len(versions)}] {v}  FAILED: {e}", flush=True)
                failed.append((v, str(e)))

    elapsed = time.perf_counter() - t0
    print(f"\nDONE {len(versions) - len(failed)}/{len(versions)} in {elapsed:.1f}s")
    if failed:
        print("failed versions:")
        for v, msg in failed:
            print(f"  {v}: {msg}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
