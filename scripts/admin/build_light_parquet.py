"""parquet/emd_<v>.parquet → parquet/simplified/{emd,sgg,sido}_<v>_light.parquet 배치.

파이프라인 (1 버전당):
    1) emd parquet (EPSG:5179) 읽기 → EPSG:4326 재투영 → 임시 GeoJSON 저장
    2) mapshaper -simplify 18.7% keep-shapes  → simplified GeoJSON
    3) 단순화에서 사라진 미세 섬(독도) 을 원본 part 그대로 복원
    4) GeoPandas 로 읽어 snappy parquet 저장 (emd_<v>_light.parquet)
    5) dissolve(sggcd)  → 작은 홀 제거(<1km²) → sgg_<v>_light.parquet
    6) dissolve(sidocd) → 작은 홀 제거(<1km²) → sido_<v>_light.parquet
    7) 임시 GeoJSON 정리

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
import shapely
from shapely.geometry import MultiPolygon, Polygon

REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = REPO_ROOT / "parquet"
OUT_DIR = PARQUET_DIR / "simplified"

SIMPLIFY_PERCENT = "18.7%"
HOLE_THRESHOLD_KM2 = 1.0
# 1 km² → 제곱도 (중위도 약 37°N 기준 대략치)
HOLE_THRESHOLD_DEG2 = 1e6 / (111_000**2 * 0.8)

# ── 독도 보존 ──────────────────────────────────────────────────────────────
# mapshaper 의 `-simplify keep-shapes` 는 **feature(행)** 을 보존할 뿐,
# MultiPolygon 안의 작은 **part** 는 지운다. 독도는 동도 0.07km²/서도 0.09km²
# 라서 18.7% 단순화에서 두 폴리곤이 통째로 사라졌다 (경상북도 울릉군 울릉읍).
# → 단순화 결과에서 이 bbox 안의 part 가 없어졌으면 **원본 part 를 그대로**
#   (단순화하지 않고) 다시 붙인다. 총 37개 점이라 용량 영향은 무시할 수준.
KEEP_PARTS_BBOX = {
    "독도": (131.80, 37.20, 131.95, 37.28),
}

# preprocessing/scripts/rebuild_sgg_sido.py 와 동일한 정책.
# 같은 sggcd 안에 sggnm 이 둘 이상 공존하는 의도된 케이스 화이트리스트.
# 등재된 (key, sggcd) 는 dissolve 키가 (sggcd, sggnm) 으로 바뀌어
# 같은 sggcd 안에서도 이름별 별도 행으로 분리된다.
# 2000-2002 충북: 43760(괴산군) 안에 "괴산군" + "증평출장소" 공존.
SGG_SPLIT_WHITELIST: set[tuple[str, str]] = {
    ("20001231", "43760"),
    ("20011231", "43760"),
    ("20021231", "43760"),
}


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


def _restore_tiny_parts(simp: gpd.GeoDataFrame, orig: gpd.GeoDataFrame) -> int:
    """단순화 중 사라진 KEEP_PARTS_BBOX 안의 part 를 원본 그대로 되살린다.

    simp/orig 는 둘 다 EPSG:4326, 같은 행 순서(mapshaper 가 순서를 보존)여야 한다.
    반환: 복원한 part 개수.
    """
    if len(simp) != len(orig):
        raise RuntimeError(
            f"row count mismatch after simplify: {len(simp)} != {len(orig)}"
        )
    restored = 0
    sgeom = simp.geometry
    ogeom = orig.geometry
    for name, bounds in KEEP_PARTS_BBOX.items():
        box = shapely.box(*bounds)
        for i in range(len(orig)):
            og = ogeom.iloc[i]
            if og is None or og.is_empty or not og.intersects(box):
                continue
            parts = list(og.geoms) if og.geom_type == "MultiPolygon" else [og]
            missing = [p for p in parts if p.intersects(box)]
            if not missing:
                continue
            sg = sgeom.iloc[i]
            kept = []
            if sg is not None and not sg.is_empty:
                kept = list(sg.geoms) if sg.geom_type == "MultiPolygon" else [sg]
            # 이미 살아남은 part 는 그대로 두고, 없어진 것만 원본에서 가져온다.
            already = sum(1 for p in kept if p.intersects(box))
            if already >= len(missing):
                continue
            kept = [p for p in kept if not p.intersects(box)]
            simp.loc[simp.index[i], simp.geometry.name] = MultiPolygon(
                kept + missing
            )
            restored += len(missing)
    return restored


def build_sgg_sido(emd_s: gpd.GeoDataFrame, version: str) -> tuple[int, int]:
    """emd light 에서 sgg/sido light 를 dissolve 로 만들고 저장.

    build_light_parquet 의 정상 경로와 patch_dokdo_light 의 보수 경로가
    **같은 dissolve 정책**(화이트리스트 포함) 을 쓰도록 공용 함수로 분리했다.
    반환: (sgg_rows, sido_rows).
    """
    # sgg dissolve.
    # 1975/1980/1985 버전은 sggcd/sidocd 가 전부 NULL 이라 이름으로 dissolve.
    # 이 경우 "sidonm + sggnm" 조합을 키로 써서 동명 시군구(예: '중구') 가
    # 서로 다른 시도에서 합쳐지지 않게 한다.
    has_sgg_code = emd_s["sggcd"].notna().any()
    if has_sgg_code:
        # 화이트리스트 sggcd 는 (sggcd, sggnm) 조합으로 분리, 나머지는 sggcd 단독.
        split_codes = {sggcd for k, sggcd in SGG_SPLIT_WHITELIST if k == version}
        if split_codes:
            split_mask = emd_s["sggcd"].isin(split_codes)
            normal = emd_s[~split_mask]
            split_part = emd_s[split_mask]
            parts = []
            if not normal.empty:
                d1 = normal.dissolve(
                    by="sggcd",
                    aggfunc={"sggnm": "first", "sidocd": "first",
                             "sidonm": "first", "area": "sum"},
                ).reset_index()
                parts.append(d1)
            if not split_part.empty:
                d2 = split_part.dissolve(
                    by=["sggcd", "sggnm"],
                    aggfunc={"sidocd": "first",
                             "sidonm": "first", "area": "sum"},
                ).reset_index()
                parts.append(d2)
            sgg = pd.concat(parts, ignore_index=True)
        else:
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

    # sido dissolve
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
    return len(sgg), len(sido)


def process_version(version: str) -> tuple[str, int, int, int, int, float]:
    """1개 버전 처리.

    반환: (version, emd_rows, sgg_rows, sido_rows, restored_parts, elapsed_s).
    """
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

        # 3) 단순화에서 사라진 미세 섬(독도) 을 원본 그대로 복원
        emd_s = gpd.read_file(simp_geojson)
        n_restored = _restore_tiny_parts(emd_s, emd)

        # 4) emd light parquet
        emd_out = OUT_DIR / f"emd_{version}_light.parquet"
        emd_s.to_parquet(emd_out, compression="snappy")

    n_sgg, n_sido = build_sgg_sido(emd_s, version)

    return version, len(emd_s), n_sgg, n_sido, n_restored, time.perf_counter() - t0


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
                ver, n_emd, n_sgg, n_sido, n_rest, sec = fut.result()
                rest = f"  restored={n_rest}" if n_rest else ""
                print(f"[{i:3d}/{len(versions)}] {ver}  "
                      f"emd={n_emd}  sgg={n_sgg}  sido={n_sido}{rest}  ({sec:.1f}s)",
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
