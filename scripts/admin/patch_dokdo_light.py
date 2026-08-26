"""parquet/simplified/*_light.parquet 에 독도를 넣는 보수 스크립트.

배경
----
1) **2012-12-31 이후 45개 시점**: 원본에는 독도 2개 폴리곤이 있는데 light 에는
   없다. mapshaper `-simplify keep-shapes` 는 **feature(행)** 만 보존하고
   MultiPolygon 안의 작은 **part** 는 지우기 때문 (동도 0.07km²/서도 0.09km²).
2) **2012-12-10 이전 18개 시점**: 원본 geojson/shp 에도 독도가 아예 없다.
   단순화 문제가 아니라 원 자료의 누락.

두 경우 모두 **독도 폴리곤을 원본 그대로(단순화 없이)** 해당 읍면동 행에
붙인다. 소속은 시점에 따라 다르다:

    1975-12-31          경상북도 울릉군 **남면**   (당시 울릉읍이 없음)
    1980-12-31 이후     경상북도 울릉군 **울릉읍**

기증 지오메트리는 원본이 독도를 가진 시점 중 **가장 이른 20121231** 것을 쓴다
(시점별 좌표 차이는 수 m 수준이라 옛 시점에 붙여도 무해하다).

주의: light 파일의 행 순서는 시점마다 다르므로 **인덱스가 아니라 이름**으로
대상 행을 찾는다.

사용:
    python scripts/admin/patch_dokdo_light.py --dry-run
    python scripts/admin/patch_dokdo_light.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import geopandas as gpd
import shapely
from shapely.geometry import MultiPolygon

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_light_parquet import (  # noqa: E402
    KEEP_PARTS_BBOX,
    OUT_DIR,
    PARQUET_DIR,
    build_sgg_sido,
)

DOKDO_BOX = shapely.box(*KEEP_PARTS_BBOX["독도"])

# 독도 지오메트리를 빌려올 시점 — 원본에 독도가 있는 가장 이른 버전.
DONOR_VERSION = "20121231"

# 시점별 독도 소속 읍면동. 키는 version 문자열, 값은 emdnm.
# 1975 는 울릉읍이 없고 남면이 그 자리다.
DOKDO_EMD_DEFAULT = "울릉읍"
DOKDO_EMD_OVERRIDE = {"19751231": "남면"}

ULLUNG_SGG = "울릉군"


def _dokdo_parts(gdf: gpd.GeoDataFrame) -> list:
    """gdf(EPSG:4326) 안의 독도 폴리곤 part 목록."""
    gc = gdf.geometry.name
    out = []
    for g in gdf[gc]:
        if g is None or g.is_empty or not g.intersects(DOKDO_BOX):
            continue
        parts = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
        out.extend(p for p in parts if p.intersects(DOKDO_BOX))
    return out


def load_donor() -> list:
    src = PARQUET_DIR / f"emd_{DONOR_VERSION}.parquet"
    parts = _dokdo_parts(gpd.read_parquet(src).to_crs(4326))
    if len(parts) != 2:
        raise RuntimeError(f"donor {DONOR_VERSION}: expected 2 parts, got {len(parts)}")
    return parts


def patch_version(version: str, donor: list, dry_run: bool) -> tuple[int, str]:
    """1개 버전 보수. 반환: (added_parts, note)."""
    light = OUT_DIR / f"emd_{version}_light.parquet"
    if not light.exists():
        return 0, "skip (no light file)"

    emd = gpd.read_parquet(light)
    if emd.crs is None:
        emd = emd.set_crs(4326)
    gc = emd.geometry.name

    if _dokdo_parts(emd):
        return 0, "already has 독도"

    # 소속 행 찾기 — 이름 기준 (행 순서는 시점마다 다르다).
    want = DOKDO_EMD_OVERRIDE.get(version, DOKDO_EMD_DEFAULT)
    mask = (
        emd["sggnm"].astype(str).str.contains(ULLUNG_SGG, na=False)
        & (emd["emdnm"].astype(str) == want)
    )
    n = int(mask.sum())
    if n != 1:
        raise RuntimeError(f"{version}: {ULLUNG_SGG} {want} 행이 {n}개 (1개여야 함)")
    i = emd.index[mask][0]

    # 원본이 독도를 가진 시점이면 그 시점 것을, 아니면 donor 를 쓴다.
    src = PARQUET_DIR / f"emd_{version}.parquet"
    own = _dokdo_parts(gpd.read_parquet(src).to_crs(4326)) if src.exists() else []
    parts = own or donor
    origin = "self" if own else f"donor {DONOR_VERSION}"

    g = emd.at[i, gc]
    kept = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
    if dry_run:
        return len(parts), f"would add to {want} ({origin})"

    emd.at[i, gc] = MultiPolygon(kept + parts)
    emd.to_parquet(light, compression="snappy")
    build_sgg_sido(emd, version)
    return len(parts), f"added to {want} ({origin})"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", nargs="*", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # stem 은 "emd_<version>_light" — 앞뒤를 모두 떼야 version 만 남는다.
    versions = args.version or sorted(
        p.stem.removeprefix("emd_").removesuffix("_light")
        for p in OUT_DIR.glob("emd_*_light.parquet")
    )
    donor = load_donor()
    print(f"donor={DONOR_VERSION} ({len(donor)} parts), checking {len(versions)} versions...",
          flush=True)
    t0 = time.perf_counter()

    total = 0
    failed = []
    for v in versions:
        try:
            n, note = patch_version(v, donor, args.dry_run)
            total += n
            if n:
                print(f"  {v}  +{n}  {note}", flush=True)
        except Exception as e:
            print(f"  {v}  FAILED: {e}", flush=True)
            failed.append((v, str(e)))

    print(f"\nDONE  added={total} parts  ({time.perf_counter() - t0:.1f}s)")
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
