"""실측 스크립트 공용 유틸."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd

REPO_ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = REPO_ROOT / "parquet"
OUT_DIR = Path(__file__).resolve().parent


LEVEL_COLS = {
    "emd": {
        "id": "emdcd",
        "name": "emdnm",
        "attrs": ["emd7", "emd8", "sggcd", "sggnm", "sidocd", "sidonm", "area"],
    },
    "sgg": {
        "id": "sggcd",
        "name": "sggnm",
        "attrs": ["sidocd", "sidonm", "area"],
    },
    "sido": {
        "id": "sidocd",
        "name": "sidonm",
        "attrs": ["area"],
    },
}


def list_versions() -> list[str]:
    files = sorted(PARQUET_DIR.glob("emd_*.parquet"))
    return [f.stem.split("_", 1)[1] for f in files]


def load_level(version: str, level: str) -> gpd.GeoDataFrame:
    """geometry 컬럼을 표준 'geometry' 로 맞춰서 리턴."""
    path = PARQUET_DIR / f"{level}_{version}.parquet"
    gdf = gpd.read_parquet(path)
    if "geom" in gdf.columns and "geometry" not in gdf.columns:
        gdf = gdf.rename(columns={"geom": "geometry"}).set_geometry("geometry")
    return gdf
