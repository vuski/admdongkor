"""emd parquet → sgg / sido parquet 디졸브.

기존 admdongkor-timeseries/scripts/rebuild_sgg_sido.py 기반, self-contained.

스키마:
  sgg:  sggcd, sggnm, sidocd, sidonm, area, geom
  sido: sidocd, sidonm, area, geom

디졸브 후 HOLE_AREA_THRESHOLD (100,000 m²) 미만 hole 제거.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon

HOLE_AREA_THRESHOLD = 100_000.0  # m²
SGG_COLS = ["sggcd", "sggnm", "sidocd", "sidonm", "area", "geom"]
SIDO_COLS = ["sidocd", "sidonm", "area", "geom"]


def _drop_small_holes(geom):
    if geom is None or geom.is_empty:
        return geom

    def _clean(p: Polygon) -> Polygon:
        kept = [r for r in p.interiors if Polygon(r).area >= HOLE_AREA_THRESHOLD]
        return Polygon(p.exterior, holes=kept)

    if geom.geom_type == "Polygon":
        return _clean(geom)
    if geom.geom_type == "MultiPolygon":
        return MultiPolygon([_clean(p) for p in geom.geoms])
    return geom


def build_sgg(emd: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    em = emd.copy()
    em["sggcd"] = em["sggcd"].astype("string")
    if em["sggcd"].notna().any():
        have = em.dropna(subset=["sggcd"]).copy()
        diss = have.dissolve(by="sggcd",
                             aggfunc={"sggnm": "first",
                                      "sidocd": "first",
                                      "sidonm": "first"}).reset_index()
    else:
        have = em.dropna(subset=["sggnm"]).copy()
        have = have[have["sggnm"].astype(str).str.len() > 0]
        if have.empty:
            return gpd.GeoDataFrame(columns=SGG_COLS, geometry="geom", crs=em.crs)
        diss = have.dissolve(by=["sidonm", "sggnm"],
                             aggfunc={"sidocd": "first"}).reset_index()
        diss["sggcd"] = pd.NA
    diss[diss.geometry.name] = diss.geometry.apply(_drop_small_holes)
    diss["area"] = diss.geometry.area
    if diss.geometry.name != "geom":
        diss = diss.rename_geometry("geom")
    return gpd.GeoDataFrame(diss[SGG_COLS], geometry="geom", crs=em.crs)


def build_sido(emd: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    em = emd.copy()
    em["sidocd"] = em["sidocd"].astype("string")
    if em["sidocd"].notna().any():
        have = em.dropna(subset=["sidocd"]).copy()
        diss = have.dissolve(by="sidocd",
                             aggfunc={"sidonm": "first"}).reset_index()
    else:
        have = em.dropna(subset=["sidonm"]).copy()
        have = have[have["sidonm"].astype(str).str.len() > 0]
        if have.empty:
            return gpd.GeoDataFrame(columns=SIDO_COLS, geometry="geom", crs=em.crs)
        diss = have.dissolve(by="sidonm").reset_index()
        diss["sidocd"] = pd.NA
    diss[diss.geometry.name] = diss.geometry.apply(_drop_small_holes)
    diss["area"] = diss.geometry.area
    if diss.geometry.name != "geom":
        diss = diss.rename_geometry("geom")
    return gpd.GeoDataFrame(diss[SIDO_COLS], geometry="geom", crs=em.crs)


def dissolve_from_emd_parquet(emd_path: str | Path,
                              sgg_out: str | Path,
                              sido_out: str | Path) -> tuple[int, int]:
    """emd parquet 를 읽어 sgg, sido parquet 생성. 행수 반환."""
    emd_path = Path(emd_path)
    sgg_out = Path(sgg_out)
    sido_out = Path(sido_out)
    emd = gpd.read_parquet(emd_path)
    sgg = build_sgg(emd)
    sido = build_sido(emd)
    sgg.to_parquet(sgg_out, index=False)
    sido.to_parquet(sido_out, index=False)
    return len(sgg), len(sido)
