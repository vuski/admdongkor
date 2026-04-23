"""HangJeongDong_ver<YYYYMMDD>.geojson → 통일 emd parquet 스키마 변환.

이 파일은 Z:/Github/admdongkor-timeseries/src/admdongkor_ts/geojson_loader.py 에서
옮겨온 것으로, 관리자용 파이프라인에서 self-contained 로 동작한다 (외부 의존성 제거).

44 버전 (2012-12 ~ 2026-02) 헤테로지니 필드명 자동 감지:
  - adm_cd: 통계청 7자리 (2023-10 이전) 또는 8자리 (2023-10 이후)
  - adm_cd2: 행안부 10자리 BJD (2018-07 이후 존재)
  - 2016-12 변종: adm_cd 가 10자리 BJD 이고 통계청 코드 없음
  - sgg/sido meta 가 빠지면 adm_nm 파싱으로 fallback

출력: 통일 emd 스키마 GeoDataFrame (아래 EMD_COLS 순서)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd


KMA_CRS = "EPSG:5179"  # Korea 2000 / Unified CS

EMD_COLS = ["code", "code_len", "bjd_code", "name",
            "sgg_code", "sgg_name", "sido_code", "sido_name",
            "bjd_sgg_code", "area", "geom"]


@dataclass(frozen=True)
class VersionSchema:
    kostat_field: str | None
    kostat_len: int | None
    bjd_field: str | None
    sgg_code_field: str | None
    sgg_name_field: str | None
    sido_name_field: str | None


def _parse_name_parts(adm_nm: str) -> tuple[str, str, str]:
    """'서울특별시 종로구 사직동' -> (sido, sgg, emd)."""
    parts = adm_nm.strip().split()
    if len(parts) == 2:
        return parts[0], "", parts[1]
    if len(parts) >= 3:
        return parts[0], " ".join(parts[1:-1]), parts[-1]
    return "", "", adm_nm.strip()


def _detect_schema(fields: set[str], sample_props: dict) -> VersionSchema:
    def _len_of(field: str) -> int | None:
        v = sample_props.get(field)
        return len(str(v)) if v is not None else None

    if "adm_cd" in fields and _len_of("adm_cd") == 10:
        return VersionSchema(
            kostat_field=None, kostat_len=None,
            bjd_field="adm_cd",
            sgg_code_field="gu_cd" if "gu_cd" in fields else None,
            sgg_name_field="gu_nm" if "gu_nm" in fields else None,
            sido_name_field="sido_nm" if "sido_nm" in fields else None,
        )

    kostat_field = "adm_cd" if "adm_cd" in fields else None
    kostat_len = _len_of(kostat_field) if kostat_field else None
    bjd_field = "adm_cd2" if "adm_cd2" in fields else None

    sgg_code_field = None
    for cand in ("sgg", "gu_cd"):
        if cand in fields:
            sgg_code_field = cand
            break

    sgg_name_field = "sggnm" if "sggnm" in fields else ("gu_nm" if "gu_nm" in fields else None)
    sido_name_field = "sidonm" if "sidonm" in fields else ("sido_nm" if "sido_nm" in fields else None)

    return VersionSchema(kostat_field, kostat_len, bjd_field,
                         sgg_code_field, sgg_name_field, sido_name_field)


def load_hangjeongdong_geojson(path: str | Path) -> gpd.GeoDataFrame:
    """한 버전의 GeoJSON 을 읽어 통일 emd 스키마로 재구성.

    - EPSG:5179 로 재투영
    - CRS 거짓말 탐지 (선언 WGS84 실제 5179)
    - make_valid() 로 Ring Self-intersection 수복
    - geometry 컬럼명 = 'geom'
    """
    from shapely import make_valid

    path = Path(path)
    gdf = gpd.read_file(path)

    minx, miny, maxx, maxy = gdf.total_bounds
    looks_projected = maxx > 1000 or maxy > 1000
    if looks_projected:
        gdf = gdf.set_crs(KMA_CRS, allow_override=True)
    elif gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    gdf = gdf.to_crs(KMA_CRS)

    invalid_mask = ~gdf.geometry.is_valid
    if invalid_mask.any():
        gdf.loc[invalid_mask, gdf.geometry.name] = gdf.loc[invalid_mask].geometry.apply(make_valid)

    fields = set(gdf.columns) - {"geometry"}
    sample = gdf.iloc[0].to_dict() if len(gdf) else {}
    schema = _detect_schema(fields, sample)

    if schema.kostat_field:
        code = gdf[schema.kostat_field].astype("string").str.strip()
    else:
        code = pd.Series([pd.NA] * len(gdf), dtype="string")
    bjd = (gdf[schema.bjd_field].astype("string").str.strip()
           if schema.bjd_field else pd.Series([pd.NA] * len(gdf), dtype="string"))

    adm_nm = gdf["adm_nm"].astype(str) if "adm_nm" in fields else pd.Series([""] * len(gdf))
    parsed = adm_nm.map(_parse_name_parts)
    sido_nm_parsed = parsed.map(lambda t: t[0])
    sgg_nm_parsed = parsed.map(lambda t: t[1])
    emd_nm_parsed = parsed.map(lambda t: t[2])

    sido_name = (gdf[schema.sido_name_field].astype("string")
                 if schema.sido_name_field else sido_nm_parsed.astype("string"))
    sgg_name = (gdf[schema.sgg_name_field].astype("string")
                if schema.sgg_name_field else sgg_nm_parsed.astype("string"))

    sgg_name = sgg_name.where(
        ~sgg_name.fillna("").str.contains(" "),
        sgg_name.fillna("").str.split(" ").str[-1],
    )

    bjd_sgg_code = (gdf[schema.sgg_code_field].astype("string").str.strip()
                    if schema.sgg_code_field else
                    pd.Series([pd.NA] * len(gdf), dtype="string"))
    if code.notna().any():
        sgg_code = code.str.slice(0, 5)
    else:
        sgg_code = pd.Series([pd.NA] * len(gdf), dtype="string")

    if code.notna().any():
        sido_code = code.str.slice(0, 2)
    elif bjd.notna().any():
        sido_code = bjd.str.slice(0, 2)
    else:
        sido_code = pd.Series([pd.NA] * len(gdf), dtype="string")

    out = pd.DataFrame({
        "code": code,
        "code_len": pd.Series([schema.kostat_len] * len(gdf), dtype="Int8"),
        "bjd_code": bjd,
        "name": emd_nm_parsed.astype("string"),
        "sgg_code": sgg_code,
        "sgg_name": sgg_name,
        "sido_code": sido_code,
        "sido_name": sido_name,
        "bjd_sgg_code": bjd_sgg_code,
        "area": gdf.geometry.area.values,
        "geom": gdf.geometry.values,
    })

    return gpd.GeoDataFrame(out[EMD_COLS], geometry="geom", crs=KMA_CRS)


def to_admdongkor_schema(emd: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """admdongkor 라이브러리의 공개 스키마로 컬럼명 변환.

    공개 스키마:
      emd7, emd8, emdcd, emdnm, sggcd, sggnm, sidocd, sidonm, area, geom
    """
    out = pd.DataFrame({
        "emd7": emd["code"].where(emd["code_len"] == 7, other=pd.NA).astype("string"),
        "emd8": emd["code"].where(emd["code_len"] == 8, other=pd.NA).astype("string"),
        "emdcd": emd["bjd_code"].astype("string"),
        "emdnm": emd["name"].astype("string"),
        "sggcd": emd["bjd_code"].astype("string").str.slice(0, 5),
        "sggnm": emd["sgg_name"].astype("string"),
        "sidocd": emd["bjd_code"].astype("string").str.slice(0, 2),
        "sidonm": emd["sido_name"].astype("string"),
        "area": emd["area"].values,
        "geom": emd.geometry.values,
    })
    unified_cols = ["emd7", "emd8", "emdcd", "emdnm",
                    "sggcd", "sggnm", "sidocd", "sidonm", "area", "geom"]
    return gpd.GeoDataFrame(out[unified_cols], geometry="geom", crs=KMA_CRS)
