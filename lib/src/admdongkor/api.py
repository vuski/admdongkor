"""공개 API: get_list, find, get."""

from __future__ import annotations

from typing import Literal

import geopandas as gpd
import pandas as pd

from . import _index
from ._cache import download_if_needed
from ._versions import VERSIONS

Level = Literal["emd", "sgg", "sido"]
_LEVELS: tuple[Level, ...] = ("emd", "sgg", "sido")


def get_list(year: int | None = None) -> list[str]:
    """버전 키 목록.

    - `get_list()` → 전체 61개
    - `get_list(year=2025)` → 해당 연도 버전만

    Args:
        year: 4자리 연도. **int 만 허용** (문자열은 TypeError).
    """
    if year is None:
        return list(VERSIONS)
    if not isinstance(year, int) or isinstance(year, bool):
        raise TypeError(f"year must be int, got {type(year).__name__}")
    prefix = f"{year:04d}"
    return [k for k in VERSIONS if k.startswith(prefix)]


def find(
    name: str,
    level: str | None = None,
    exact: bool = False,
    year: list[int] | None = None,
) -> pd.DataFrame:
    """행정구역명으로 버전 검색. NFC 정규화 후 대소문자·공백 무시 substring 매칭.

    Args:
        name: 검색할 이름. 공백으로 토큰 구분 가능:
            - 1 토큰 `"종로"` → 전 레벨 substring
            - 2 토큰 `"서울특별시 종로구"` → **sgg 만** 자동 필터
            - 3 토큰 `"서울특별시 종로구 사직동"` → **emd 만** 자동 필터
            - 4 토큰 이상은 `ValueError`

            `level=` 을 명시하면 자동 필터보다 우선.
            매칭은 `sidonm + sggnm + name` 을 이어붙여 공백을 제거한 문자열에 대한
            substring. 그래서 `"수원시 권선구"` 도 `"수원시권선구"` 로 저장된 sgg 를 찾아낸다.
        level: `"sido"` / `"sgg"` / `"emd"` 중 하나, 또는 `None` (자동).
        exact: True 면 `name` 컬럼 단독 완전일치. 공백 포함 쿼리와 결합시 `ValueError`.
        year: `[2025]` 단일 연도, `[2000, 2005]` 범위(inclusive). 3개 이상은 ValueError.

    Returns:
        DataFrame. 컬럼 순서:
        `version_key, level, sidonm, sggnm, name, code, code7, code8, sggcd, sidocd`.

        - `code` = level 별 행안부 코드 (sido 2자리 / sgg 5자리 / emd 10자리)
        - `code7`, `code8` = 통계청 7/8자리. emd 레벨에서만 채워짐
        - `sggcd`, `sggnm` = 상위 시군구 (emd 행에서만 채워짐)
        - `sidonm`, `sidocd` = 상위 시도 (emd/sgg 행에서 채워짐)
    """
    return _index.find(name, level=level, exact=exact, year=year)


def get(
    key: str,
    level: str = "emd",
    *,
    force_refresh: bool = False,
) -> gpd.GeoDataFrame:
    """특정 버전의 지도를 GeoDataFrame 으로 반환 (EPSG:5179).

    Args:
        key: 버전 키 문자열 (예: `"20250401"`). int 입력 거부.
        level: `"emd"` / `"sgg"` / `"sido"` 중 하나.
        force_refresh: True 면 캐시 무시 재다운로드.
    """
    if not isinstance(key, str):
        raise TypeError(
            f"key must be a version key string like '20250401', got {type(key).__name__}. "
            "Use adk.get_list() to see available keys."
        )
    if key not in VERSIONS:
        raise ValueError(
            f"unknown version key: {key!r}. "
            "Use adk.get_list() to see available keys."
        )
    if level not in _LEVELS:
        raise ValueError(f"level must be one of {_LEVELS}, got {level!r}")

    filename = f"{level}_{key}.parquet"
    path = download_if_needed(filename, force_refresh=force_refresh)
    return gpd.read_parquet(path)
