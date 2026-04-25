"""공개 API: get_list, find, get, match_adm, compare."""

from __future__ import annotations

from typing import Literal

import geopandas as gpd
import pandas as pd

from . import _cache, _compare, _index, _match, _report
from ._cache import download_if_needed
from ._compare import CompareResult
from ._match import MatchResult
from ._versions import VERSIONS

report_issue = _report.report_issue

Level = Literal["emd", "sgg", "sido"]
_LEVELS: tuple[Level, ...] = ("emd", "sgg", "sido")


class VersionList(list):
    """list 서브클래스. pandas 스타일 `.head()` / `.tail()` 편의 메서드 추가.

    모든 표준 list 연산 (인덱싱, 순회, len 등) 가능.
    """

    def head(self, n: int = 5) -> list[str]:
        """처음 n 개 (기본 5)."""
        return list(self[:n])

    def tail(self, n: int = 5) -> list[str]:
        """마지막 n 개 (기본 5)."""
        return list(self[-n:]) if n > 0 else []


def versions(year: int | None = None) -> VersionList:
    """버전 키 목록.

    - `adk.versions()` → 전체 버전
    - `adk.versions(2005)` → 해당 연도만
    - `adk.versions().head()` / `.tail()` → 처음/마지막 5개

    Args:
        year: 4자리 연도. **int 만 허용** (문자열 입력은 TypeError).
    """
    if year is None:
        return VersionList(VERSIONS)
    if not isinstance(year, int) or isinstance(year, bool):
        raise TypeError(f"year must be int, got {type(year).__name__}")
    prefix = f"{year:04d}"
    return VersionList(k for k in VERSIONS if k.startswith(prefix))


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
        `FindResult` (= `pd.DataFrame` 서브클래스). 컬럼 순서:
        `version_key, level, sidonm, sggnm, name, code, code7, code8, sggcd, sidocd`.

        - `code` = level 별 행안부 코드 (sido 2자리 / sgg 5자리 / emd 10자리)
        - `code7`, `code8` = 통계청 7/8자리. emd 레벨에서만 채워짐
        - `sggcd`, `sggnm` = 상위 시군구 (emd 행에서만 채워짐)
        - `sidonm`, `sidocd` = 상위 시도 (emd/sgg 행에서 채워짐)

        체이닝 메서드:
        - `.versions()` → 매치된 고유 `version_key` 리스트
        - `.first()` → 가장 이른 version_key (비어있으면 `None`)
        - `.last()` → 가장 늦은 version_key (비어있으면 `None`)
    """
    return _index.find(name, level=level, exact=exact, year=year)


def get(
    key: str,
    level: str = "emd",
    *,
    detail: bool = False,
    crs: str | int | None = None,
    force_refresh: bool = False,
) -> gpd.GeoDataFrame:
    """특정 버전의 지도를 GeoDataFrame 으로 반환.

    **기본 CRS 는 항상 EPSG:5179** (한국 직각좌표계, m 단위). light 파일은
    저장 포맷상 EPSG:4326 이지만 파이썬 get() 은 자동으로 5179 로 재투영한다.
    면적·거리 계산을 그대로 할 수 있도록 원본·light 의 기본 CRS 를 일치시킨다.

    Args:
        key: 버전 키 문자열 (예: `"20250401"`). int 입력 거부.
        level: `"emd"` / `"sgg"` / `"sido"` 중 하나.
        detail: False (기본) 이면 **단순화 light 버전**(mapshaper 18.7% simplify,
            작은 홀 제거) 을 받는다. emd 약 2.4MB / sgg 약 1MB / sido 약 0.5MB.
            True 면 **원본 해상도**(emd 약 11MB) — 좀 더 상세한 버전이 필요할 때.
            두 경우 모두 기본 반환 CRS 는 EPSG:5179.
        crs: 재투영 대상 CRS. `None` (기본) 이면 EPSG:5179 반환.
            `"EPSG:4326"` 또는 `4326` 처럼 문자열·int 모두 허용.
        force_refresh: True 면 캐시 무시 재다운로드.

    Examples:
        >>> adk.get("20250401", "sido")                    # light, EPSG:5179 (기본)
        >>> adk.get("20250401", "sido", detail=True)       # 원본, EPSG:5179
        >>> adk.get("20250401", "sido", crs="EPSG:4326")   # light → WGS84
        >>> adk.get("20250401", "sido", crs="EPSG:3857")   # light → Web Mercator
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

    if detail:
        filename = f"{level}_{key}.parquet"
        path = download_if_needed(filename, force_refresh=force_refresh)
    else:
        filename = f"{level}_{key}_light.parquet"
        path = download_if_needed(filename, subdir="simplified",
                                  force_refresh=force_refresh)
    gdf = gpd.read_parquet(path)
    # light 는 저장 CRS 가 4326. 기본 반환은 항상 5179 로 맞춤.
    # crs 명시 시 그 값이 우선 (5179 에서 최종 CRS 로 재투영).
    if gdf.crs is not None and gdf.crs.to_epsg() != 5179:
        gdf = gdf.to_crs(5179)
    if crs is not None:
        gdf = gdf.to_crs(crs)
    return gdf


def match_adm(
    *,
    base: str,
    region: str,
    target: str | list[str],
    min_weight: float = 0.0,
) -> MatchResult:
    """base 시점 region 영역에 걸치는 target 시점 emd 목록 + weight 반환.

    Args:
        base: 버전 키 (예: `"20251231"`).
        region: 2/5/7/10 자리 코드.
            - 2자리: 시도 (행안부)
            - 5자리: 시군구 (행안부)
            - 7자리: 읍면동 (통계청 과거 코드)
            - 10자리: 읍면동 (행안부)
        target: 버전 키 하나 또는 리스트.
        min_weight: 이 값 미만 weight 는 제외. 기본 0.0.

    Returns:
        `MatchResult`. 컬럼:
            `version_key, emdcd, emdnm, sggcd, sggnm, sidocd, sidonm, area, weight`

        weight = "target emd 면적 중 base region 영역에 속하는 비율"
               = `area(target_emd ∩ base_region) / area(target_emd)`

        `.emd()` / `.sgg()` / `.sido()` 로 레벨 변환.

    Examples:
        >>> adk.match_adm(base="20251231", region="27", target="20111231")
        # 2025 대구광역시 영역에 걸치는 2011 emd 들 (군위군 + 당시 대구)
        >>> adk.match_adm(base="20251231", region="27", target=["20111231", "20241231"]).sgg()
        # sgg 단위 집계 (각 sgg 의 몇 %가 base 영역에 속하는가)
    """
    if base not in VERSIONS:
        raise ValueError(
            f"unknown base version key: {base!r}. "
            "Use adk.get_list() to see available keys."
        )
    if isinstance(target, str):
        target_list = [target]
    elif isinstance(target, list):
        target_list = target
    else:
        raise TypeError("target must be str or list[str]")
    for t in target_list:
        if t not in VERSIONS:
            raise ValueError(
                f"unknown target version key: {t!r}. "
                "Use adk.get_list() to see available keys."
            )
    return _match.match_adm(
        base=base, region=region, target=target, min_weight=min_weight,
    )


def data_version() -> str | None:
    """현재 로컬 캐시에 반영된 인덱스 data_version.

    캐시가 없거나 아직 갱신된 적 없으면 `None`. 포맷 예: `"2026.04.25"`.
    """
    return _cache.data_version()


def changelog() -> pd.DataFrame:
    """인덱스 수정 이력.

    반환 컬럼: `version`, `changes`. 최신 항목이 위. 캐시된 manifest 가 없으면
    빈 DataFrame.

    Examples:
        >>> adk.changelog()
            version      changes
        0   2026.04.25   1980 경상북도 대구시수성구 이름 수정
        1   2026.04.20   1975 대전시 prefix 추가
    """
    items = _cache.changelog()
    if not items:
        return pd.DataFrame(columns=["version", "changes"])
    df = pd.DataFrame(items)
    # 누락된 컬럼 보정
    for col in ("version", "changes"):
        if col not in df.columns:
            df[col] = ""
    return df[["version", "changes"]]


def compare(versions: list[str], threshold: float = 0.99) -> CompareResult:
    """두 시점 emd 비교. 경계·이름·코드 변화 찾기.

    Args:
        versions: `[va, vb]` 정확히 2개. 버전 키 문자열.
        threshold: shape_id 가 다를 때도 shape 간 IoU >= threshold 면 same 으로 승격.
            기본 0.99 (미세 경계 변화 무시). 1.0 으로 주면 엄격히 shape_id 일치만.

    Returns:
        `CompareResult`. 두 메서드 제공:
            - `.same()` → 공간 동일 (threshold 이상) emd 들. emdcd 당 2 rows (va, vb).
            - `.diff()` → 변화 있는 emd 들. status 컬럼으로 구분:
                * `changed`   : 둘 다 존재하는데 경계 달라짐 (iou < threshold)
                * `only_in_a` : va 에만 존재 (vb 에서 소멸)
                * `only_in_b` : vb 에만 존재 (va 이후 신설)

    Examples:
        >>> r = adk.compare(["20251231", "20111231"])
        >>> r.same().head()   # 경계 유지된 emd 들
        >>> r.diff()          # 변화된 emd 들 + status
    """
    if not isinstance(versions, list):
        raise TypeError(f"versions must be list, got {type(versions).__name__}")
    for v in versions:
        if v not in VERSIONS:
            raise ValueError(
                f"unknown version key: {v!r}. "
                "Use adk.get_list() to see available keys."
            )
    return _compare.compare(versions=versions, threshold=threshold)
