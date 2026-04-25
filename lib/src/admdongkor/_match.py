"""영역 기반 시계열 매칭 (match_adm).

base 시점의 region (sido/sgg/emd 코드) 경계를 기준으로, target 시점의
읍면동들이 그 경계에 얼마나 걸치는지 (weight) 반환.

원리:
    1. timeline_v3_emd 에서 base 의 emd 집합과 각 emd 의 global shape_id
    2. shape_pairs_v3_emd 에서 base shape_id 와 공간 겹쳤던 shape_id 목록 + weight
    3. target version 에 해당 shape_id 들이 등장하는 emd 를 찾아 매칭
    4. 동일 target emd 가 여러 base emd 로부터 매칭되면 weight 합산

인덱스는 로컬 캐시 (`_cache.index_dir()`) 에서 로드. 캐시는 import 시
`_cache.ensure_latest()` 가 GitHub dist/data/ 에서 받아둔다.
"""

from __future__ import annotations

from functools import lru_cache

import pandas as pd

from . import _cache

_OUTPUT_EMD_COLS = [
    "version_key", "emdcd", "emdnm",
    "sggcd", "sggnm", "sidocd", "sidonm",
    "area", "weight",
]


class MatchResult(pd.DataFrame):
    """`match_adm()` 반환. pd.DataFrame 이지만 `.emd() / .sgg() / .sido()` 로 레벨 변환."""

    @property
    def _constructor(self):
        return MatchResult

    def emd(self) -> pd.DataFrame:
        """emd 단위 (기본 저장 형태). 이 메서드는 순수 DataFrame 을 반환."""
        return pd.DataFrame(self.copy())

    def sgg(self) -> pd.DataFrame:
        """sgg 단위 집계.

        weight = Σ(emd_weight × emd_area) / sgg_total_area
        (sgg_total_area 는 해당 target 시점의 sgg 면적 = timeline_v3_sgg 에서)

        해석: "target sgg 의 몇 %가 base 영역에 속하는가"
        """
        return _aggregate_to_level(self, "sgg")

    def sido(self) -> pd.DataFrame:
        """sido 단위 집계. 해석은 sgg 와 동일."""
        return _aggregate_to_level(self, "sido")


@lru_cache(maxsize=3)
def _load_timeline(level: str) -> pd.DataFrame:
    fname = f"timeline_v3_{level}.parquet"
    return pd.read_parquet(_cache.index_data_path(fname))


@lru_cache(maxsize=3)
def _load_shape_pairs(level: str) -> pd.DataFrame:
    fname = f"shape_pairs_v3_{level}.parquet"
    return pd.read_parquet(_cache.index_data_path(fname))


def _resolve_region_mask(tl: pd.DataFrame, base: str, region: str) -> pd.Series:
    """region 코드 자리수로 적절한 컬럼에 매칭 mask 생성."""
    L = len(region)
    v = tl.version_key == base
    if L == 2:
        return v & (tl.sidocd == region)
    if L == 5:
        return v & (tl.sggcd == region)
    if L == 7:
        return v & (tl.emd7 == region)
    if L == 10:
        return v & (tl.element_id == region)
    raise ValueError(
        f"region must be 2/5/7/10 digit code, got len={L}: {region!r}"
    )


def _related_shapes(sp: pd.DataFrame, base_shapes: set[int]) -> pd.DataFrame:
    """base_shapes 와 shape_pairs 로 연결된 (other_shape, weight) 수집.

    base 가 shape_id_a 쪽 → w_backward = area(∩) / area_b = target 기준 비율
    base 가 shape_id_b 쪽 → w_forward  = area(∩) / area_a = target 기준 비율
    """
    a_side = sp[sp.shape_id_a.isin(base_shapes)][["shape_id_b", "w_backward"]].rename(
        columns={"shape_id_b": "other_shape", "w_backward": "weight"}
    )
    b_side = sp[sp.shape_id_b.isin(base_shapes)][["shape_id_a", "w_forward"]].rename(
        columns={"shape_id_a": "other_shape", "w_forward": "weight"}
    )
    return pd.concat([a_side, b_side], ignore_index=True)


def match_adm(
    *,
    base: str,
    region: str,
    target: str | list[str],
    min_weight: float = 0.0,
) -> MatchResult:
    """base 시점 region 영역에 걸치는 target 시점 emd 목록 + weight.

    Args:
        base: 버전 키 (예: `"20251231"`). region 이 이 시점에 존재해야 함.
        region: 2/5/7/10 자리 코드.
            - 2자리: 시도 (행안부)
            - 5자리: 시군구 (행안부)
            - 7자리: 읍면동 (통계청 과거 코드, 1975-2015 주로)
            - 10자리: 읍면동 (행안부)
        target: 버전 키 하나 또는 리스트. 각 target 시점의 matched emd 반환.
        min_weight: 이 값 미만 weight 는 결과에서 제외. 기본 0.0 (필터 없음).

    Returns:
        MatchResult (pd.DataFrame). 컬럼:
            version_key, emdcd, emdnm, sggcd, sggnm, sidocd, sidonm, area, weight
        weight 의미: "이 target emd 의 몇 %가 base region 영역에 속하는가"
                     = area(target_emd ∩ base_region) / area(target_emd)
        동일 (version_key, emdcd) 이 여러 base emd 로부터 매칭되면 weight 합산 (1.0 상한).

        `.emd()` / `.sgg()` / `.sido()` 로 레벨 변환 가능.
    """
    if not isinstance(base, str):
        raise TypeError(f"base must be str, got {type(base).__name__}")
    if not isinstance(region, str):
        raise TypeError(f"region must be str, got {type(region).__name__}")
    if isinstance(target, str):
        targets = [target]
    elif isinstance(target, list) and all(isinstance(t, str) for t in target):
        targets = target
    else:
        raise TypeError("target must be str or list[str]")
    if not targets:
        raise ValueError("target must have at least one version key")

    tl = _load_timeline("emd")
    sp = _load_shape_pairs("emd")

    # 1. region → base emds + shape_id 집합
    base_mask = _resolve_region_mask(tl, base, region)
    base_rows = tl[base_mask]
    if base_rows.empty:
        return MatchResult(pd.DataFrame(columns=_OUTPUT_EMD_COLS))

    base_shapes = set(base_rows.shape_id.tolist())

    # 2. shape_pairs 로 연결된 다른 shape 들
    related = _related_shapes(sp, base_shapes)
    if min_weight > 0:
        related = related[related.weight >= min_weight]

    # 3. 각 target 시점에 대해 매칭 결과 수집
    results: list[pd.DataFrame] = []
    for t in targets:
        tl_t = tl[tl.version_key == t]
        if tl_t.empty:
            continue

        # 3a. 같은 shape_id (weight = 1.0)
        same = tl_t[tl_t.shape_id.isin(base_shapes)].copy()
        same["weight"] = 1.0
        results.append(same)

        # 3b. 연결된 shape_id
        if not related.empty:
            rel = related.merge(tl_t, left_on="other_shape", right_on="shape_id",
                                how="inner")
            rel = rel.drop(columns=["other_shape"])
            results.append(rel)

    if not results:
        return MatchResult(pd.DataFrame(columns=_OUTPUT_EMD_COLS))

    out = pd.concat(results, ignore_index=True)

    # 4. 중복 합산: 같은 (version_key, element_id) 는 weight 합산
    out = out.groupby(["version_key", "element_id"], as_index=False).agg({
        "name": "first",
        "sggcd": "first", "sggnm": "first",
        "sidocd": "first", "sidonm": "first",
        "area": "first",
        "weight": "sum",
    })
    # 부동소수·겹침으로 1.0 초과 가능 → clamp
    out["weight"] = out["weight"].clip(upper=1.0)
    if min_weight > 0:
        out = out[out.weight >= min_weight]

    # 5. 컬럼 정리 & 정렬
    out = out.rename(columns={"element_id": "emdcd", "name": "emdnm"})
    out = out[_OUTPUT_EMD_COLS]
    out = out.sort_values(
        ["version_key", "weight"], ascending=[True, False],
    ).reset_index(drop=True)

    return MatchResult(out)


def _aggregate_to_level(emd_result: pd.DataFrame, target_level: str) -> pd.DataFrame:
    """emd 단위 결과를 sgg/sido 단위로 집계.

    weight_out = Σ(emd_weight × emd_area) / total_area_of_level
    total_area_of_level: 해당 target version 의 그 sgg/sido 의 총 면적.
    """
    if emd_result.empty:
        cols = ["version_key"]
        if target_level == "sgg":
            cols += ["sggcd", "sggnm", "sidocd", "sidonm", "area", "weight"]
        else:
            cols += ["sidocd", "sidonm", "area", "weight"]
        return pd.DataFrame(columns=cols)

    code_col = "sggcd" if target_level == "sgg" else "sidocd"
    name_col = "sggnm" if target_level == "sgg" else "sidonm"

    # numerator: Σ(weight × area) per (version, target_code)
    tmp = emd_result.copy()
    tmp["_wa"] = tmp["weight"] * tmp["area"]
    agg_keys = ["version_key", code_col]
    if target_level == "sgg":
        num = tmp.groupby(agg_keys, as_index=False).agg({
            "_wa": "sum",
            name_col: "first",
            "sidocd": "first", "sidonm": "first",
        })
    else:
        num = tmp.groupby(agg_keys, as_index=False).agg({
            "_wa": "sum",
            name_col: "first",
        })

    # denominator: target level 의 total area — timeline_v3_<level> 에서 조회
    tl_level = _load_timeline(target_level)
    versions = tmp.version_key.unique().tolist()
    codes = tmp[code_col].unique().tolist()
    denom_src = tl_level[
        tl_level.version_key.isin(versions) & (tl_level.element_id.isin(codes))
    ][["version_key", "element_id", "area"]].rename(
        columns={"element_id": code_col, "area": "_total_area"}
    )

    merged = num.merge(denom_src, on=["version_key", code_col], how="left")
    # _total_area 가 없으면 (level 테이블에 없는 코드) numerator 합으로 fallback
    # 이 경우 weight 는 과대평가될 수 있음
    total_area = merged["_total_area"].fillna(merged["_wa"])
    merged["weight"] = (merged["_wa"] / total_area).clip(upper=1.0)
    merged = merged.rename(columns={"_total_area": "area"})

    if target_level == "sgg":
        out_cols = ["version_key", "sggcd", "sggnm",
                    "sidocd", "sidonm", "area", "weight"]
    else:
        out_cols = ["version_key", "sidocd", "sidonm", "area", "weight"]
    out = merged[out_cols]
    return out.sort_values(["version_key", "weight"],
                           ascending=[True, False]).reset_index(drop=True)
