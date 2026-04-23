"""match_adm 동작 테스트. 실제 embed 된 timeline/shape_pairs 사용."""

from __future__ import annotations

import pandas as pd
import pytest

import admdongkor as adk
from admdongkor._match import MatchResult


# ──────── 기본 타입 / 스키마 ────────

def test_returns_matchresult():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    assert isinstance(r, MatchResult)
    assert isinstance(r, pd.DataFrame)


def test_output_columns():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    assert list(r.columns) == [
        "version_key", "emdcd", "emdnm",
        "sggcd", "sggnm", "sidocd", "sidonm",
        "area", "weight",
    ]


def test_weight_range():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    assert r.weight.min() > 0
    assert r.weight.max() <= 1.0


# ──────── 군위군 편입 케이스 (핵심 검증) ────────

def test_gunwi_appears_for_daegu_2025():
    """2025 대구(27) 영역에 걸치는 2011 emd 에 경북(47) 군위군 (sggcd=47720) 이 포함돼야."""
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    gunwi = r[r.sggcd == "47720"]
    assert len(gunwi) > 0
    # 군위군 읍면은 전부 대구 영역에 들어가야 (weight ≈ 1.0)
    assert (gunwi.weight > 0.9).all()


def test_daegu_sido_aggregation_close_to_1():
    """sido() 호출시 대구 자체의 weight 는 거의 1.0 이어야."""
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    sido = r.sido()
    daegu_2011 = sido[sido.sidocd == "27"]
    assert len(daegu_2011) == 1
    assert daegu_2011.weight.iloc[0] > 0.99


def test_gyeongbuk_sido_aggregation_small():
    """경북(47) 은 군위군만 걸치니 비율 작아야 (3% 안팎)."""
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    sido = r.sido()
    gb = sido[sido.sidocd == "47"]
    assert len(gb) == 1
    assert 0.01 < gb.weight.iloc[0] < 0.1


# ──────── target 단일/리스트 ────────

def test_target_single_string():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    assert r.version_key.unique().tolist() == ["20111231"]


def test_target_list_multi():
    r = adk.match_adm(
        base="20251231", region="27",
        target=["20111231", "20241231"],
    )
    versions = sorted(r.version_key.unique().tolist())
    assert versions == ["20111231", "20241231"]


# ──────── sgg/sido 메서드 ────────

def test_sgg_method():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    sgg = r.sgg()
    assert set(sgg.columns) == {
        "version_key", "sggcd", "sggnm", "sidocd", "sidonm", "area", "weight",
    }
    assert (sgg.weight <= 1.0).all()


def test_sido_method():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    sido = r.sido()
    assert set(sido.columns) == {
        "version_key", "sidocd", "sidonm", "area", "weight",
    }


def test_emd_method_returns_dataframe():
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    emd = r.emd()
    assert isinstance(emd, pd.DataFrame)
    assert len(emd) == len(r)


# ──────── region 자리수별 ────────

def test_region_sgg_5digit():
    """대구 동구(27140) 만 쿼리."""
    r = adk.match_adm(base="20251231", region="27140", target="20111231")
    assert len(r) > 0
    # 대부분 rows 가 대구 동구 또는 인접 (2011 동구 주 구성)
    assert (r.sggcd == "27140").any()


def test_region_emd_10digit():
    """특정 emd 단일 코드 쿼리."""
    r = adk.match_adm(base="20251231", region="2714076000", target="20111231")
    assert len(r) > 0


def test_region_bad_length_raises():
    with pytest.raises(ValueError):
        adk.match_adm(base="20251231", region="123", target="20111231")


# ──────── min_weight 필터 ────────

def test_min_weight_filter():
    r_all = adk.match_adm(base="20251231", region="27", target="20111231")
    r_strict = adk.match_adm(
        base="20251231", region="27", target="20111231", min_weight=0.5,
    )
    assert len(r_strict) <= len(r_all)
    if len(r_strict) > 0:
        assert (r_strict.weight >= 0.5).all()


# ──────── 유효성 ────────

def test_bad_base_raises():
    with pytest.raises(ValueError):
        adk.match_adm(base="20990101", region="27", target="20111231")


def test_bad_target_raises():
    with pytest.raises(ValueError):
        adk.match_adm(base="20251231", region="27", target="20990101")


def test_bad_target_type_raises():
    with pytest.raises(TypeError):
        adk.match_adm(base="20251231", region="27", target=20111231)


def test_positional_args_rejected():
    """keyword-only 강제."""
    with pytest.raises(TypeError):
        adk.match_adm("20251231", "27", "20111231")


def test_empty_result_for_nonexistent_region():
    """base 시점에 존재하지 않는 코드는 빈 결과."""
    # 99 는 실재 sidocd 에 없음
    r = adk.match_adm(base="20251231", region="99", target="20111231")
    assert len(r) == 0


# ──────── 합산 검증 ────────

def test_weight_summed_for_overlapping_base():
    """sido 쿼리는 여러 base emd → 같은 target emd 가 여러 번 매칭되면 합산된다."""
    r = adk.match_adm(base="20251231", region="27", target="20111231")
    # 같은 (version, emdcd) 가 한 행씩만 있는지
    keys = r[["version_key", "emdcd"]]
    assert not keys.duplicated().any()
