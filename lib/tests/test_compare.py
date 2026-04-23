"""compare() 동작 테스트. embed 된 실제 인덱스 사용."""

from __future__ import annotations

import pandas as pd
import pytest

import admdongkor as adk
from admdongkor._compare import CompareResult


# ──────── 타입 / 반환 ────────

def test_returns_compare_result():
    r = adk.compare(["20251231", "20111231"])
    assert isinstance(r, CompareResult)
    assert r.va == "20251231"
    assert r.vb == "20111231"


def test_same_and_diff_are_dataframes():
    r = adk.compare(["20251231", "20111231"])
    assert isinstance(r.same(), pd.DataFrame)
    assert isinstance(r.diff(), pd.DataFrame)


def test_same_columns():
    r = adk.compare(["20251231", "20111231"])
    s = r.same()
    assert list(s.columns) == [
        "version_key", "emdcd", "emdnm",
        "sggcd", "sggnm", "sidocd", "sidonm",
        "shape_id",
    ]


def test_diff_columns_include_status_and_iou():
    r = adk.compare(["20251231", "20111231"])
    d = r.diff()
    assert "status" in d.columns
    assert "iou" in d.columns


# ──────── 의미 검증 ────────

def test_same_version_yields_no_diff():
    """같은 버전끼리 비교하면 diff 는 비어야 한다."""
    r = adk.compare(["20251231", "20251231"])
    assert len(r.diff()) == 0


def test_same_contains_both_versions():
    """same 은 emdcd 당 2 rows (va, vb)."""
    r = adk.compare(["20251231", "20111231"])
    s = r.same()
    if len(s) > 0:
        counts = s.groupby("emdcd").size()
        # 대부분 2 (양쪽에서 선택) — 하나만 나오면 잘못된 것
        assert (counts == 2).all()


def test_diff_status_values():
    r = adk.compare(["20251231", "20111231"])
    d = r.diff()
    assert set(d.status.unique()) <= {"changed", "only_in_a", "only_in_b"}


def test_changed_has_iou_below_threshold():
    """changed 인 emd 의 iou 는 threshold 미만."""
    threshold = 0.99
    r = adk.compare(["20251231", "20111231"], threshold=threshold)
    changed = r.diff()[r.diff().status == "changed"]
    if len(changed) > 0:
        assert (changed.iou < threshold).all()


def test_only_in_a_has_version_va():
    r = adk.compare(["20251231", "20111231"])
    d = r.diff()
    only_a = d[d.status == "only_in_a"]
    if len(only_a) > 0:
        assert (only_a.version_key == "20251231").all()


def test_only_in_b_has_version_vb():
    r = adk.compare(["20251231", "20111231"])
    d = r.diff()
    only_b = d[d.status == "only_in_b"]
    if len(only_b) > 0:
        assert (only_b.version_key == "20111231").all()


# ──────── threshold 효과 ────────

def test_threshold_zero_means_all_same():
    """threshold=0.0 이면 shape_pairs 의 거의 모든 쌍이 same 으로 승격.
    공간적 교차가 있는 이상 iou > 0 이므로 diff 에서 changed 는 크게 줄어듬."""
    r_strict = adk.compare(["20251231", "20111231"], threshold=1.0)
    r_loose = adk.compare(["20251231", "20111231"], threshold=0.0)
    strict_changed = (r_strict.diff().status == "changed").sum()
    loose_changed = (r_loose.diff().status == "changed").sum()
    assert loose_changed <= strict_changed


def test_threshold_1_strict():
    """threshold=1.0 이면 shape_id 완전 일치만 same."""
    r = adk.compare(["20251231", "20111231"], threshold=1.0)
    # same 의 두 version 은 shape_id 가 같아야
    s = r.same()
    if len(s) > 0:
        pairs = s.groupby("emdcd").shape_id.unique()
        # 각 emdcd 의 shape_id 가 하나로 묶여야
        for ids in pairs:
            assert len(ids) == 1


# ──────── 에러 케이스 ────────

def test_non_list_raises():
    with pytest.raises(TypeError):
        adk.compare("20251231")


def test_wrong_length_raises():
    with pytest.raises(ValueError):
        adk.compare(["20251231"])
    with pytest.raises(ValueError):
        adk.compare(["20251231", "20111231", "20201001"])


def test_bad_version_raises():
    with pytest.raises(ValueError):
        adk.compare(["20251231", "20990101"])


def test_threshold_out_of_range():
    with pytest.raises(ValueError):
        adk.compare(["20251231", "20111231"], threshold=1.5)
    with pytest.raises(ValueError):
        adk.compare(["20251231", "20111231"], threshold=-0.1)


# ──────── 세종시 검증 (특징적 케이스) ────────

def test_sejong_appears_in_only_in_a_for_2011_vs_2013():
    """2011 에는 세종 없고 2013 에 있음 → 2013 쪽 emd 중 세종 소속이 only_in_b 에 나와야.
    여기서 va=20111231, vb=20131231."""
    r = adk.compare(["20111231", "20131231"])
    only_b = r.diff()[r.diff().status == "only_in_b"]
    # 세종 sidonm 포함 emd 가 있어야
    sejong = only_b[only_b.sidonm.astype(str).str.contains("세종", na=False)]
    assert len(sejong) > 0
