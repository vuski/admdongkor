"""adk.versions() 동작 테스트."""

from __future__ import annotations

import pytest

import admdongkor as adk
from admdongkor.api import VersionList


def test_returns_version_list():
    v = adk.versions()
    assert isinstance(v, VersionList)
    assert isinstance(v, list)


def test_all_versions():
    v = adk.versions()
    assert len(v) == len(adk.get_list())
    assert v == adk.get_list()


def test_head_default_5():
    v = adk.versions().head()
    assert len(v) == 5
    assert v[0] == "19751231"


def test_head_custom():
    v = adk.versions().head(3)
    assert len(v) == 3


def test_tail_default_5():
    v = adk.versions().tail()
    assert len(v) == 5
    assert v[-1] == "20260401"


def test_tail_custom():
    v = adk.versions().tail(2)
    assert len(v) == 2
    assert v[-1] == "20260401"


def test_year_filter():
    v = adk.versions(2023)
    assert len(v) == 5
    assert all(k.startswith("2023") for k in v)


def test_year_no_match():
    v = adk.versions(1999)
    assert len(v) == 0
    assert isinstance(v, VersionList)


def test_year_str_raises():
    with pytest.raises(TypeError):
        adk.versions("2023")


def test_year_bool_raises():
    # bool 은 int 의 서브클래스라 조심
    with pytest.raises(TypeError):
        adk.versions(True)


def test_indexable():
    v = adk.versions()
    assert v[0] == "19751231"
    assert v[-1] == "20260401"
    assert v[0:3] == ["19751231", "19801231", "19851231"]


def test_get_list_still_works():
    """이전 이름 get_list 도 유지."""
    assert adk.get_list() == list(adk.versions())
    assert adk.get_list(2023) == list(adk.versions(2023))
