"""adk.versions() 동작 테스트."""

from __future__ import annotations

import pytest

import admdongkor as adk
from admdongkor import _cache
from admdongkor._versions import VERSIONS
from admdongkor.api import VersionList


@pytest.fixture(autouse=True)
def _no_manifest(monkeypatch):
    """사용자 캐시의 manifest 를 격리 — 결과가 패키지 스냅샷 VERSIONS 만으로 정해지게.

    get_list()/versions() 는 캐시된 manifest 의 versions 를 합치므로, 격리하지 않으면
    테스트 결과가 실행 환경의 캐시 상태에 따라 달라진다.
    """
    monkeypatch.setattr(_cache, "manifest_versions", lambda: None)


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
    assert v[-1] == VERSIONS[-1]


def test_tail_custom():
    v = adk.versions().tail(2)
    assert len(v) == 2
    assert v[-1] == VERSIONS[-1]


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
    assert v[-1] == VERSIONS[-1]
    assert v[0:3] == ["19751231", "19801231", "19851231"]


def test_get_list_still_works():
    """이전 이름 get_list 도 유지."""
    assert adk.get_list() == list(adk.versions())
    assert adk.get_list(2023) == list(adk.versions(2023))
