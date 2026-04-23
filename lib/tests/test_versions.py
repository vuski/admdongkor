"""VERSIONS 상수 자체 불변 검증."""

from admdongkor._versions import VERSIONS


def test_count():
    assert len(VERSIONS) == 62


def test_sorted():
    assert VERSIONS == sorted(VERSIONS)


def test_format():
    for k in VERSIONS:
        assert len(k) == 8 and k.isdigit()


def test_excluded_keys_not_present():
    excluded = {"20161231", "20200401", "20211001"}
    assert excluded.isdisjoint(VERSIONS)


def test_bounds():
    assert VERSIONS[0] == "19751231"
    assert VERSIONS[-1] == "20260401"
