"""네트워크 없이 돌아가는 API 테스트 (get_list, get/find 의 input validation)."""

import pytest

import admdongkor as adk


class TestGetList:
    def test_no_arg(self):
        assert len(adk.get_list()) == 61

    def test_returns_list_copy(self):
        a = adk.get_list()
        a.append("fake")
        assert len(adk.get_list()) == 61  # 원본 건드리지 않음

    def test_year_2025(self):
        assert adk.get_list(year=2025) == [
            "20250101", "20250401", "20250701", "20251001", "20251231",
        ]

    def test_year_1975(self):
        assert adk.get_list(year=1975) == ["19751231"]

    def test_year_no_data(self):
        assert adk.get_list(year=1999) == []

    def test_str_year_raises(self):
        with pytest.raises(TypeError):
            adk.get_list(year="2025")

    def test_bool_year_raises(self):
        with pytest.raises(TypeError):
            adk.get_list(year=True)

    def test_float_year_raises(self):
        with pytest.raises(TypeError):
            adk.get_list(year=2025.0)


class TestGetValidation:
    def test_int_key_raises(self):
        with pytest.raises(TypeError):
            adk.get(20250401, "emd")

    def test_unknown_key_raises(self):
        with pytest.raises(ValueError):
            adk.get("20200401", "emd")

    def test_bad_level_raises(self):
        with pytest.raises(ValueError):
            adk.get("20250401", "bjd")
