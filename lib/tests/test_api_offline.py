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


class TestGetCrs:
    """crs 파라미터 동작. 캐시에 parquet 있을 때만 실제 로드 검증."""

    def _cached_sido(self):
        """캐시에 sido_20250401.parquet 있으면 경로 반환, 없으면 None."""
        p = adk.cache_dir() / "sido_20250401.parquet"
        return p if p.exists() else None

    def test_default_crs_is_5179(self):
        if not self._cached_sido():
            pytest.skip("no cached parquet")
        gdf = adk.get("20250401", "sido")
        assert gdf.crs.to_epsg() == 5179

    def test_crs_string_wgs84(self):
        if not self._cached_sido():
            pytest.skip("no cached parquet")
        gdf = adk.get("20250401", "sido", crs="EPSG:4326")
        assert gdf.crs.to_epsg() == 4326
        # WGS84 한국 경도 대략 124~132, 위도 33~43
        minx, miny, maxx, maxy = gdf.total_bounds
        assert 124 < minx < 132
        assert 33 < miny < 43

    def test_crs_int(self):
        if not self._cached_sido():
            pytest.skip("no cached parquet")
        gdf = adk.get("20250401", "sido", crs=4326)
        assert gdf.crs.to_epsg() == 4326

    def test_crs_none_no_reproject(self):
        if not self._cached_sido():
            pytest.skip("no cached parquet")
        gdf = adk.get("20250401", "sido", crs=None)
        assert gdf.crs.to_epsg() == 5179
