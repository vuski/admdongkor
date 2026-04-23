"""네트워크 없이 돌아가는 API 테스트 (get_list, get/find 의 input validation)."""

import pytest

import admdongkor as adk


class TestGetList:
    def test_no_arg(self):
        assert len(adk.get_list()) == 62

    def test_returns_list_copy(self):
        a = adk.get_list()
        a.append("fake")
        assert len(adk.get_list()) == 62  # 원본 건드리지 않음

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
    """crs 파라미터 + detail 동작. 캐시에 parquet 있을 때만 실제 로드 검증.

    파이썬 get() 은 light/detail 구분 없이 **기본 반환 CRS = EPSG:5179**.
    저장 파일은 light=4326, detail=5179 로 다르지만, 파이썬 사용자에게는
    일관된 5179 로 보여준다 (면적·거리 계산 그대로 가능).
    """

    def _cached_light_sido(self):
        """캐시에 simplified/sido_20250401_light.parquet 있으면 경로, 없으면 None."""
        p = adk.cache_dir() / "simplified" / "sido_20250401_light.parquet"
        return p if p.exists() else None

    def _cached_full_sido(self):
        """캐시에 sido_20250401.parquet 있으면 경로 반환, 없으면 None."""
        p = adk.cache_dir() / "sido_20250401.parquet"
        return p if p.exists() else None

    def test_default_is_5179_light(self):
        """기본(detail=False) 도 반환 CRS 는 EPSG:5179 로 재투영."""
        if not self._cached_light_sido():
            pytest.skip("no cached light parquet")
        gdf = adk.get("20250401", "sido")
        assert gdf.crs.to_epsg() == 5179

    def test_detail_true_is_5179(self):
        """detail=True 는 원본 그대로 EPSG:5179."""
        if not self._cached_full_sido():
            pytest.skip("no cached full parquet")
        gdf = adk.get("20250401", "sido", detail=True)
        assert gdf.crs.to_epsg() == 5179

    def test_crs_string_wgs84_on_light(self):
        """detail=False + crs='EPSG:4326' → 5179 → WGS84 로 재투영."""
        if not self._cached_light_sido():
            pytest.skip("no cached light parquet")
        gdf = adk.get("20250401", "sido", crs="EPSG:4326")
        assert gdf.crs.to_epsg() == 4326
        minx, miny, maxx, maxy = gdf.total_bounds
        assert 124 < minx < 132
        assert 33 < miny < 43

    def test_crs_int(self):
        if not self._cached_light_sido():
            pytest.skip("no cached light parquet")
        gdf = adk.get("20250401", "sido", crs=4326)
        assert gdf.crs.to_epsg() == 4326

    def test_crs_none_light_is_5179(self):
        """detail=False + crs=None → 5179 (light 파일의 4326 을 자동 재투영)."""
        if not self._cached_light_sido():
            pytest.skip("no cached light parquet")
        gdf = adk.get("20250401", "sido", crs=None)
        assert gdf.crs.to_epsg() == 5179

    def test_crs_none_detail_is_5179(self):
        """detail=True + crs=None → 원본 5179 그대로."""
        if not self._cached_full_sido():
            pytest.skip("no cached full parquet")
        gdf = adk.get("20250401", "sido", detail=True, crs=None)
        assert gdf.crs.to_epsg() == 5179


class TestCachedPath:
    """cached_path() 의 subdir 인자 동작."""

    def test_no_subdir(self):
        from admdongkor._cache import cache_dir, cached_path
        p = cached_path("emd_20250401.parquet")
        assert p == cache_dir() / "emd_20250401.parquet"

    def test_with_subdir(self):
        from admdongkor._cache import cache_dir, cached_path
        p = cached_path("emd_20250401_light.parquet", subdir="simplified")
        assert p == cache_dir() / "simplified" / "emd_20250401_light.parquet"
