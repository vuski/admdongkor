"""build_index() 함수 테스트. 실제 parquet 파일을 2~3개만 로컬에서 만들어 돌린다."""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from admdongkor import _versions
from admdongkor.build_index import build_index


def _make_emd_parquet(path: Path) -> None:
    gdf = gpd.GeoDataFrame(
        {
            "emd7": pd.array(["1101001", "1101002"], dtype="string"),
            "emd8": pd.array([pd.NA, pd.NA], dtype="string"),
            "emdcd": pd.array(["1111053000", "1111054000"], dtype="string"),
            "emdnm": pd.array(["사직동", "삼청동"], dtype="string"),
            "sggcd": pd.array(["11110", "11110"], dtype="string"),
            "sggnm": pd.array(["종로구", "종로구"], dtype="string"),
            "sidocd": pd.array(["11", "11"], dtype="string"),
            "sidonm": pd.array(["서울특별시", "서울특별시"], dtype="string"),
            "area": [100.0, 200.0],
            "geom": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                     Polygon([(1, 0), (2, 0), (2, 1), (1, 1)])],
        },
        geometry="geom",
        crs="EPSG:5179",
    )
    gdf.to_parquet(path)


def _make_sgg_parquet(path: Path) -> None:
    gdf = gpd.GeoDataFrame(
        {
            "sggcd": pd.array(["11110"], dtype="string"),
            "sggnm": pd.array(["종로구"], dtype="string"),
            "sidocd": pd.array(["11"], dtype="string"),
            "sidonm": pd.array(["서울특별시"], dtype="string"),
            "area": [300.0],
            "geom": [Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])],
        },
        geometry="geom",
        crs="EPSG:5179",
    )
    gdf.to_parquet(path)


def _make_sido_parquet(path: Path) -> None:
    gdf = gpd.GeoDataFrame(
        {
            "sidocd": pd.array(["11"], dtype="string"),
            "sidonm": pd.array(["서울특별시"], dtype="string"),
            "area": [300.0],
            "geom": [Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])],
        },
        geometry="geom",
        crs="EPSG:5179",
    )
    gdf.to_parquet(path)


@pytest.fixture
def mini_repo(tmp_path, monkeypatch):
    """VERSIONS 를 2개로 축소하고 샘플 parquet 을 3레벨 각 2개씩 만든다."""
    monkeypatch.setattr(_versions, "VERSIONS", ["20250401", "20260201"])
    # build_index 모듈에도 참조 강제 갱신
    import admdongkor.build_index as bi
    monkeypatch.setattr(bi, "VERSIONS", ["20250401", "20260201"])

    for key in ["20250401", "20260201"]:
        _make_emd_parquet(tmp_path / f"emd_{key}.parquet")
        _make_sgg_parquet(tmp_path / f"sgg_{key}.parquet")
        _make_sido_parquet(tmp_path / f"sido_{key}.parquet")
    return tmp_path


def test_build_creates_file(mini_repo):
    out = build_index(mini_repo)
    assert out.exists()
    assert out == mini_repo / "_index.parquet"


def test_build_schema(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    assert list(df.columns) == [
        "version_key", "level",
        "code", "code7", "code8",
        "name",
        "sggcd", "sggnm",
        "sidocd", "sidonm",
        "_fullpath",
    ]


def test_build_emd_has_statcodes(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    emd = df[df.level == "emd"]
    assert set(emd["code7"].dropna()) == {"1101001", "1101002"}
    # emd8 은 이 샘플에선 전부 NA
    assert emd["code8"].isna().all()


def test_build_sgg_sido_no_statcodes(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    non_emd = df[df.level != "emd"]
    assert non_emd["code7"].isna().all()
    assert non_emd["code8"].isna().all()


def test_build_emd_has_parent_context(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    emd = df[df.level == "emd"]
    assert (emd["sggnm"] == "종로구").all()
    assert (emd["sidonm"] == "서울특별시").all()


def test_build_fullpath_has_no_whitespace(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    # 공백이 들어있으면 안 됨
    assert not df["_fullpath"].astype(str).str.contains(r"\s", regex=True).any()


def test_build_row_count(mini_repo):
    # 2 versions x (2 emd + 1 sgg + 1 sido) = 8 rows
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    assert len(df) == 8


def test_build_levels_sorted(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    # 같은 version_key 안에서 sido → sgg → emd 순
    v = df[df.version_key == "20250401"]
    assert list(v.level) == ["sido", "sgg", "emd", "emd"]


def test_build_codes_from_correct_column(mini_repo):
    build_index(mini_repo)
    df = pd.read_parquet(mini_repo / "_index.parquet")
    sido_rows = df[df.level == "sido"]
    assert (sido_rows["code"] == "11").all()
    sgg_rows = df[df.level == "sgg"]
    assert (sgg_rows["code"] == "11110").all()
    emd_rows = df[df.level == "emd"]
    assert set(emd_rows["code"]) == {"1111053000", "1111054000"}


def test_build_empty_dir_raises(tmp_path):
    with pytest.raises(RuntimeError):
        build_index(tmp_path)


def test_build_custom_output(mini_repo, tmp_path):
    out_path = tmp_path / "sub" / "custom_index.parquet"
    result = build_index(mini_repo, output=out_path)
    assert result == out_path
    assert out_path.exists()
