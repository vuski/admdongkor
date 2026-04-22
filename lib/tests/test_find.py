"""find() 동작 테스트. 실제 parquet 을 만들지 않고 인덱스 DataFrame 을 주입."""

import unicodedata

import pandas as pd
import pytest

from admdongkor import _index
from admdongkor.api import find


def _nfc(s):
    return unicodedata.normalize("NFC", s) if isinstance(s, str) else s


def _fullpath(sidonm, sggnm, name):
    parts = []
    for x in (sidonm, sggnm, name):
        if isinstance(x, str):
            parts.append(x)
    joined = "".join(parts)
    return "".join(joined.split()).casefold()


@pytest.fixture
def fake_index(monkeypatch):
    """6개 행의 미니 인덱스. 서울 종로구 / 수원시권선구 / 제주도 서귀포시 종로동 등을 포함."""
    rows = [
        # (version_key, level, code, code7, code8, name, sggcd, sggnm, sidocd, sidonm)
        ("19751231", "sido", pd.NA,       pd.NA,      pd.NA,       "서울특별시",     pd.NA,    pd.NA,            pd.NA, pd.NA),
        ("19751231", "sgg",  pd.NA,       pd.NA,      pd.NA,       "종로구",          pd.NA,    pd.NA,            pd.NA, "서울특별시"),
        ("19751231", "emd",  pd.NA,       "1101053",  pd.NA,       "사직동",          "11110",  "종로구",          "11",  "서울특별시"),
        ("20250401", "sido", "11",        pd.NA,      pd.NA,       "서울특별시",     pd.NA,    pd.NA,            pd.NA, pd.NA),
        ("20250401", "sgg",  "11110",     pd.NA,      pd.NA,       "종로구",          pd.NA,    pd.NA,            "11",  "서울특별시"),
        ("20250401", "emd",  "1111053000","1101053",  "11010530",  "사직동",          "11110",  "종로구",          "11",  "서울특별시"),
        ("20250401", "sgg",  "41113",     pd.NA,      pd.NA,       "수원시권선구",    pd.NA,    pd.NA,            "41",  "경기도"),
        ("20250401", "emd",  "4111356000","3101251",  pd.NA,       "권선동",          "41113",  "수원시권선구",    "41",  "경기도"),
        # 영문 혼합 케이스
        ("20250401", "sgg",  "99999",     pd.NA,      pd.NA,       "Jongno-gu",       pd.NA,    pd.NA,            "99",  "Test-do"),
    ]
    cols = ["version_key", "level", "code", "code7", "code8", "name",
            "sggcd", "sggnm", "sidocd", "sidonm"]
    df = pd.DataFrame(rows, columns=cols)
    for col in cols[2:]:  # 전부 string dtype
        df[col] = df[col].astype("string").map(_nfc)
    df["_fullpath"] = [
        _fullpath(r.sidonm, r.sggnm, r.name) for r in df.itertuples()
    ]

    _index.clear_index_cache()
    monkeypatch.setattr(_index, "_load_index", lambda: df)
    yield df


# ────────── 단일 토큰 ──────────

def test_single_token_substring(fake_index):
    df = find("종로")
    assert len(df) > 0
    # 종로구(sgg) 2개 + 사직동 2개 (sggnm=종로구 포함) + Jongno-gu 1개
    assert set(df.level.unique()) >= {"sgg", "emd"}


def test_exact(fake_index):
    df = find("종로구", exact=True)
    assert set(df.name.unique()) == {"종로구"}
    assert len(df) == 2


def test_exact_no_match(fake_index):
    df = find("종로", exact=True)
    assert len(df) == 0


def test_single_token_case_insensitive(fake_index):
    df = find("jongno")
    assert "Jongno-gu" in df.name.values


# ────────── 2 토큰 (자동 sgg) ──────────

def test_two_tokens_auto_sgg(fake_index):
    df = find("서울특별시 종로구")
    assert (df.level == "sgg").all()
    assert (df.name == "종로구").all()
    assert len(df) == 2  # 19751231 + 20250401


def test_two_tokens_matches_concatenated_name(fake_index):
    # "수원시 권선구" 가 한덩어리 sggnm "수원시권선구" 와 매치
    df = find("수원시 권선구")
    assert len(df) == 1
    assert df.name.iloc[0] == "수원시권선구"
    assert df.level.iloc[0] == "sgg"


def test_two_tokens_no_emd_leak(fake_index):
    # 2토큰은 sgg 만 나와야 — 그 시군구의 읍면동 list 안 나와야 함
    df = find("서울특별시 종로구")
    assert (df.level == "sgg").all()
    assert "사직동" not in df.name.values


# ────────── 3 토큰 (자동 emd) ──────────

def test_three_tokens_auto_emd(fake_index):
    df = find("서울특별시 종로구 사직동")
    assert (df.level == "emd").all()
    assert (df.name == "사직동").all()


# ────────── level override ──────────

def test_level_override_beats_auto(fake_index):
    # 2 토큰인데 emd 명시 → 종로구 내 사직동 나와야
    df = find("서울특별시 종로구", level="emd")
    assert (df.level == "emd").all()
    assert "사직동" in df.name.values


# ────────── year 필터 ──────────

def test_year_single(fake_index):
    df = find("서울", year=[2025])
    assert df.version_key.str[:4].eq("2025").all()


def test_year_range(fake_index):
    df = find("종로", year=[2010, 2025])
    ys = df.version_key.str[:4].astype(int)
    assert ((ys >= 2010) & (ys <= 2025)).all()


# ────────── 에러 케이스 ──────────

def test_empty_name_raises(fake_index):
    with pytest.raises(ValueError):
        find("")


def test_whitespace_only_raises(fake_index):
    with pytest.raises(ValueError):
        find("   ")


def test_too_many_tokens_raises(fake_index):
    with pytest.raises(ValueError):
        find("a b c d")


def test_exact_with_whitespace_raises(fake_index):
    with pytest.raises(ValueError):
        find("서울특별시 종로구", exact=True)


def test_year_length_3_raises(fake_index):
    with pytest.raises(ValueError):
        find("종로", year=[2000, 2005, 2010])


def test_year_empty_raises(fake_index):
    with pytest.raises(ValueError):
        find("종로", year=[])


def test_bad_level_raises(fake_index):
    with pytest.raises(ValueError):
        find("종로", level="bjd")


def test_non_str_name_raises(fake_index):
    with pytest.raises(TypeError):
        find(123)


# ────────── 반환 스키마 & 정렬 ──────────

def test_returned_columns(fake_index):
    df = find("종로구", exact=True)
    assert list(df.columns) == [
        "version_key", "level",
        "sidonm", "sggnm", "name",
        "code", "code7", "code8",
        "sggcd", "sidocd",
    ]


def test_code7_code8_populated_for_emd(fake_index):
    df = find("사직동", exact=True)
    # 20250401 emd 에 code7/code8 채워져 있어야
    row = df[df.version_key == "20250401"].iloc[0]
    assert row.code7 == "1101053"
    assert row.code8 == "11010530"


def test_sgg_sido_context_for_emd(fake_index):
    df = find("사직동", exact=True)
    row = df[df.version_key == "20250401"].iloc[0]
    assert row.sggnm == "종로구"
    assert row.sidonm == "서울특별시"


def test_sort_order(fake_index):
    # 종로구 sgg 두 개: 1975 먼저, 2025 나중
    df = find("종로구", exact=True)
    assert list(df.version_key) == ["19751231", "20250401"]
