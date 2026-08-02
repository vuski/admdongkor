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

    _index._load_index.cache_clear()
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


# ────────── FindResult 체이닝 메서드 ──────────

def test_versions_method(fake_index):
    vs = find("종로구", exact=True).versions()
    assert vs == ["19751231", "20250401"]


def test_versions_dedupe(fake_index):
    # 같은 버전 안에 여러 매치 (emd 사직동 + sgg 종로구) 가 있어도 버전키는 유니크
    vs = find("종로").versions()
    assert len(vs) == len(set(vs))


def test_first_method(fake_index):
    assert find("종로구", exact=True).first() == "19751231"


def test_last_method(fake_index):
    assert find("종로구", exact=True).last() == "20250401"


def test_first_last_empty(fake_index):
    empty = find("없는이름")
    assert len(empty) == 0
    assert empty.first() is None
    assert empty.last() is None


def test_find_returns_findresult_subclass(fake_index):
    from admdongkor._index import FindResult
    import pandas as pd
    df = find("종로구", exact=True)
    assert isinstance(df, FindResult)
    assert isinstance(df, pd.DataFrame)


def test_findresult_preserves_type_after_filter(fake_index):
    from admdongkor._index import FindResult
    df = find("종로")
    # boolean indexing 결과도 FindResult 유지
    subset = df[df.level == "sgg"]
    assert isinstance(subset, FindResult)
    assert subset.first() is not None


# ────────── 코드 검색 ──────────

def test_code_auto_detect_emd(fake_index):
    """숫자만인 쿼리는 자동으로 코드 검색."""
    df = find("1111053000")
    assert len(df) == 1
    assert df.iloc[0]["name"] == "사직동"
    assert df.iloc[0]["level"] == "emd"


def test_code_prefix_matches_descendants(fake_index):
    """'11110' → sgg 11110 자체 + 하위 emd 1111053000 둘 다."""
    df = find("11110")
    levels = set(df["level"])
    assert "sgg" in levels and "emd" in levels
    # sggcd=11110 인 emd 도 code prefix 로 걸린다
    assert "사직동" in set(df["name"])


def test_code_prefix_sido(fake_index):
    """'11' → 시도 11 + 시군구 11xxx + 읍면동 11xxxxxxxx."""
    df = find("11")
    assert set(df["level"]) == {"sido", "sgg", "emd"}
    # 경기도(41) 쪽은 안 걸려야 한다
    assert "권선동" not in set(df["name"])
    assert "수원시권선구" not in set(df["name"])


def test_code_exact_requires_full_width(fake_index):
    """exact=True 면 prefix 가 아니라 자릿수 완전일치."""
    df = find("11110", exact=True)
    assert set(df["level"]) == {"sgg"}
    assert set(df["name"]) == {"종로구"}


def test_code_matches_code7(fake_index):
    """통계청 7자리도 매칭 대상."""
    df = find("1101053", exact=True)
    assert len(df) > 0
    assert set(df["name"]) == {"사직동"}


def test_code_matches_code8(fake_index):
    """통계청 8자리도 매칭 대상."""
    df = find("11010530", exact=True)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "사직동"


def test_code_with_level_filter(fake_index):
    df = find("11110", level="emd")
    assert set(df["level"]) == {"emd"}


def test_code_with_year_filter(fake_index):
    df = find("11110", year=[2025])
    assert len(df) > 0
    assert all(v.startswith("2025") for v in df["version_key"])


def test_by_name_forces_name_search(fake_index):
    """by='name' 이면 숫자 쿼리도 이름으로 찾는다 (결과 없음)."""
    assert len(find("11110", by="name")) == 0


def test_by_code_rejects_non_digits(fake_index):
    with pytest.raises(ValueError, match="digits-only"):
        find("종로구", by="code")


def test_by_invalid_value(fake_index):
    with pytest.raises(ValueError, match="by must be"):
        find("11110", by="codes")


def test_name_search_unaffected_by_code_feature(fake_index):
    """이름 검색 회귀 없음."""
    df = find("종로구", exact=True)
    assert len(df) > 0
    assert set(df["name"]) == {"종로구"}


def test_code_no_match_returns_empty(fake_index):
    df = find("99999999")
    assert len(df) == 0
    assert df.first() is None


# ────────── 출장소 ──────────

@pytest.fixture
def fake_offices(monkeypatch):
    """출장소 4행. 읍면동급 2 + 시군구급 2, 현존/말소 섞음."""
    rows = [
        # code, name, sggnm, sidonm, sggcd, sidocd, level, created, abolished
        ("2811400000", "중구영종출장소", None, "인천광역시", "28114", "28", "sgg", "20060712", None),
        ("2811500000", "중구영종출장소", None, "인천광역시", "28115", "28", "sgg", "19950101", "20031016"),
        ("2920083000", "임곡출장소", "광산구", "광주광역시", "29200", "29", "emd", "19950101", "19981015"),
        ("2871042500", "서도면볼음출장소", "강화군", "인천광역시", "28710", "28", "emd", "19950301", None),
    ]
    cols = ["code", "name", "sggnm", "sidonm", "sggcd", "sidocd",
            "level", "created", "abolished"]
    df = pd.DataFrame(rows, columns=cols)
    for c in cols:
        df[c] = df[c].astype("string")
    df["_fullpath"] = (
        df["sidonm"].fillna("") + df["sggnm"].fillna("") + df["name"].fillna("")
    ).str.casefold()

    # clear_index_cache() 는 LRU 를 지우는데, fake_index 가 먼저 적용됐다면
    # _load_index 가 이미 lambda 라 .cache_clear() 가 없다. 실제 LRU 만 직접 비운다.
    _index._load_offices.cache_clear()
    monkeypatch.setattr(_index, "_load_offices", lambda: df)
    yield df


def test_offices_by_full_code(fake_offices):
    from admdongkor.api import find_offices
    d = find_offices("2920083000")
    assert len(d) == 1
    assert d.iloc[0]["name"] == "임곡출장소"
    assert d.iloc[0]["abolished"] == "19981015"


def test_offices_code_prefix(fake_offices):
    from admdongkor.api import find_offices
    d = find_offices("28")
    assert len(d) == 3  # 인천 3건
    assert all(c.startswith("28") for c in d["code"])


def test_offices_by_name(fake_offices):
    from admdongkor.api import find_offices
    d = find_offices("영종")
    assert len(d) == 2
    assert set(d["code"]) == {"2811400000", "2811500000"}


def test_offices_alive_vs_abolished(fake_offices):
    from admdongkor.api import find_offices
    d = find_offices("28")
    alive = d[d["abolished"].isna()]
    assert set(alive["code"]) == {"2811400000", "2871042500"}


def test_offices_exact_code(fake_offices):
    from admdongkor.api import find_offices
    assert len(find_offices("28", exact=True)) == 0
    assert len(find_offices("2811400000", exact=True)) == 1


def test_offices_no_match(fake_offices):
    from admdongkor.api import find_offices
    assert len(find_offices("9999999999")) == 0


def test_offices_excluded_from_find(fake_index, fake_offices):
    """find() 는 지도 기반이므로 출장소를 절대 반환하지 않는다."""
    assert len(find("2920083000")) == 0


def test_offices_missing_file_returns_empty(monkeypatch, tmp_path):
    """구버전 캐시(= _offices.parquet 없음) 에서도 크래시하지 않는다.

    출장소는 부가 기능이므로, 파일이 없다고 세션을 깨뜨리면 안 된다.
    """
    from admdongkor import _cache
    from admdongkor.api import find_offices

    _index._load_offices.cache_clear()
    monkeypatch.setattr(_cache, "index_dir", lambda: tmp_path)
    d = find_offices("2920083000")
    assert len(d) == 0
    assert list(d.columns) == _index._OFFICE_COLUMNS
    _index._load_offices.cache_clear()
