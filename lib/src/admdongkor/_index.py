"""인덱스(_index.parquet) 로드 + find() 내부 필터 로직.

인덱스는 외부 저장 (GitHub dist/data/). 로컬 캐시는 `_cache.index_dir()`.
import 시 `_cache.ensure_latest()` 가 자동으로 최신 버전을 받아둔다.
"""

from __future__ import annotations

import unicodedata
from functools import lru_cache
from typing import Literal

import pandas as pd

from . import _cache

_INDEX_FILENAME = "_index_v3.parquet"
# 출장소 코드표. 지도 경계가 없어 _index_v3 에 못 들어가므로 별도 파일.
_OFFICES_FILENAME = "_offices.parquet"
Level = Literal["sido", "sgg", "emd"]
LEVELS: tuple[Level, ...] = ("sido", "sgg", "emd")

# 사용자에게 반환할 컬럼 (내부 _fullpath 는 제외)
_PUBLIC_COLUMNS = [
    "version_key", "level",
    "sidonm", "sggnm", "name",
    "code", "code7", "code8",
    "sggcd", "sidocd",
]

# 쿼리 토큰 수 → 자동 적용 level
_AUTO_LEVEL = {1: None, 2: "sgg", 3: "emd"}

# 코드 검색이 훑는 컬럼. code = 행안부(sido 2 / sgg 5 / emd 10),
# code7 / code8 = 통계청 (emd 레벨에서만 채워짐).
_CODE_COLUMNS = ("code", "code7", "code8")

# 출장소 결과 컬럼. 지도가 없으므로 version_key 대신 created/abolished 로
# 유효 기간을 표현한다.
_OFFICE_COLUMNS = [
    "code", "name", "sggnm", "sidonm",
    "sggcd", "sidocd", "level", "created", "abolished",
]


class FindResult(pd.DataFrame):
    """`find()` 의 반환 타입. `pd.DataFrame` 이므로 pandas 기능 그대로 쓰면서
    자주 쓰는 버전 키 추출을 체이닝으로.

    Examples:
        adk.find("여주군").versions()   # ['19751231', '19801231', ...]
        adk.find("여주군").first()      # '19751231'
        adk.find("여주군").last()       # '20130701'
    """

    @property
    def _constructor(self):
        return FindResult

    def versions(self) -> list[str]:
        """매치된 고유 version_key 목록 (정렬된 순서 유지)."""
        return self["version_key"].drop_duplicates().tolist()

    def first(self) -> str | None:
        """가장 이른 version_key. 결과가 없으면 `None`."""
        v = self.versions()
        return v[0] if v else None

    def last(self) -> str | None:
        """가장 늦은 version_key. 결과가 없으면 `None`."""
        v = self.versions()
        return v[-1] if v else None


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


@lru_cache(maxsize=1)
def _load_index() -> pd.DataFrame:
    """캐시된 _index.parquet 를 로드. LRU 로 프로세스 내 재사용."""
    path = _cache.index_data_path(_INDEX_FILENAME)
    return pd.read_parquet(path)


@lru_cache(maxsize=1)
def _load_offices() -> pd.DataFrame:
    """출장소 코드표. **선택적 파일** — 없으면 빈 DataFrame.

    `_offices.parquet` 은 데이터 버전 2026.08.02 부터 추가됐다. 그 이전 캐시를
    들고 있는 사용자(자동 업데이트를 껐거나 `ADMDONGKOR_DATA_DIR` 로 옛 사본을
    고정한 경우)에게는 파일이 없다. 이때 예외를 던지면 출장소라는 부가 기능
    때문에 세션이 깨지므로, 조용히 빈 결과로 처리한다.

    `index_data_path()` 는 없는 파일에 FileNotFoundError 를 던지므로 쓰지 않고
    경로를 직접 만든다.
    """
    path = _cache.index_dir() / _OFFICES_FILENAME
    if not path.exists():
        return pd.DataFrame(columns=[*_OFFICE_COLUMNS, "_fullpath"], dtype="string")
    return pd.read_parquet(path)


def clear_index_cache() -> None:
    """테스트·개발용. 메모리 LRU 를 비운다 (디스크 캐시는 유지)."""
    _load_index.cache_clear()
    _load_offices.cache_clear()


def _is_code_query(q: str) -> bool:
    """숫자로만 이루어진 단일 토큰이면 코드 쿼리로 본다.

    행정구역명에 숫자만으로 된 것은 없으므로 이름 검색과 충돌하지 않는다
    ('미아6.7동' 처럼 숫자가 섞인 이름은 있지만 숫자 *만* 인 이름은 없다).
    """
    return q.isdigit()


def _find_by_code(df: pd.DataFrame, code: str) -> "pd.Series[bool]":
    """코드 prefix 매칭 마스크. `code` / `code7` / `code8` 중 하나라도 걸리면 매치.

    자릿수를 정확히 맞추지 않아도 되도록 **prefix** 로 본다.
    '11' → 시도 11 + 그 하위 시군구 11xxx + 읍면동 11xxxxxxxx 전부.
    level / year 필터는 호출부에서 이름 검색과 공통으로 적용한다.
    """
    mask = pd.Series(False, index=df.index)
    for col in _CODE_COLUMNS:
        if col not in df.columns:
            continue
        mask |= df[col].astype("string").str.startswith(code, na=False)
    return mask


def find_offices(
    query: str,
    exact: bool = False,
    by: str | None = None,
) -> pd.DataFrame:
    """출장소 검색. 코드(prefix) 또는 이름 substring.

    출장소는 **경계 지도가 없다** — 코드 체계에만 존재. 그래서 `version_key` 가
    없고 `created` / `abolished` (YYYYMMDD 문자열) 로 유효 기간을 나타낸다.
    `abolished` 가 NA 면 현존.
    """
    if not isinstance(query, str):
        raise TypeError(f"query must be str, got {type(query).__name__}")
    tokens = _nfc(query).strip().split()
    if not tokens:
        return pd.DataFrame(columns=_OFFICE_COLUMNS)
    q = "".join(tokens)

    df = _load_offices()
    if df.empty:
        return pd.DataFrame(columns=_OFFICE_COLUMNS)

    code_mode = _is_code_query(q) if by is None else (by == "code")
    if code_mode:
        codes = df["code"].astype("string")
        mask = (codes == q) if exact else codes.str.startswith(q, na=False)
    else:
        needle = q.casefold()
        if exact:
            mask = df["name"].astype("string").str.casefold() == needle
        else:
            mask = df["_fullpath"].str.contains(needle, regex=False, na=False)

    out = df.loc[mask].copy()
    out = out.sort_values(["code", "created"], kind="stable").reset_index(drop=True)
    return out[_OFFICE_COLUMNS]


def find(
    name: str,
    level: str | None = None,
    exact: bool = False,
    year: list[int] | None = None,
    by: str | None = None,
) -> pd.DataFrame:
    if not isinstance(name, str):
        raise TypeError(f"name must be str, got {type(name).__name__}")
    if level is not None and level not in LEVELS:
        raise ValueError(f"level must be one of {LEVELS} or None, got {level!r}")
    if by is not None and by not in ("name", "code"):
        raise ValueError(f"by must be 'name', 'code', or None, got {by!r}")
    if year is not None:
        if not isinstance(year, list) or not all(isinstance(y, int) for y in year):
            raise TypeError("year must be list[int]")
        if len(year) not in (1, 2):
            raise ValueError(
                f"year must have length 1 (single year) or 2 (inclusive range), got {len(year)}"
            )

    tokens = _nfc(name).strip().split()
    if len(tokens) == 0:
        raise ValueError("name cannot be empty")

    # 코드 검색 여부 판정. by= 가 명시되면 그 쪽을 강제.
    query = "".join(tokens)
    if by == "code":
        if not _is_code_query(query):
            raise ValueError(
                f"by='code' requires a digits-only query, got {name!r}"
            )
        code_mode = True
    elif by == "name":
        code_mode = False
    else:
        code_mode = _is_code_query(query)

    df = _load_index()

    if code_mode:
        if exact:
            # 코드는 prefix 매칭이 기본. exact 는 자릿수 완전일치를 뜻한다.
            mask = pd.Series(False, index=df.index)
            for col in _CODE_COLUMNS:
                if col in df.columns:
                    mask |= df[col].astype("string") == query
        else:
            mask = _find_by_code(df, query)
        effective_level = level
    else:
        if len(tokens) > 3:
            raise ValueError(
                f"name must have 1-3 whitespace-separated tokens "
                f"(sido, sgg, emd), got {len(tokens)}"
            )

        multi_token = len(tokens) >= 2
        if exact and multi_token:
            raise ValueError(
                "exact=True requires a single-token name (no whitespace). "
                "Use level= to narrow scope instead."
            )

        # 자동 level: 사용자가 명시 안 했으면 토큰 수 기반
        effective_level = level if level is not None else _AUTO_LEVEL[len(tokens)]

        # 공백 제거한 쿼리 — 인덱스의 _fullpath 는 이미 공백제거 + casefold 된 상태
        needle = query.casefold()

        if exact:
            # exact 는 단일 토큰 전용. name 컬럼 단독 완전일치.
            names_nfc = df["name"].astype(str).map(_nfc)
            mask = names_nfc.str.casefold() == needle
        else:
            mask = df["_fullpath"].str.contains(needle, regex=False, na=False)

    if effective_level is not None:
        mask &= df["level"] == effective_level

    if year is not None:
        ys = df["version_key"].str[:4].astype(int)
        if len(year) == 1:
            mask &= ys == year[0]
        else:
            lo, hi = sorted(year)
            mask &= (ys >= lo) & (ys <= hi)

    out = df.loc[mask].copy()

    level_order = pd.Categorical(out["level"], categories=list(LEVELS), ordered=True)
    out = out.assign(_lvl=level_order)
    out = out.sort_values(
        ["version_key", "_lvl", "code"], kind="stable"
    ).drop(columns="_lvl").reset_index(drop=True)

    # 내부 _fullpath 감추고 공개 컬럼만. FindResult 로 감싸 체이닝 메서드 제공.
    return FindResult(out[_PUBLIC_COLUMNS])
