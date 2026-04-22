"""인덱스(_index.parquet) 로드 + find() 내부 필터 로직."""

from __future__ import annotations

import unicodedata
from functools import lru_cache
from typing import Literal

import pandas as pd

from ._cache import download_if_needed

_INDEX_FILENAME = "_index.parquet"
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


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


@lru_cache(maxsize=1)
def _load_index() -> pd.DataFrame:
    """_index.parquet 를 캐시에서 로드 (없으면 다운로드). LRU 로 프로세스 내 재사용."""
    path = download_if_needed(_INDEX_FILENAME)
    df = pd.read_parquet(path)
    return df


def clear_index_cache() -> None:
    """테스트·개발용. 메모리 LRU 를 비운다 (디스크 캐시는 유지)."""
    _load_index.cache_clear()


def find(
    name: str,
    level: str | None = None,
    exact: bool = False,
    year: list[int] | None = None,
) -> pd.DataFrame:
    if not isinstance(name, str):
        raise TypeError(f"name must be str, got {type(name).__name__}")
    if level is not None and level not in LEVELS:
        raise ValueError(f"level must be one of {LEVELS} or None, got {level!r}")
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

    df = _load_index()

    # 공백 제거한 쿼리 — 인덱스의 _fullpath 는 이미 공백제거 + casefold 된 상태
    needle = "".join(tokens).casefold()

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

    # 내부 _fullpath 감추고 공개 컬럼만
    return out[_PUBLIC_COLUMNS]
