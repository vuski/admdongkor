"""_index.parquet 재생성 함수 + CLI.

사용:
    python -m admdongkor.build_index
    python -m admdongkor.build_index --data-root Z:/Github/admdongkor/parquet
    python -m admdongkor.build_index --output Z:/tmp/_index.parquet --verbose
"""

from __future__ import annotations

import argparse
import sys
import time
import unicodedata
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow.parquet as pq

from ._versions import VERSIONS

LEVELS: tuple[str, ...] = ("sido", "sgg", "emd")

# 각 레벨의 parquet 에서 읽을 컬럼
_READ_COLS = {
    "sido": ["sidocd", "sidonm"],
    "sgg": ["sggcd", "sggnm", "sidocd", "sidonm"],
    "emd": ["emd7", "emd8", "emdcd", "emdnm", "sggcd", "sggnm", "sidocd", "sidonm"],
}

# 최종 인덱스 스키마 (고정 컬럼 순서)
_INDEX_COLUMNS = [
    "version_key", "level",
    "code", "code7", "code8",
    "name",
    "sggcd", "sggnm",
    "sidocd", "sidonm",
    "_fullpath",  # 검색용 (sidonm+sggnm+name 공백제거 lowercased)
]


def _nfc(s: object) -> object:
    if isinstance(s, str):
        return unicodedata.normalize("NFC", s)
    return s


def _read_level_parquet(path: Path, level: str) -> pd.DataFrame:
    """레벨별 parquet 에서 필요한 컬럼만 읽어 통일 스키마 DataFrame 반환.

    반환 컬럼(모두 `string` dtype): version_key, level, code, code7, code8,
    name, sggcd, sggnm, sidocd, sidonm.  현재 레벨에 없는 컬럼은 `<NA>`.
    """
    key = path.stem.split("_", 1)[1]
    cols = _READ_COLS[level]
    raw = pq.read_table(path, columns=cols).to_pandas()

    # 레벨별 매핑
    if level == "emd":
        out = pd.DataFrame({
            "code": raw["emdcd"],
            "code7": raw["emd7"],
            "code8": raw["emd8"],
            "name": raw["emdnm"],
            "sggcd": raw["sggcd"],
            "sggnm": raw["sggnm"],
            "sidocd": raw["sidocd"],
            "sidonm": raw["sidonm"],
        })
    elif level == "sgg":
        out = pd.DataFrame({
            "code": raw["sggcd"],
            "code7": pd.NA,
            "code8": pd.NA,
            "name": raw["sggnm"],
            "sggcd": pd.NA,
            "sggnm": pd.NA,
            "sidocd": raw["sidocd"],
            "sidonm": raw["sidonm"],
        })
    else:  # sido
        out = pd.DataFrame({
            "code": raw["sidocd"],
            "code7": pd.NA,
            "code8": pd.NA,
            "name": raw["sidonm"],
            "sggcd": pd.NA,
            "sggnm": pd.NA,
            "sidocd": pd.NA,
            "sidonm": pd.NA,
        })

    for col in ["code", "code7", "code8", "name", "sggcd", "sggnm", "sidocd", "sidonm"]:
        out[col] = out[col].astype("string")

    out.insert(0, "version_key", key)
    out.insert(1, "level", level)
    return out


def _iter_level_files(data_root: Path, levels: Iterable[str]) -> Iterable[tuple[str, Path]]:
    for level in levels:
        for key in VERSIONS:
            p = data_root / f"{level}_{key}.parquet"
            if p.exists():
                yield level, p


def _compute_fullpath(df: pd.DataFrame) -> pd.Series:
    """검색용 fullpath 컬럼 계산.

    각 행에 대해 sidonm + sggnm + name 을 이어 붙인 뒤 공백 제거, lowercase.
    NA 는 빈 문자열로 처리.
    """
    def _strip(col: str) -> pd.Series:
        s = df[col].astype("string").fillna("").map(_nfc)
        return s

    combined = _strip("sidonm") + _strip("sggnm") + _strip("name")
    # 공백·탭·개행 전부 제거 + lowercase
    return combined.str.replace(r"\s+", "", regex=True).str.casefold()


def build_index(
    data_root: Path | str,
    output: Path | str | None = None,
    *,
    levels: Iterable[str] = LEVELS,
    verbose: bool = False,
) -> Path:
    """Build `_index.parquet` from `{level}_{key}.parquet` files under data_root.

    Args:
        data_root: Directory containing `emd_*.parquet`, `sgg_*.parquet`, `sido_*.parquet`.
        output: Output path. Defaults to `data_root / "_index.parquet"`.
        levels: Subset of levels to include.
        verbose: Print progress.

    Returns:
        The output path.
    """
    data_root = Path(data_root)
    if not data_root.is_dir():
        raise NotADirectoryError(f"data_root does not exist: {data_root}")
    out_path = Path(output) if output else data_root / "_index.parquet"

    frames: list[pd.DataFrame] = []
    t0 = time.time()
    n_files = 0
    for level, path in _iter_level_files(data_root, levels):
        if verbose:
            print(f"  reading {path.name}", flush=True)
        frames.append(_read_level_parquet(path, level))
        n_files += 1

    if not frames:
        raise RuntimeError(
            f"no parquet files found under {data_root}. "
            f"Expected files like 'emd_20250401.parquet'."
        )

    df = pd.concat(frames, ignore_index=True)

    # NFC 정규화 (이름 관련 컬럼 전부)
    for col in ["name", "sggnm", "sidonm"]:
        df[col] = df[col].map(_nfc).astype("string")

    # 검색용 fullpath
    df["_fullpath"] = _compute_fullpath(df)

    level_order = pd.Categorical(df["level"], categories=list(LEVELS), ordered=True)
    df = (
        df.assign(_lvl=level_order)
        .sort_values(["version_key", "_lvl", "code"], kind="stable")
        .drop(columns="_lvl")
        .reset_index(drop=True)
    )

    df = df[_INDEX_COLUMNS]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    if verbose:
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(
            f"wrote {out_path}: {len(df):,} rows, {size_mb:.2f} MB "
            f"from {n_files} files in {time.time() - t0:.1f}s",
            flush=True,
        )
    return out_path


def _resolve_default_data_root() -> Path:
    """CLI 기본값. 현재 디렉토리가 parquet 이면 `.`, 아니면 `./parquet`."""
    cwd = Path.cwd()
    if cwd.name == "parquet":
        return cwd
    pq_sub = cwd / "parquet"
    if pq_sub.is_dir():
        return pq_sub
    return cwd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m admdongkor.build_index",
        description="Rebuild _index.parquet from {level}_{key}.parquet files.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Directory containing the level parquets. Default: ./parquet (or . if already in parquet).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path. Default: <data-root>/_index.parquet.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    data_root = args.data_root or _resolve_default_data_root()
    build_index(data_root, output=args.output, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
