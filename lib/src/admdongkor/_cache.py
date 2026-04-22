"""캐시 디렉토리 해석 + 다운로드."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import requests

BASE_URL = "https://raw.githubusercontent.com/vuski/admdongkor/master/parquet"
_APP_NAME = "admdongkor"


def cache_dir() -> Path:
    """OS 관례 + 환경변수 override 를 따르는 캐시 디렉토리.

    우선순위:
      1. `ADMDONGKOR_CACHE_DIR` 환경변수
      2. Windows: `%LOCALAPPDATA%\\admdongkor\\`
      3. macOS/Linux: `$XDG_CACHE_HOME/admdongkor/` 또는 `~/.cache/admdongkor/`
    """
    override = os.environ.get("ADMDONGKOR_CACHE_DIR")
    if override:
        return Path(override)

    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA")
        if base:
            return Path(base) / _APP_NAME
        return Path.home() / "AppData" / "Local" / _APP_NAME

    xdg = os.environ.get("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg) / _APP_NAME
    return Path.home() / ".cache" / _APP_NAME


def cached_path(filename: str) -> Path:
    """캐시 디렉토리 내의 파일 경로 (파일 존재 여부 보장 안 함)."""
    return cache_dir() / filename


def download_if_needed(filename: str, *, force_refresh: bool = False) -> Path:
    """캐시에 없으면 raw URL 에서 받아 저장. 있으면 그대로 반환.

    Args:
        filename: 예) "emd_20250401.parquet", "_index.parquet"
        force_refresh: True 면 캐시 무시하고 재다운로드
    """
    dst = cached_path(filename)
    if dst.exists() and not force_refresh:
        return dst

    url = f"{BASE_URL}/{filename}"
    dst.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        delete=False, dir=dst.parent, prefix=f".{filename}.", suffix=".part"
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
        shutil.move(str(tmp_path), str(dst))
    except BaseException:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise

    return dst
