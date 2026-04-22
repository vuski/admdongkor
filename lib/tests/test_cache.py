"""캐시 경로 해석."""

import os
from pathlib import Path

import pytest

from admdongkor import _cache


def test_override_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMDONGKOR_CACHE_DIR", str(tmp_path))
    assert _cache.cache_dir() == tmp_path


def test_windows_localappdata(monkeypatch, tmp_path):
    monkeypatch.delenv("ADMDONGKOR_CACHE_DIR", raising=False)
    monkeypatch.setattr(_cache.sys, "platform", "win32")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    assert _cache.cache_dir() == tmp_path / "admdongkor"


def test_unix_xdg(monkeypatch, tmp_path):
    monkeypatch.delenv("ADMDONGKOR_CACHE_DIR", raising=False)
    monkeypatch.setattr(_cache.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert _cache.cache_dir() == tmp_path / "admdongkor"


def test_unix_fallback(monkeypatch, tmp_path):
    monkeypatch.delenv("ADMDONGKOR_CACHE_DIR", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setattr(_cache.sys, "platform", "linux")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    assert _cache.cache_dir() == tmp_path / ".cache" / "admdongkor"


def test_cached_path(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMDONGKOR_CACHE_DIR", str(tmp_path))
    p = _cache.cached_path("emd_20250401.parquet")
    assert p == tmp_path / "emd_20250401.parquet"
