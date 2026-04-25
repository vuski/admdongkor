"""캐시 디렉토리 해석 + 다운로드.

두 종류의 다운로드:
1. geoparquet (emd/sgg/sido_<key>.parquet) — `download_if_needed()`, on-demand
2. 인덱스 (_index, timeline_v3_*, shape_pairs_v3_*) — `ensure_latest()`,
   import 시 자동. manifest.json 기반 sha256 검증.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://raw.githubusercontent.com/vuski/admdongkor/master/parquet"
INDEX_BASE_URL = "https://raw.githubusercontent.com/vuski/admdongkor/master/dist/data"
_APP_NAME = "admdongkor"

INDEX_FILES: tuple[str, ...] = (
    "_index_v3.parquet",
    "timeline_v3_sido.parquet",
    "timeline_v3_sgg.parquet",
    "timeline_v3_emd.parquet",
    "shape_pairs_v3_sido.parquet",
    "shape_pairs_v3_sgg.parquet",
    "shape_pairs_v3_emd.parquet",
)
# 구 파일 `_index.parquet` 은 `_index_v3.parquet` 과 동일 내용으로 영구 유지되지만,
# lib 0.6.0+ 는 canonical 한 `_v3` 이름만 사용한다.


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


def index_dir() -> Path:
    """인덱스 파일 전용 디렉토리.

    `ADMDONGKOR_DATA_DIR` 이 지정돼 있으면 그 경로를 그대로 반환 (로컬 고정 모드).
    아니면 `<cache_dir>/index/`.
    """
    override = os.environ.get("ADMDONGKOR_DATA_DIR")
    if override:
        return Path(override)
    return cache_dir() / "index"


def index_data_path(filename: str) -> Path:
    """인덱스 파일 경로. 존재하지 않으면 `FileNotFoundError`.

    `_index.py` / `_match.py` / `_compare.py` 가 parquet 를 열 때 사용. 경로는
    캐시 디렉토리(`<cache_dir>/index/`) 또는 `ADMDONGKOR_DATA_DIR` override.
    """
    p = index_dir() / filename
    if not p.exists():
        raise FileNotFoundError(
            f"index file not cached: {filename}. "
            f"Cache dir: {index_dir()}. "
            "Check network or set ADMDONGKOR_DATA_DIR to a local copy."
        )
    return p


def cached_path(filename: str, *, subdir: str | None = None) -> Path:
    """캐시 디렉토리 내의 파일 경로 (파일 존재 여부 보장 안 함).

    subdir 을 주면 `<cache_dir>/<subdir>/<filename>` 으로 해석. geoparquet 전용.
    """
    base = cache_dir()
    if subdir:
        return base / subdir / filename
    return base / filename


def download_if_needed(
    filename: str,
    *,
    subdir: str | None = None,
    force_refresh: bool = False,
) -> Path:
    """캐시에 없으면 raw URL 에서 받아 저장. 있으면 그대로 반환. geoparquet 전용.

    Args:
        filename: 예) "emd_20250401.parquet"
        subdir: 원격 `BASE_URL` 하위 경로. 예) `"simplified"`
        force_refresh: True 면 캐시 무시하고 재다운로드
    """
    dst = cached_path(filename, subdir=subdir)
    if dst.exists() and not force_refresh:
        return dst

    url = f"{BASE_URL}/{subdir}/{filename}" if subdir else f"{BASE_URL}/{filename}"
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


# ─────────────────────────────────────────────────────────────
# 인덱스 전용 로직 (manifest + sha256)
# ─────────────────────────────────────────────────────────────


def _manifest_url() -> str:
    return f"{INDEX_BASE_URL}/manifest.json"


def _current_json_path() -> Path:
    return index_dir() / "current.json"


def _manifest_cache_path() -> Path:
    return index_dir() / "manifest.json"


def _read_json_safe(p: Path) -> dict[str, Any] | None:
    try:
        with p.open("rb") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def fetch_manifest(timeout: float = 10.0) -> dict[str, Any]:
    """원격 manifest.json 을 받아 파싱. 네트워크 에러는 그대로 전파."""
    r = requests.get(_manifest_url(), timeout=timeout)
    r.raise_for_status()
    return r.json()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_with_sha(url: str, sha256: str, dest: Path, timeout: float = 60.0) -> None:
    """sha256 검증하며 다운로드. `.part` 임시파일 → atomic rename."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        delete=False, dir=dest.parent, prefix=f".{dest.name}.", suffix=".part"
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
        actual = _sha256_file(tmp_path)
        if actual != sha256:
            raise RuntimeError(
                f"sha256 mismatch for {dest.name}: "
                f"expected {sha256[:12]}..., got {actual[:12]}..."
            )
        os.replace(tmp_path, dest)
    except BaseException:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


def data_version() -> str | None:
    """현재 캐시에 반영된 data_version. 캐시 없으면 None.

    우선순위:
      1. index_dir()/current.json  (auto-update 가 갱신)
      2. index_dir()/manifest.json  (ADMDONGKOR_DATA_DIR 로컬 고정 모드에서 유용)
    """
    cur = _read_json_safe(_current_json_path())
    if cur is not None:
        v = cur.get("data_version")
        if isinstance(v, str):
            return v
    m = _read_json_safe(_manifest_cache_path())
    if m is not None:
        v = m.get("data_version")
        if isinstance(v, str):
            return v
    return None


def changelog() -> list[dict[str, str]]:
    """data 수정 이력. manifest 의 `history` 필드.

    반환 리스트의 각 요소: `{"version": "2026.04.25", "changes": "..."}`.
    캐시된 manifest 가 없으면 빈 리스트.
    """
    m = _read_json_safe(_manifest_cache_path())
    if m is None:
        return []
    h = m.get("history", [])
    return h if isinstance(h, list) else []


def ensure_latest(quiet: bool = True, force: bool = False) -> str | None:
    """원격 manifest 와 비교해 필요시 인덱스를 갱신.

    Args:
        quiet: True (기본) 이면 실제 다운로드 발생 시에만 stderr 에 한 줄 출력.
               False 면 더 자세한 진행 로그.
        force: True 면 sha 무관 전체 재다운로드.

    Returns:
        현재 반영된 data_version (성공 시). 네트워크 실패 + 캐시 없음 → None.

    Never raises — 네트워크 실패는 조용히 넘어감. 첫 find()/match_adm() 호출에서
    FileNotFoundError 로 의미있는 에러가 뜬다.
    """
    # ADMDONGKOR_DATA_DIR override 는 네트워크 완전 우회 (로컬 고정 모드)
    if os.environ.get("ADMDONGKOR_DATA_DIR"):
        return data_version()

    idx = index_dir()
    idx.mkdir(parents=True, exist_ok=True)

    try:
        remote = fetch_manifest()
    except Exception as e:
        if not quiet:
            print(f"[admdongkor] manifest fetch failed: {e}", file=sys.stderr)
        return data_version()  # 캐시 있으면 그거, 없으면 None

    remote_version = remote.get("data_version")
    if not isinstance(remote_version, str):
        if not quiet:
            print("[admdongkor] invalid manifest (no data_version)", file=sys.stderr)
        return data_version()

    files = remote.get("files", {})
    if not isinstance(files, dict):
        if not quiet:
            print("[admdongkor] invalid manifest (no files)", file=sys.stderr)
        return data_version()

    # 현재 캐시 상태와 비교
    cur_version = data_version()
    need_any = force or (cur_version != remote_version)

    # 파일별 sha 검증 — 버전 같아도 파일 하나라도 깨졌으면 재받음
    to_download: list[tuple[str, str]] = []  # (filename, sha256)
    for fname in INDEX_FILES:
        entry = files.get(fname)
        if not isinstance(entry, dict):
            # manifest 에 없음 — schema mismatch. 일단 skip.
            continue
        sha = entry.get("sha256")
        if not isinstance(sha, str):
            continue
        dest = idx / fname
        if force or not dest.exists():
            to_download.append((fname, sha))
            continue
        # 버전이 올라갔으면 파일별 sha 재확인
        if need_any:
            try:
                if _sha256_file(dest) != sha:
                    to_download.append((fname, sha))
            except OSError:
                to_download.append((fname, sha))

    if not to_download and cur_version == remote_version:
        # 이미 최신. manifest 캐시만 갱신 (changelog 최신화)
        try:
            _manifest_cache_path().write_text(
                json.dumps(remote, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except OSError:
            pass
        return cur_version

    # 다운로드 실행
    if not quiet or to_download:
        prev = cur_version or "(none)"
        print(
            f"[admdongkor] updating index: {prev} -> {remote_version} "
            f"({len(to_download)} file(s))",
            file=sys.stderr,
        )

    for fname, sha in to_download:
        url = f"{INDEX_BASE_URL}/{fname}"
        dest = idx / fname
        try:
            _download_with_sha(url, sha, dest)
        except Exception as e:
            if not quiet:
                print(f"[admdongkor] download failed for {fname}: {e}", file=sys.stderr)
            # 하나 실패해도 다른 파일은 계속 시도. current.json 은 갱신 안 함.
            return data_version()

    # 모두 성공 → manifest + current.json 갱신
    try:
        _manifest_cache_path().write_text(
            json.dumps(remote, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        _current_json_path().write_text(
            json.dumps({"data_version": remote_version}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as e:
        if not quiet:
            print(f"[admdongkor] failed to write current.json: {e}", file=sys.stderr)

    return remote_version
