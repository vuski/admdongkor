"""인덱스 parquet 을 dist/data/ 로 배포 + manifest.json 생성.

rebuild_all.py 가 lib/src/admdongkor/data/ 에 새 인덱스를 써 넣은 뒤, 이 스크립트가
그 파일들을 admdongkor repo 의 dist/data/ 로 복사하고 manifest.json 을 만든다.
사용자는 import 시 GitHub raw 에서 dist/data/manifest.json 을 받아 sha 비교 후
필요한 파일만 갱신한다.

배포 후 git commit + push 는 수동 (공개 repo 는 사용자가 직접 커밋).

옵션:
    --data-version YYYY.MM.DD   data_version 태그 (기본: 오늘 날짜)
    --changes "..."             이번 수정 내용 설명 (manifest.history 에 들어감)
    --dry-run

사용 예:
    python scripts/admin/publish_data.py --changes "1980 대구시수성구 이름 수정"
    python scripts/admin/publish_data.py --data-version 2026.04.25 --changes "..."
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LIB_DATA_DIR = REPO_ROOT / "lib" / "src" / "admdongkor" / "data"
DIST_DATA_DIR = REPO_ROOT / "dist" / "data"
CHANGELOG_FILE = DIST_DATA_DIR / "CHANGELOG.md"

INDEX_FILES = (
    "_index.parquet",
    "_index_v3.parquet",
    # 출장소 코드표. 경계 지도가 없어 _index_v3 에는 못 들어가지만 코드 검색 대상.
    # preprocessing/scripts/build_offices.py 로 생성 (행안부 KIKcd_H xlsx).
    "_offices.parquet",
    "timeline_v3_sido.parquet",
    "timeline_v3_sgg.parquet",
    "timeline_v3_emd.parquet",
    "shape_pairs_v3_sido.parquet",
    "shape_pairs_v3_sgg.parquet",
    "shape_pairs_v3_emd.parquet",
)

# `_index.parquet` 과 `_index_v3.parquet` 은 **내용 동일**. v3 가 canonical 이름이고,
# `_index.parquet` 은 구 포맷 호환용으로 같은 내용을 복제 유지. rebuild_all.py phase2 가
# `_index.parquet` 을 생성하므로, publish_data 단계에서 v3 복사본을 만든다.

SCHEMA_VERSION = "v3"
MIN_LIB_VERSION = "0.6.0"


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_existing_history() -> list[dict]:
    m = DIST_DATA_DIR / "manifest.json"
    if not m.exists():
        return []
    try:
        data = json.loads(m.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    h = data.get("history", [])
    return h if isinstance(h, list) else []


def _default_data_version() -> str:
    return _dt.date.today().strftime("%Y.%m.%d")


def _iso_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_v3_alias(dry_run: bool) -> None:
    """lib/data/_index_v3.parquet 이 없으면 _index.parquet 을 복사해서 생성.

    rebuild_all.py phase2 는 canonical 하지 않은 옛 이름 `_index.parquet` 만 쓴다.
    publish 단계에서 v3 별칭을 붙여 두 이름이 항상 같은 내용이도록 유지한다.
    """
    base = LIB_DATA_DIR / "_index.parquet"
    alias = LIB_DATA_DIR / "_index_v3.parquet"
    if not base.exists():
        return  # 아래 build_manifest 에서 FileNotFoundError 로 걸림
    if alias.exists() and _sha256(alias) == _sha256(base):
        return  # 이미 동일
    print(f"  alias {base.name} -> {alias.name}")
    if not dry_run:
        shutil.copy2(base, alias)


def _version_keys() -> list[str]:
    """`_index.parquet` 의 version_key 컬럼에서 정렬된 유니크 버전 목록을 뽑는다.

    이 목록이 버전 목록의 single source of truth. manifest 에 실어 두면
    JS `versionsAsync()` 가 런타임에 이걸 읽어 슬라이더 등에 쓴다.
    """
    import pandas as pd  # 지연 import — publish 시에만 필요

    src = LIB_DATA_DIR / "_index.parquet"
    if not src.exists():
        raise FileNotFoundError(f"missing index file: {src}")
    keys = pd.read_parquet(src, columns=["version_key"])["version_key"]
    return sorted(keys.astype(str).unique())


def build_manifest(data_version: str, changes: str) -> dict:
    files: dict[str, dict] = {}
    for fname in INDEX_FILES:
        src = LIB_DATA_DIR / fname
        if not src.exists():
            raise FileNotFoundError(f"missing index file: {src}")
        files[fname] = {"size": src.stat().st_size, "sha256": _sha256(src)}

    existing = _load_existing_history()
    # 같은 data_version 이 이미 있으면 갱신, 아니면 새로 추가 (최신이 맨 위)
    new_entry = {"version": data_version, "changes": changes}
    history = [new_entry] + [h for h in existing if h.get("version") != data_version]

    return {
        "data_version": data_version,
        "schema_version": SCHEMA_VERSION,
        "min_lib_version": MIN_LIB_VERSION,
        "created_at": _iso_now(),
        "versions": _version_keys(),
        "history": history,
        "files": files,
    }


def copy_files(dry_run: bool) -> None:
    if not dry_run:
        DIST_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for fname in INDEX_FILES:
        src = LIB_DATA_DIR / fname
        dst = DIST_DATA_DIR / fname
        print(f"  copy {src.name} -> {dst.relative_to(REPO_ROOT)}")
        if not dry_run:
            shutil.copy2(src, dst)


def write_manifest(manifest: dict, dry_run: bool) -> None:
    out = DIST_DATA_DIR / "manifest.json"
    print(f"  write {out.relative_to(REPO_ROOT)}")
    if not dry_run:
        out.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def append_changelog(data_version: str, changes: str, dry_run: bool) -> None:
    """CHANGELOG.md 앞에 엔트리 추가 (최신이 위)."""
    entry = f"## {data_version}\n\n- {changes}\n\n"
    if CHANGELOG_FILE.exists() and not dry_run:
        existing = CHANGELOG_FILE.read_text(encoding="utf-8")
    else:
        existing = "# admdongkor data changelog\n\n"

    # 같은 data_version 이 이미 있으면 스킵 (append_changelog 중복 호출 방어)
    if f"## {data_version}" in existing:
        print(f"  changelog: {data_version} already present, skipping")
        return

    # 헤더 다음에 새 엔트리 삽입
    header, _, rest = existing.partition("\n\n")
    new_body = header + "\n\n" + entry + rest

    print(f"  append {CHANGELOG_FILE.relative_to(REPO_ROOT)}")
    if not dry_run:
        CHANGELOG_FILE.write_text(new_body, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-version", default=_default_data_version())
    ap.add_argument("--changes", required=True,
                    help="이번 수정 내용 (manifest.history + CHANGELOG.md 에 기록)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"data_version: {args.data_version}")
    print(f"changes: {args.changes}")
    print(f"dry_run: {args.dry_run}")
    print()

    print("=== ensure _index_v3 alias ===")
    _ensure_v3_alias(args.dry_run)

    print("\n=== build manifest ===")
    manifest = build_manifest(args.data_version, args.changes)

    print("\n=== copy index files ===")
    copy_files(args.dry_run)

    print("\n=== write manifest.json ===")
    write_manifest(manifest, args.dry_run)

    print("\n=== update CHANGELOG.md ===")
    append_changelog(args.data_version, args.changes, args.dry_run)

    print("\nDone.")
    if not args.dry_run:
        print("\nnext:")
        print(f"  cd {REPO_ROOT}")
        print("  git add dist/data/")
        print(f"  git commit -m \"data: {args.data_version} - {args.changes}\"")
        print("  git push")
    return 0


if __name__ == "__main__":
    sys.exit(main())
