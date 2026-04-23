"""전체 파이프라인 래퍼 — 새 버전 추가 시 한 번에 돌리는 관리자 스크립트.

4개월 주기 신규 배포 프로세스:
    1. raw/geojson/<YYYYMMDD>/*.geojson 배치
    2. python scripts/admin/rebuild_all.py --version <YYYYMMDD>

단계:
    Phase 1: GeoJSON → parquet/{emd,sgg,sido}_<version>.parquet
    Phase 2: admdongkor._index.parquet 재빌드 (find() 용)
    Phase 3: 시계열 shape 인덱스 재빌드 (timeline + shape_pairs) × sido/sgg/emd
    Phase 4: 인덱스 산출물을 parquet/ 로 이동

옵션:
    --version YYYYMMDD    처리할 새 버전. 여러 개 공백 구분
    --skip-phase1         parquet 이 이미 있으면 phase1 건너뛰기
    --only-phase N        특정 phase 만 (1/2/3/4). 0 은 _versions.py 만 재생성
    --workers N           병렬 워커 수 (기본 50)
    --dry-run             실행 계획만 출력

단계:
    Phase 1: GeoJSON → parquet/{emd,sgg,sido}_<version>.parquet
    Phase 2: _versions.py 재생성 + admdongkor._index.parquet 재빌드
    Phase 3: 시계열 shape 인덱스 재빌드
    Phase 4: 인덱스 산출물 parquet/ 로 이동

사용 예:
    python scripts/admin/rebuild_all.py --version 20260501
    python scripts/admin/rebuild_all.py --version 20260501 20260901
    python scripts/admin/rebuild_all.py --only-phase 3
    python scripts/admin/rebuild_all.py --skip-phase1 --dry-run
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = REPO_ROOT / "parquet"
SCRIPTS_DIR = REPO_ROOT / "scripts"
ADMIN_DIR = SCRIPTS_DIR / "admin"
LIB_DATA_DIR = REPO_ROOT / "lib" / "src" / "admdongkor" / "data"
VERSIONS_FILE = REPO_ROOT / "lib" / "src" / "admdongkor" / "_versions.py"
PYTHON = sys.executable  # 호출자가 사용한 파이썬 재사용

# 라이브러리에 embed 할 인덱스 파일 (wheel 에 포함됨)
EMBED_FILES = [
    "_index.parquet",
    "timeline_v3_sido.parquet", "timeline_v3_sgg.parquet", "timeline_v3_emd.parquet",
    "shape_pairs_v3_sido.parquet", "shape_pairs_v3_sgg.parquet", "shape_pairs_v3_emd.parquet",
]


def regenerate_versions_py(dry_run: bool) -> None:
    """parquet/emd_*.parquet 을 스캔해 _versions.py 를 재생성."""
    keys = sorted(p.stem.split("_", 1)[1]
                  for p in PARQUET_DIR.glob("emd_*.parquet"))
    if not keys:
        raise RuntimeError(f"no emd_*.parquet in {PARQUET_DIR}")

    # 10개씩 줄바꿈
    lines = ['"""버전 키 상수. rebuild_all.py 가 parquet/ 스캔으로 자동 생성한다. 손대지 말 것."""',
             "",
             "from __future__ import annotations",
             "",
             "VERSIONS: list[str] = ["]
    for i in range(0, len(keys), 6):
        chunk = keys[i:i + 6]
        lines.append("    " + ", ".join(f'"{k}"' for k in chunk) + ",")
    lines.append("]")
    lines.append("")
    lines.append('assert len(VERSIONS) > 0, "VERSIONS must not be empty"')
    lines.append('assert VERSIONS == sorted(VERSIONS), "VERSIONS must be in ascending order"')
    lines.append("")
    content = "\n".join(lines)

    print(f"$ regenerate {VERSIONS_FILE.relative_to(REPO_ROOT)} ({len(keys)} versions)", flush=True)
    if not dry_run:
        VERSIONS_FILE.write_text(content, encoding="utf-8")


def run(cmd: list[str], dry_run: bool = False) -> int:
    print(f"$ {' '.join(str(c) for c in cmd)}", flush=True)
    if dry_run:
        return 0
    return subprocess.call([str(c) for c in cmd])


def phase1_build_parquets(versions: list[str], dry_run: bool) -> None:
    """GeoJSON → 통일 parquet 변환."""
    print("\n=== PHASE 1: GeoJSON -> parquet ===", flush=True)
    script = ADMIN_DIR / "build_unified_parquet.py"
    for v in versions:
        rc = run([PYTHON, script, "--version", v], dry_run=dry_run)
        if rc != 0:
            raise RuntimeError(f"phase1 failed for version {v} (rc={rc})")


def phase2_build_find_index(dry_run: bool) -> None:
    """_versions.py 재생성 + admdongkor.build_index CLI → lib/data/_index.parquet.

    output 을 직접 lib/src/admdongkor/data/ 로 지정해 phase 4 에서 복사 불필요.
    """
    print("\n=== PHASE 2: _versions.py + find() 인덱스 재빌드 ===", flush=True)
    regenerate_versions_py(dry_run)
    if not dry_run:
        LIB_DATA_DIR.mkdir(parents=True, exist_ok=True)
    index_out = LIB_DATA_DIR / "_index.parquet"
    rc = run([PYTHON, "-m", "admdongkor.build_index",
              "--data-root", str(PARQUET_DIR),
              "--output", str(index_out),
              "--verbose"],
             dry_run=dry_run)
    if rc != 0:
        raise RuntimeError(f"phase2 failed (rc={rc})")


def phase3_build_timeseries_index(workers: int, dry_run: bool) -> None:
    """v3 step1/2/3 × sido/sgg/emd. 전체 재빌드."""
    print(f"\n=== PHASE 3: 시계열 shape 인덱스 재빌드 (workers={workers}) ===", flush=True)

    step1 = SCRIPTS_DIR / "measure_v3_step1_spatial_iou.py"
    step2 = SCRIPTS_DIR / "measure_v3_step2_timeline.py"
    step3 = SCRIPTS_DIR / "measure_v3_step3_shape_pairs.py"

    for level in ("sido", "sgg", "emd"):
        print(f"\n--- level: {level} ---", flush=True)
        rc = run([PYTHON, str(step1), "--level", level,
                  "--workers", str(workers), "--min-iou", "0.01"], dry_run=dry_run)
        if rc != 0:
            raise RuntimeError(f"phase3 step1 failed for {level}")

        rc = run([PYTHON, str(step2), "--level", level, "--iou", "0.99"], dry_run=dry_run)
        if rc != 0:
            raise RuntimeError(f"phase3 step2 failed for {level}")

        rc = run([PYTHON, str(step3), "--level", level,
                  "--workers", str(workers)], dry_run=dry_run)
        if rc != 0:
            raise RuntimeError(f"phase3 step3 failed for {level}")


def phase4_move_indexes(dry_run: bool) -> None:
    """인덱스 산출물을 lib/src/admdongkor/data/ 로 배포 (wheel embed 용).

    phase 3 중간 산출물 (scripts/_timeline_v3_*.parquet, scripts/_shape_pairs_v3_*.parquet,
    parquet/_index.parquet) 을 lib/src/admdongkor/data/ 로 모은다.

    이전에는 parquet/ 에도 복사했지만, 라이브러리는 인덱스를 importlib.resources
    로 embed 에서만 읽고 GitHub raw 에서 받지 않으므로 단일 canonical 위치만 유지.
    """
    print("\n=== PHASE 4: 인덱스 배포 (lib/data/) ===", flush=True)

    if not dry_run:
        LIB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) phase 3 산출물 (scripts/_timeline_v3_*.parquet 등) → lib/data/
    for level in ("sido", "sgg", "emd"):
        for fname in (f"_timeline_v3_{level}.parquet",
                      f"_shape_pairs_v3_{level}.parquet"):
            src = SCRIPTS_DIR / fname
            dst = LIB_DATA_DIR / fname.lstrip("_")  # lib/data/ 에선 _prefix 제거
            if not src.exists():
                print(f"  WARN: {src} 없음 (phase3 건너뛴 경우?)", flush=True)
                continue
            print(f"  {src.name} -> {dst.relative_to(REPO_ROOT)}", flush=True)
            if not dry_run:
                shutil.copy2(src, dst)

    # phase 2 의 _index.parquet 은 phase2 에서 직접 lib/data/ 로 쓰므로 복사 불필요


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", nargs="*", default=[],
                    help="처리할 새 버전 YYYYMMDD. phase1 에 전달")
    ap.add_argument("--skip-phase1", action="store_true")
    ap.add_argument("--only-phase", type=int, choices=[1, 2, 3, 4])
    ap.add_argument("--workers", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    t0 = time.perf_counter()

    phases = {1, 2, 3, 4}
    if args.only_phase:
        phases = {args.only_phase}
    if args.skip_phase1:
        phases.discard(1)

    if 1 in phases:
        if not args.version:
            if args.only_phase == 1:
                print("ERROR: phase 1 에는 --version 필수", flush=True)
                return 1
            print("skip phase 1: --version 없음", flush=True)
        else:
            phase1_build_parquets(args.version, args.dry_run)

    if 2 in phases:
        phase2_build_find_index(args.dry_run)

    if 3 in phases:
        phase3_build_timeseries_index(args.workers, args.dry_run)

    if 4 in phases:
        phase4_move_indexes(args.dry_run)

    elapsed = time.perf_counter() - t0
    print(f"\nALL DONE in {elapsed:.1f}s", flush=True)
    if not args.dry_run:
        print("\nnext:", flush=True)
        print("  git status", flush=True)
        print("  # 변경 확인 후 커밋", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
