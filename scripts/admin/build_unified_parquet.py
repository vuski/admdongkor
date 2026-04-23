"""GeoJSON → 통일 parquet 3종 (emd, sgg, sido) 원스톱 변환.

관리자 시나리오 (4개월 주기):
    1. raw/geojson/YYYYMMDD/HangJeongDong_verYYYYMMDD.geojson 배치
    2. 아래 실행:

        python scripts/admin/build_unified_parquet.py --version 20260501

    3. parquet/{emd,sgg,sido}_20260501.parquet 3개 생성

옵션:
    --raw-root PATH    원본 GeoJSON 루트 (기본 Z:/Github/admdongkor/raw/geojson)
    --out PATH         parquet 출력 루트 (기본 Z:/Github/admdongkor/parquet)
    --geojson PATH     특정 GeoJSON 파일 직접 지정 (raw 구조 무시)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dissolve import build_sgg, build_sido
from geojson_loader import load_hangjeongdong_geojson, to_admdongkor_schema

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = REPO_ROOT / "raw" / "geojson"
DEFAULT_OUT = REPO_ROOT / "parquet"


def locate_geojson(version: str, raw_root: Path) -> Path:
    """raw/geojson/<version>/ 에서 .geojson 파일 찾기."""
    version_dir = raw_root / version
    if not version_dir.exists():
        raise FileNotFoundError(f"not found: {version_dir}")
    candidates = list(version_dir.glob("*.geojson"))
    if not candidates:
        raise FileNotFoundError(f"no .geojson in {version_dir}")
    if len(candidates) > 1:
        # HangJeongDong_ver* 패턴 우선
        for p in candidates:
            if p.name.startswith("HangJeongDong"):
                return p
    return candidates[0]


def build(version: str, geojson_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    print(f"loading {geojson_path.name} ...", flush=True)
    raw = load_hangjeongdong_geojson(geojson_path)
    emd = to_admdongkor_schema(raw)
    print(f"  emd: {len(emd)} rows, emdcd notna={emd['emdcd'].notna().sum()}", flush=True)

    print(f"dissolving sgg ...", flush=True)
    sgg = build_sgg(emd)
    print(f"  sgg: {len(sgg)} rows", flush=True)

    print(f"dissolving sido ...", flush=True)
    sido = build_sido(emd)
    print(f"  sido: {len(sido)} rows", flush=True)

    emd_out = out_dir / f"emd_{version}.parquet"
    sgg_out = out_dir / f"sgg_{version}.parquet"
    sido_out = out_dir / f"sido_{version}.parquet"

    emd.to_parquet(emd_out, index=False)
    sgg.to_parquet(sgg_out, index=False)
    sido.to_parquet(sido_out, index=False)

    elapsed = time.perf_counter() - t0
    print(f"\ndone in {elapsed:.1f}s", flush=True)
    print(f"  {emd_out}  ({emd_out.stat().st_size/1024/1024:.1f} MB)", flush=True)
    print(f"  {sgg_out}  ({sgg_out.stat().st_size/1024/1024:.1f} MB)", flush=True)
    print(f"  {sido_out}  ({sido_out.stat().st_size/1024/1024:.1f} MB)", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, help="YYYYMMDD")
    ap.add_argument("--raw-root", type=Path, default=DEFAULT_RAW)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--geojson", type=Path, default=None,
                    help="geojson 직접 지정 (--raw-root 무시)")
    args = ap.parse_args()

    if args.geojson:
        gj = args.geojson
    else:
        gj = locate_geojson(args.version, args.raw_root)
    build(args.version, gj, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
