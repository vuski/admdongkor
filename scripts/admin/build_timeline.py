"""parquet/simplified/*_light.parquet -> timeline/v/<ver>/{geom.bin, meta.parquet}.

설계: adk-master/.readme/admdongkor/2026-04-25-timeline-meta-size-analysis.md

버전별 독립 파일셋 (format_version=2):

  geom.bin        : WKB concat. 정렬 sido → sgg → emd, 각 레벨 내 코드 오름차순
  meta.parquet    : columns = (code: string, length: uint32). 같은 순서.
                    offset 은 length 의 누적합으로 클라에서 재구성.
                    레벨 판정은 code.length (2=sido, 5=sgg, 10=emd).

이름/area/sgg_range/emd_range 등은 meta 에서 전부 제거.
  - 이름 검색 → admdongkor JS 라이브러리의 find() 가 _index.parquet 참조
  - 라벨 이름 → 동일
  - 하위 code 목록 → meta.parquet 의 code 배열을 prefix 필터로 뽑음
  - batch Range → 필터한 자식들 length 누적합의 min/max 로 계산

1990 이전 버전의 null code 는 이름 기반 surrogate 그대로 유지:
  sidocd_surrogate = "name:" + sidonm
  sggcd_surrogate  = "name:" + sidonm + "|" + sggnm
  emdcd_surrogate  = "name:" + sidonm + "|" + sggnm + "|" + emdnm

⚠️  surrogate code 는 `code.length` 로 레벨을 판정할 수 없다. 이 경우
    meta.parquet 에 `level` 컬럼도 함께 기록한다 (sido/sgg/emd).
    코드가 진짜 숫자면 length 로 판정 가능하지만 surrogate 때문에 level 컬럼 필수.

Usage:
    python scripts/admin/build_timeline.py --version 20240101
    python scripts/admin/build_timeline.py --all
    python scripts/admin/build_timeline.py --all --force
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]   # admdongkor/
SIMPLIFIED_DIR = REPO_ROOT / "parquet" / "simplified"
OUT_ROOT = REPO_ROOT / "web" / "public" / "timeline"

FORMAT_VERSION = 2


# ---------------------------------------------------------------------------
# surrogate code 처리 (null code 인 초기 연도용)
# ---------------------------------------------------------------------------
def _surrogate_codes(
    sido_df: gpd.GeoDataFrame,
    sgg_df: gpd.GeoDataFrame,
    emd_df: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    sido_df = sido_df.copy()
    sgg_df = sgg_df.copy()
    emd_df = emd_df.copy()

    def _name_key_sido(row) -> str:
        return f"name:{row['sidonm']}"

    def _name_key_sgg(row) -> str:
        return f"name:{row['sidonm']}|{row['sggnm']}"

    def _name_key_emd(row) -> str:
        return f"name:{row['sidonm']}|{row['sggnm']}|{row['emdnm']}"

    def _resolve_duplicates(codes: list[str]) -> list[str]:
        seen: dict[str, int] = {}
        out: list[str] = []
        for c in codes:
            n = seen.get(c, 0) + 1
            seen[c] = n
            out.append(c if n == 1 else f"{c}#{n}")
        return out

    if sido_df["sidocd"].isna().any():
        if not sido_df["sidocd"].isna().all():
            raise ValueError("sido: partial null sidocd is unexpected")
        names = [_name_key_sido(r) for _, r in sido_df.iterrows()]
        sido_df["sidocd"] = _resolve_duplicates(names)

    if sgg_df["sidocd"].isna().any() or sgg_df["sggcd"].isna().any():
        if not (sgg_df["sidocd"].isna().all() and sgg_df["sggcd"].isna().all()):
            raise ValueError("sgg: partial null codes is unexpected")
        sgg_df["sidocd"] = [_name_key_sido(r) for _, r in sgg_df.iterrows()]
        names = [_name_key_sgg(r) for _, r in sgg_df.iterrows()]
        sgg_df["sggcd"] = _resolve_duplicates(names)

    if emd_df["sidocd"].isna().any() or emd_df["sggcd"].isna().any() or emd_df["emdcd"].isna().any():
        if not (
            emd_df["sidocd"].isna().all()
            and emd_df["sggcd"].isna().all()
            and emd_df["emdcd"].isna().all()
        ):
            raise ValueError("emd: partial null codes is unexpected")
        emd_df["sidocd"] = [_name_key_sido(r) for _, r in emd_df.iterrows()]
        emd_df["sggcd"] = [_name_key_sgg(r) for _, r in emd_df.iterrows()]
        names = [_name_key_emd(r) for _, r in emd_df.iterrows()]
        emd_df["emdcd"] = _resolve_duplicates(names)

    return sido_df, sgg_df, emd_df


# ---------------------------------------------------------------------------
# 버전 1개 처리
# ---------------------------------------------------------------------------
def build_version(version: str, force: bool = False) -> dict:
    t0 = time.perf_counter()

    out_dir = OUT_ROOT / "v" / version
    if out_dir.exists() and not force:
        expected = [out_dir / n for n in ("geom.bin", "meta.parquet")]
        if all(p.exists() for p in expected):
            return {"version": version, "skipped": True, "elapsed": 0.0}

    sido_src = SIMPLIFIED_DIR / f"sido_{version}_light.parquet"
    sgg_src = SIMPLIFIED_DIR / f"sgg_{version}_light.parquet"
    emd_src = SIMPLIFIED_DIR / f"emd_{version}_light.parquet"
    for p in (sido_src, sgg_src, emd_src):
        if not p.exists():
            raise FileNotFoundError(p)

    sido_df = gpd.read_parquet(sido_src)
    sgg_df = gpd.read_parquet(sgg_src)
    emd_df = gpd.read_parquet(emd_src)

    # 1) null code -> surrogate
    sido_df, sgg_df, emd_df = _surrogate_codes(sido_df, sgg_df, emd_df)

    # 2) 정렬
    sido_df = sido_df.sort_values("sidocd", kind="stable").reset_index(drop=True)
    sgg_df = sgg_df.sort_values(["sidocd", "sggcd"], kind="stable").reset_index(drop=True)
    emd_df = emd_df.sort_values(["sidocd", "sggcd", "emdcd"], kind="stable").reset_index(drop=True)

    out_dir.mkdir(parents=True, exist_ok=True)

    # 3) geom.bin + meta rows (sido → sgg → emd 순)
    codes: list[str] = []
    lengths: list[int] = []
    levels: list[str] = []

    geom_path = out_dir / "geom.bin"
    with open(geom_path, "wb") as f:
        for _, row in sido_df.iterrows():
            wkb = bytes(row.geometry.wkb)
            f.write(wkb)
            codes.append(str(row.sidocd))
            lengths.append(len(wkb))
            levels.append("sido")
        for _, row in sgg_df.iterrows():
            wkb = bytes(row.geometry.wkb)
            f.write(wkb)
            codes.append(str(row.sggcd))
            lengths.append(len(wkb))
            levels.append("sgg")
        for _, row in emd_df.iterrows():
            wkb = bytes(row.geometry.wkb)
            f.write(wkb)
            codes.append(str(row.emdcd))
            lengths.append(len(wkb))
            levels.append("emd")

    # 4) meta.parquet 저장.
    #    레벨은 code.length 로 판정 가능하지만 surrogate("name:...") 때문에 level 컬럼 필수.
    table = pa.table({
        "code": pa.array(codes, type=pa.string()),
        "length": pa.array(lengths, type=pa.uint32()),
        "level": pa.array(levels, type=pa.dictionary(pa.int8(), pa.string())),
    })
    meta_path = out_dir / "meta.parquet"
    pq.write_table(
        table,
        meta_path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=False,
    )

    elapsed = time.perf_counter() - t0
    return {
        "version": version,
        "skipped": False,
        "elapsed": elapsed,
        "files": {
            "geom.bin": geom_path.stat().st_size,
            "meta.parquet": meta_path.stat().st_size,
        },
        "counts": {
            "sido": len(sido_df),
            "sgg": len(sgg_df),
            "emd": len(emd_df),
        },
    }


def update_versions_json() -> dict:
    versions_dir = OUT_ROOT / "v"
    versions = sorted(
        p.name for p in versions_dir.iterdir()
        if p.is_dir() and (p / "meta.parquet").exists() and (p / "geom.bin").exists()
    ) if versions_dir.exists() else []
    payload = {
        "versions": versions,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format_version": FORMAT_VERSION,
    }
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    out = OUT_ROOT / "versions.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def discover_all_versions() -> list[str]:
    versions = set()
    for p in SIMPLIFIED_DIR.glob("emd_*_light.parquet"):
        stem = p.stem  # emd_20240101_light
        parts = stem.split("_")
        if len(parts) == 3 and parts[0] == "emd" and parts[2] == "light":
            versions.add(parts[1])
    return sorted(versions)


def _print_result(i: int, total: int, v: str, r: dict) -> None:
    if r.get("skipped"):
        print(f"[{i:3d}/{total}] {v}  SKIP (exists)", flush=True)
        return
    fs = r["files"]
    cs = r["counts"]
    print(
        f"[{i:3d}/{total}] {v}  "
        f"geom={fs['geom.bin']:>10,}B  meta={fs['meta.parquet']:>7,}B  "
        f"(sido={cs['sido']} sgg={cs['sgg']} emd={cs['emd']})  "
        f"({r['elapsed']:.1f}s)",
        flush=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--version", nargs="*", default=None, help="특정 버전만 (여러 개 가능)")
    ap.add_argument("--all", action="store_true", help="parquet/simplified/ 전체")
    ap.add_argument("--force", action="store_true", help="이미 있어도 재생성")
    ap.add_argument("--workers", type=int, default=1, help="병렬 처리 (기본 1, 직렬)")
    args = ap.parse_args()

    if args.version:
        versions = list(args.version)
    elif args.all:
        versions = discover_all_versions()
    else:
        ap.error("--version 또는 --all 중 하나 필요")
        return 2

    if not versions:
        print("대상 버전이 없습니다.")
        return 1

    print(f"처리 대상: {len(versions)} 버전, workers={args.workers}, force={args.force}")
    t0 = time.perf_counter()

    results: list[dict] = []
    failed: list[tuple[str, str]] = []

    if args.workers <= 1:
        for i, v in enumerate(versions, 1):
            try:
                r = build_version(v, force=args.force)
                results.append(r)
                _print_result(i, len(versions), v, r)
            except Exception as e:
                print(f"[{i:3d}/{len(versions)}] {v}  FAILED: {e}", flush=True)
                failed.append((v, str(e)))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(build_version, v, args.force): v for v in versions}
            for i, fut in enumerate(as_completed(futs), 1):
                v = futs[fut]
                try:
                    r = fut.result()
                    results.append(r)
                    _print_result(i, len(versions), v, r)
                except Exception as e:
                    print(f"[{i:3d}/{len(versions)}] {v}  FAILED: {e}", flush=True)
                    failed.append((v, str(e)))

    vinfo = update_versions_json()
    print(f"\nversions.json 갱신: {len(vinfo['versions'])} 버전")

    total_bytes = 0
    for d in (OUT_ROOT / "v").iterdir() if (OUT_ROOT / "v").exists() else []:
        for f in d.iterdir():
            total_bytes += f.stat().st_size
    if (OUT_ROOT / "versions.json").exists():
        total_bytes += (OUT_ROOT / "versions.json").stat().st_size
    print(f"총 timeline/ 용량: {total_bytes:,} B = {total_bytes/1024/1024:.1f} MB")

    elapsed = time.perf_counter() - t0
    ok = len(versions) - len(failed)
    print(f"DONE {ok}/{len(versions)} in {elapsed:.1f}s")
    if failed:
        print("실패 버전:")
        for v, msg in failed:
            print(f"  {v}: {msg}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
