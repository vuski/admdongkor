"""울릉도 위치 보정 + 독도 삽입 — 원본/light/timeline 전 시점.

## 1. 울릉도 위치 보정

행안부 원본 geojson 단계에서 이미 울릉도 본섬이 실제 위치와 어긋나 있다.
시점 그룹마다 편차가 달라, QGIS 측정(2026-07 기준 dx=+186.856 dy=-411.309,
EPSG:5179)을 참값 기준으로 삼고 그룹별 최적 평행이동량을 IoU 최대화로 구했다.

    그룹7(2016~2026) 최적 = (+186.75, -411.25)  ← 측정치와 0.1m 이내 일치
    → 방법 검증 완료. 나머지 그룹도 같은 방식으로 산출.

보정 후 전 그룹 IoU ≈ 0.965 (잔차는 해안선 재디지타이즈 차이로, 평행이동으로
없앨 수 있는 성분이 아니다).

## 2. 독도는 보정 대상이 아니다

독도는 울릉읍과 **같은 행**에 들어 있지만 편차가 다르다:

    2012 → 2026 사이 이동량   울릉도 477m / 독도 57m

실제 좌표(동도 131.86917,37.23944)와 대조하면 독도는 이미 제 위치다.
→ **울릉도 본섬(+죽도)만 이동시키고 독도는 건드리지 않는다.**

## 3. 독도 — 시점별로 상태가 셋으로 갈린다

    1975~2006        12개  아예 없음                → 20121231 폴리곤 이식
    2007~2012-12-10   6개  있지만 22.65km 북동 오류  → 잘못된 것 제거 후 이식
    2012-12-31~2026  45개  정상                     → 그대로

소속은 1975 는 울릉군 남면, 1980 이후는 울릉군 울릉읍.

독도 오류 위치(131.947,37.434) 는 울릉도 본섬 boundingbox 밖이라 울릉도 보정
box 에도 걸리지 않는다. 별도의 넓은 탐색 상자로 찾아 제거한다.

사용:
    python scripts/admin/fix_ulleung.py --dry-run
    python scripts/admin/fix_ulleung.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import geopandas as gpd
import shapely
import shapely.affinity as aff
from shapely.geometry import MultiPolygon

REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = REPO_ROOT / "parquet"

# EPSG:5179 기준 영역 상자
ULLEUNG_BOX = shapely.box(1_250_000, 1_900_000, 1_350_000, 2_000_000)  # 울릉도+죽도
# 독도 정상 위치. 실제 독도 bounds 는 X 1386.3k~1388.6k / Y 1923.7k~1925.8k 이므로
# 여유 2km 만 준다. 넓게 잡으면 2007~2012 의 오류 위치(Y≈1946.3k) 까지
# "정상" 으로 잘못 판정된다.
DOKDO_BOX = shapely.box(1_384_000, 1_921_500, 1_391_000, 1_928_000)
# 2007~2012 의 잘못 놓인 독도(131.947,37.434 부근) 까지 포함하는 넓은 상자.
# 울릉도 본섬은 ULLEUNG_BOX 쪽이므로 여기 걸리지 않는다.
DOKDO_SEARCH_BOX = shapely.box(1_350_000, 1_880_000, 1_450_000, 1_990_000)
DOKDO_MAX_AREA = 1_000_000   # m² — 독도급 미세 섬만 (본섬 75km² 는 제외)

# 보정 완료 판정용 — 보정 후 울릉도 최남단 Y (전 그룹 공통 수렴값)
FIXED_SOUTH_Y = 1_944_674.3
FIXED_SOUTH_TOL = 50.0

# 시점 → 보정량 (dx, dy) in metres, EPSG:5179. 울릉도 본섬에만 적용.
_G = {
    (+237.25, -1332.25): ["19751231", "19801231", "19851231", "19901231", "19951231"],
    (+237.25, -1331.75): ["20001231", "20011231", "20021231", "20031231",
                          "20041231", "20051231"],
    (+203.50, -392.75): ["20061231", "20071231", "20081231", "20091231",
                         "20101231", "20111231"],
    (+85.75, +58.25): ["20121210"],
    (+78.75, +52.50): ["20121231", "20131231", "20141231", "20151231"],
    (+186.75, -411.25): [
        "20160201", "20170418", "20170801", "20171016", "20180301", "20180401",
        "20180724", "20181106", "20190403", "20190908", "20191001", "20191231",
        "20200101", "20200701", "20201001", "20210101", "20210401", "20210701",
        "20220101", "20220309", "20220401", "20220701", "20221001", "20230101",
        "20230401", "20230701", "20231001", "20231231", "20240101", "20240401",
        "20240701", "20241001", "20241231", "20250101", "20250401", "20250701",
        "20251001", "20251231", "20260201", "20260401", "20260701",
    ],
}
OFFSET: dict[str, tuple[float, float]] = {
    v: off for off, vs in _G.items() for v in vs
}

# 독도를 빌려올 시점 (원본에 독도가 있는 가장 이른 버전)
DONOR_VERSION = "20121231"
DOKDO_EMD_DEFAULT = "울릉읍"
DOKDO_EMD_OVERRIDE = {"19751231": "남면"}
ULLUNG_SGG = "울릉군"


def _split(geom, box):
    """geom 을 (box 안 part, box 밖 part) 로 나눈다."""
    if geom is None or geom.is_empty:
        return [], []
    parts = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    inside = [p for p in parts if p.intersects(box)]
    outside = [p for p in parts if not p.intersects(box)]
    return inside, outside


def _already_fixed(d, gc) -> bool:
    """울릉도 최남단 Y 가 보정 후 기대값 근처면 이미 보정된 파일."""
    ys = [
        p.bounds[1]
        for g in d[gc] if g is not None and not g.is_empty
        for p in (list(g.geoms) if g.geom_type == "MultiPolygon" else [g])
        if p.intersects(ULLEUNG_BOX)
    ]
    if not ys:
        return False
    return abs(min(ys) - FIXED_SOUTH_Y) < FIXED_SOUTH_TOL


def load_donor() -> list:
    src = PARQUET_DIR / f"emd_{DONOR_VERSION}.parquet"
    d = gpd.read_parquet(src)
    gc = d.geometry.name
    parts = []
    for g in d[gc]:
        ins, _ = _split(g, DOKDO_BOX)
        parts += ins
    if len(parts) != 2:
        raise RuntimeError(f"donor {DONOR_VERSION}: expected 2 parts, got {len(parts)}")
    return parts


def fix_emd(version: str, donor: list, dry_run: bool) -> dict:
    """원본 emd parquet 보정. 반환: 통계 dict."""
    path = PARQUET_DIR / f"emd_{version}.parquet"
    d = gpd.read_parquet(path)
    gc = d.geometry.name
    dx, dy = OFFSET[version]

    # 재실행 안전장치 — 평행이동은 몇 번이든 다시 적용되므로, 이미 보정된
    # 파일에 또 돌리면 그만큼 더 밀린다. 보정 후 울릉도 최남단 Y 는 전 그룹이
    # 1944674 ± 몇 m 로 수렴하므로 그것으로 판정한다.
    if _already_fixed(d, gc):
        return {"moved": 0, "added": 0, "removed": 0, "dx": dx, "dy": dy,
                "note": "이미 보정됨 (건너뜀)"}

    # 1) 울릉도 본섬(+죽도) 평행이동. 독도는 다른 상자라 영향 없다.
    moved = 0
    for i in d.index:
        g = d.at[i, gc]
        ins, out = _split(g, ULLEUNG_BOX)
        if not ins:
            continue
        shifted = [aff.translate(p, dx, dy) for p in ins]
        d.at[i, gc] = MultiPolygon(shifted + out)
        moved += len(ins)

    # 2) 독도 상태 판정 — 넓은 상자에서 독도급(작은) part 를 찾는다.
    good, wrong = [], []
    for i in d.index:
        g = d.at[i, gc]
        if g is None or g.is_empty:
            continue
        parts = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
        for p in parts:
            if p.intersects(DOKDO_SEARCH_BOX) and p.area < DOKDO_MAX_AREA:
                (good if p.intersects(DOKDO_BOX) else wrong).append((i, p))

    # 독도가 이미 정상이면 독도 손질은 건너뛰되, **울릉도 이동은 저장해야 한다**.
    # (여기서 early-return 하면 step 1 의 이동이 통째로 버려진다.)
    removed = added = 0
    note = "독도 정상" if (good and not wrong) else ""

    # 3) 잘못 놓인 독도 제거.
    # id() 로 비교하면 안 된다 — d.at[] 로 다시 꺼내면 새 객체가 만들어져
    # id 가 달라진다. 상자 조건으로 다시 판정한다.
    if wrong:
        for i in {i for i, _ in wrong}:
            g = d.at[i, gc]
            parts = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
            keep = [
                p for p in parts
                if not (p.intersects(DOKDO_SEARCH_BOX)
                        and p.area < DOKDO_MAX_AREA
                        and not p.intersects(DOKDO_BOX))
            ]
            if not dry_run:
                d.at[i, gc] = MultiPolygon(keep)
            removed += len(parts) - len(keep)

    # 4) 정상 독도가 없으면 이식
    if not good:
        want = DOKDO_EMD_OVERRIDE.get(version, DOKDO_EMD_DEFAULT)
        mask = (
            d["sggnm"].astype(str).str.contains(ULLUNG_SGG, na=False)
            & (d["emdnm"].astype(str) == want)
        )
        if int(mask.sum()) != 1:
            raise RuntimeError(f"{version}: {ULLUNG_SGG} {want} 행이 {int(mask.sum())}개")
        i = d.index[mask][0]
        g = d.at[i, gc]
        kept = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
        if not dry_run:
            d.at[i, gc] = MultiPolygon(kept + donor)
        added = len(donor)

    if not dry_run:
        d.to_parquet(path, compression="snappy")
    return {"moved": moved, "added": added, "removed": removed,
            "dx": dx, "dy": dy, "note": note}


def fix_sgg_sido(version: str, donor: list, dry_run: bool) -> dict:
    """sgg/sido 원본도 같은 보정 적용.

    sgg/sido 는 emd 를 dissolve 한 결과이므로 동일한 평행이동·독도 처리를
    그대로 적용하면 emd 와 일치한다 (전체 재-dissolve 불필요).
    """
    out = {}
    for level in ("sgg", "sido"):
        path = PARQUET_DIR / f"{level}_{version}.parquet"
        if not path.exists():
            continue
        d = gpd.read_parquet(path)
        gc = d.geometry.name
        dx, dy = OFFSET[version]
        if _already_fixed(d, gc):
            out[level] = (0, 0, 0)
            continue

        moved = 0
        for i in d.index:
            g = d.at[i, gc]
            ins, rest = _split(g, ULLEUNG_BOX)
            if not ins:
                continue
            d.at[i, gc] = MultiPolygon([aff.translate(p, dx, dy) for p in ins] + rest)
            moved += len(ins)

        # 잘못 놓인 독도 제거
        removed = 0
        for i in d.index:
            g = d.at[i, gc]
            if g is None or g.is_empty:
                continue
            parts = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
            keep = [
                p for p in parts
                if not (p.intersects(DOKDO_SEARCH_BOX)
                        and p.area < DOKDO_MAX_AREA
                        and not p.intersects(DOKDO_BOX))
            ]
            if len(keep) != len(parts):
                if not dry_run:
                    d.at[i, gc] = MultiPolygon(keep)
                removed += len(parts) - len(keep)

        # 독도가 없으면 울릉군/경상북도 행에 이식
        has = any(
            p.intersects(DOKDO_BOX)
            for g in d[gc] if g is not None and not g.is_empty
            for p in (list(g.geoms) if g.geom_type == "MultiPolygon" else [g])
        )
        added = 0
        if not has:
            col = "sggnm" if level == "sgg" else "sidonm"
            key = ULLUNG_SGG if level == "sgg" else "경상북도"
            mask = d[col].astype(str).str.contains(key, na=False)
            if int(mask.sum()) != 1:
                raise RuntimeError(f"{version} {level}: {key} 행이 {int(mask.sum())}개")
            i = d.index[mask][0]
            g = d.at[i, gc]
            kept = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]
            if not dry_run:
                d.at[i, gc] = MultiPolygon(kept + donor)
            added = len(donor)

        if not dry_run:
            d.to_parquet(path, compression="snappy")
        out[level] = (moved, removed, added)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", nargs="*", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    versions = args.version or sorted(
        p.stem.removeprefix("emd_") for p in PARQUET_DIR.glob("emd_*.parquet")
    )
    missing = [v for v in versions if v not in OFFSET]
    if missing:
        print(f"보정량 미정의 시점: {missing}")
        return 1

    donor = load_donor()
    print(f"donor={DONOR_VERSION} ({len(donor)} parts), {len(versions)} versions"
          f"{' [DRY-RUN]' if args.dry_run else ''}", flush=True)
    t0 = time.perf_counter()

    failed = []
    for v in versions:
        try:
            r = fix_emd(v, donor, args.dry_run)
            r["ss"] = fix_sgg_sido(v, donor, args.dry_run)
            note = f"moved={r['moved']}"
            if r.get("removed"):
                note += f"  독도오류-{r['removed']}"
            if r["added"]:
                note += f"  독도+{r['added']}"
            if r.get("note"):
                note += f"  {r['note']}"
            ss = r.get("ss", {})
            note += "  [" + " ".join(
                f"{k}:m{v[0]}/r{v[1]}/a{v[2]}" for k, v in ss.items()) + "]"
            print(f"  {v}  dx={r['dx']:+8.2f} dy={r['dy']:+9.2f}  {note}", flush=True)
        except Exception as e:
            print(f"  {v}  FAILED: {e}", flush=True)
            failed.append((v, str(e)))

    print(f"\nDONE ({time.perf_counter() - t0:.1f}s)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
