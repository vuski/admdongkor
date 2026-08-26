"""**원본 geojson** 의 울릉도 위치 보정 + 독도 정리.

`fix_ulleung.py` 는 산출물(parquet) 을 고쳤지만, parquet 은 이 geojson 에서
다시 빌드되므로 재빌드하면 보정이 사라진다. **원본을 고쳐야 근본 해결**이다.

## 대상

`admdongkor/ver<YYYYMMDD>/*.geojson` — 2012-12-10 및 2016-02-01 이후 시점.
1975~2011 은 geojson 이 없다 (통계청 shapefile 이 원본, 별도 처리 필요).

## 보정 내용

1. **울릉도 본섬(+죽도) 평행이동** — EPSG:5179 기준 그룹별 (dx, dy).
   WGS84 파일은 5179 로 변환 → 이동 → 역변환한다. 도 단위로 근사 이동하면
   위도에 따라 오차가 생긴다.

2. **독도는 이동하지 않는다.** 울릉읍과 같은 feature 안에 있지만 편차가 다르고
   (2012→2026 울릉도 477m vs 독도 57m), 실제 좌표와 대조하면 이미 제 위치다.

3. **ver20121210 의 오류 독도 교체** — (131.95, 37.43) 에 있는 두 폴리곤은
   실제 위치에서 22.65km 북동쪽이다. 제거하고 ver20160201 의 독도를 이식한다.

## 주의

`ver20200701` 은 **EPSG:5179 로 저장**돼 있다 (나머지 47개는 WGS84, CRS 선언도
없음). 좌표 단위를 파일별로 판정해 처리하며, **원래 좌표계를 바꾸지 않는다.**

`ver20220309` 의 `*_vote_simple.geojson` 은 선거구용 별도 자산이라 제외한다.

## 파일을 통째로 재직렬화하지 않는다

이 geojson 들은 ogr2ogr 스타일로 **feature 1개 = 1줄** 이다. `json.dumps` 로
전체를 다시 쓰면 (a) 34MB 가 한 줄이 되어 git diff 가 무의미해지고,
(b) 좌표 문자열 표현이 바뀌어 손대지 않은 feature 까지 전부 변경된다
(실측 -2.9MB).

→ **울릉군 feature 가 들어있는 줄만** 파싱해서 고치고, 나머지 줄은 원문
그대로 흘려보낸다. 결과적으로 파일당 3줄만 바뀐다.

사용:
    python scripts/admin/fix_ulleung_geojson.py --dry-run
    python scripts/admin/fix_ulleung_geojson.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import shapely
import shapely.ops
import shapely.affinity as aff
from pyproj import Transformer
from shapely.geometry import mapping, shape

REPO_ROOT = Path(__file__).resolve().parents[2]

TO_5179 = Transformer.from_crs(4326, 5179, always_xy=True)
TO_4326 = Transformer.from_crs(5179, 4326, always_xy=True)

# EPSG:5179 기준 영역 상자
ULLEUNG_BOX = shapely.box(1_250_000, 1_900_000, 1_350_000, 2_000_000)
DOKDO_BOX = shapely.box(1_384_000, 1_921_500, 1_391_000, 1_928_000)
DOKDO_SEARCH_BOX = shapely.box(1_350_000, 1_880_000, 1_450_000, 1_990_000)
DOKDO_MAX_AREA = 1_000_000  # m²

# 시점별 울릉도 보정량 (dx, dy) — fix_ulleung.py 와 동일한 산출값.
OFFSET_20121210 = (+85.75, +58.25)
OFFSET_2016PLUS = (+186.75, -411.25)

# 독도를 빌려올 파일 (오류 독도 교체용)
DONOR = "ver20160201/HangJeongDong_ver20160201.geojson"

EXCLUDE_SUFFIX = "_vote_simple.geojson"


# feature 줄의 시작 위치. 공백 배치가 파일마다 다르고, 이 스크립트가 다시 쓴
# 줄은 json.dumps 포맷('{"type": "Feature"') 이라 또 다르다. 고정 문자열로
# 찾으면 재실행 시 인식하지 못하므로 정규식으로 잡는다.
_FEATURE_RE = re.compile(r'\{\s*"type"\s*:\s*"Feature"')


def _feature_start(line: str) -> int:
    m = _FEATURE_RE.search(line)
    return m.start() if m else -1


def offset_for(version: str) -> tuple[float, float]:
    return OFFSET_20121210 if version <= "20121210" else OFFSET_2016PLUS


def _is_metric(geom_coords) -> bool:
    """첫 좌표 크기로 5179(m) / 4326(도) 판정."""
    c = geom_coords
    while isinstance(c[0], list):
        c = c[0]
    return abs(c[0]) > 1000


def to_metric(g, metric: bool):
    return g if metric else shapely.ops.transform(TO_5179.transform, g)


def from_metric(g, metric: bool):
    return g if metric else shapely.ops.transform(TO_4326.transform, g)


# 보정 완료 판정용 — 보정 후 울릉도 최남단 Y (EPSG:5179)
FIXED_SOUTH_Y = 1_944_674.4
FIXED_SOUTH_TOL = 60.0


def _already_fixed(lines: list[str], metric: bool) -> bool:
    ys = []
    for ln in lines:
        if "울릉" not in ln:
            continue
        i = _feature_start(ln)
        if i < 0:
            continue
        try:
            ft = json.loads(ln[i:].rstrip().rstrip(","))
        except Exception:
            continue
        if "울릉" not in str(ft["properties"].get("adm_nm", "")):
            continue
        g = to_metric(shape(ft["geometry"]), metric)
        for p in (list(g.geoms) if g.geom_type == "MultiPolygon" else [g]):
            if p.intersects(ULLEUNG_BOX):
                ys.append(p.bounds[1])
    return bool(ys) and abs(min(ys) - FIXED_SOUTH_Y) < FIXED_SOUTH_TOL


def load_donor_parts() -> list:
    """정상 독도 2개 part 를 EPSG:5179 로 반환."""
    f = REPO_ROOT / DONOR
    j = json.loads(f.read_text(encoding="utf-8"))
    metric = _is_metric(j["features"][0]["geometry"]["coordinates"])
    out = []
    for ft in j["features"]:
        if "울릉" not in str(ft["properties"].get("adm_nm", "")):
            continue
        g = to_metric(shape(ft["geometry"]), metric)
        for p in (list(g.geoms) if g.geom_type == "MultiPolygon" else [g]):
            if p.intersects(DOKDO_BOX) and p.area < DOKDO_MAX_AREA:
                out.append(p)
    if len(out) != 2:
        raise RuntimeError(f"donor: expected 2 dokdo parts, got {len(out)}")
    return out


def fix_geometry(geom: dict, dx: float, dy: float, metric: bool,
                 donor: list, is_ulleungeup: bool) -> tuple[dict, int, int, int]:
    """feature 하나의 geometry 를 보정. 반환: (geometry, moved, removed, added)."""
    g = to_metric(shape(geom), metric)
    parts = list(g.geoms) if g.geom_type == "MultiPolygon" else [g]

    moved = removed = added = 0
    new_parts = []
    has_good_dokdo = False
    for p in parts:
        if p.intersects(ULLEUNG_BOX):
            new_parts.append(aff.translate(p, dx, dy))      # 울릉도 이동
            moved += 1
        elif (p.intersects(DOKDO_SEARCH_BOX) and p.area < DOKDO_MAX_AREA
              and not p.intersects(DOKDO_BOX)):
            removed += 1                                     # 오류 독도 제거
        else:
            if p.intersects(DOKDO_BOX):
                has_good_dokdo = True
            new_parts.append(p)                              # 독도는 그대로

    if removed and not has_good_dokdo and is_ulleungeup:
        new_parts += donor
        added = len(donor)

    merged = (new_parts[0] if len(new_parts) == 1
              else shapely.MultiPolygon(new_parts))
    return mapping(from_metric(merged, metric)), moved, removed, added


def fix_file(f: Path, donor: list, dry_run: bool) -> dict:
    """울릉군 feature 가 든 줄만 고치고 나머지 줄은 원문 그대로 유지."""
    version = "".join(ch for ch in f.parent.name if ch.isdigit())
    dx, dy = offset_for(version)

    lines = f.read_text(encoding="utf-8").splitlines(keepends=True)

    # 좌표 단위 판정 — 아무 feature 줄이나 하나 파싱
    metric = None
    for ln in lines:
        i = _feature_start(ln)
        if i < 0:
            continue
        body = ln[i:].rstrip().rstrip(",")
        try:
            metric = _is_metric(json.loads(body)["geometry"]["coordinates"])
            break
        except Exception:
            continue
    if metric is None:
        raise RuntimeError(f"{f.name}: feature 줄을 찾지 못함")

    # 재실행 안전장치 — 평행이동은 재적용되면 그만큼 더 밀린다.
    # 보정 후 울릉도 최남단 Y 로 이미 보정된 파일을 판정한다.
    if _already_fixed(lines, metric):
        return {"moved": 0, "removed": 0, "added": 0,
                "dx": dx, "dy": dy, "lines": 0, "note": "이미 보정됨"}

    moved = removed = added = 0
    touched = 0
    for k, ln in enumerate(lines):
        if "울릉" not in ln:
            continue
        i = _feature_start(ln)
        if i < 0:
            continue
        prefix, rest = ln[:i], ln[i:]
        stripped = rest.rstrip()
        tail = rest[len(stripped):]              # 개행/공백
        comma = ""
        if stripped.endswith(","):
            stripped, comma = stripped[:-1], ","

        ft = json.loads(stripped)
        nm = str(ft["properties"].get("adm_nm", ""))
        if "울릉" not in nm:
            continue

        ft["geometry"], m, r, a = fix_geometry(
            ft["geometry"], dx, dy, metric, donor, "울릉읍" in nm)
        moved += m
        removed += r
        added += a
        lines[k] = prefix + json.dumps(ft, ensure_ascii=False) + comma + tail
        touched += 1

    if touched and not dry_run:
        f.write_text("".join(lines), encoding="utf-8")
    return {"moved": moved, "removed": removed, "added": added,
            "dx": dx, "dy": dy, "lines": touched}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    files = [f for f in sorted(REPO_ROOT.glob("ver*/*.geojson"))
             if not f.name.endswith(EXCLUDE_SUFFIX)]
    donor = load_donor_parts()
    print(f"donor={len(donor)} parts, {len(files)} geojson"
          f"{' [DRY-RUN]' if args.dry_run else ''}", flush=True)
    t0 = time.perf_counter()

    failed = []
    for f in files:
        try:
            r = fix_file(f, donor, args.dry_run)
            note = r.get("note") or f"lines={r['lines']} moved={r['moved']}"
            if r["removed"]:
                note += f"  오류독도-{r['removed']}"
            if r["added"]:
                note += f"  독도+{r['added']}"
            print(f"  {f.parent.name:14} dx={r['dx']:+7.2f} dy={r['dy']:+8.2f}  {note}",
                  flush=True)
        except Exception as e:
            print(f"  {f.parent.name:14} FAILED: {e}", flush=True)
            failed.append((f.name, str(e)))

    print(f"\nDONE ({time.perf_counter() - t0:.1f}s)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
