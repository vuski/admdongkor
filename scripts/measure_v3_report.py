"""v3 실측 최종 리포트."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import OUT_DIR

LEVELS = ["sido", "sgg", "emd"]


def kb(p: Path) -> float:
    return p.stat().st_size / 1024 if p.exists() else 0.0


def main() -> int:
    lines = [
        "# v3 실측 리포트 — global shape_id + 공간 크로스",
        "",
        "## 방법",
        "",
        "1. **step1**: STRtree 공간 후보 기반, 공간적으로 겹치는 모든 element 쌍 IoU 계산 (emdcd 재할당 포함)",
        "2. **step2**: IoU >= 0.99 edge 로 global UF → 도형 공간 기준 shape_id (element_id 무관)",
        "3. **step3**: 공간 크로스에서 IoU < 0.99 인 shape_id 쌍 전수 intersection + weight",
        "",
        "## Level 별 결과",
        "",
    ]

    for lv in LEVELS:
        iou_path = OUT_DIR / f"_spatial_iou_{lv}.parquet"
        tl_path = OUT_DIR / f"_timeline_v3_{lv}.parquet"
        sp_path = OUT_DIR / f"_shape_pairs_v3_{lv}.parquet"

        if not all(p.exists() for p in (iou_path, tl_path, sp_path)):
            lines.append(f"### {lv} — (missing outputs)")
            lines.append("")
            continue

        iou = pd.read_parquet(iou_path)
        tl = pd.read_parquet(tl_path)
        sp = pd.read_parquet(sp_path)

        n_elements = tl.element_id.nunique()
        n_shapes = tl.shape_id.nunique()
        shapes_per_element = tl.groupby("element_id").shape_id.nunique()
        elements_per_shape = tl.groupby("shape_id").element_id.nunique()
        reused = (elements_per_shape > 1).sum()

        cross = (iou.element_id_a != iou.element_id_b).sum()

        lines.extend([
            f"### `{lv}`",
            "",
            f"| 지표 | 값 |",
            f"|---|---|",
            f"| element 수 | {n_elements:,} |",
            f"| **global shape 수** | **{n_shapes:,}** |",
            f"| 재할당 shape 수 (여러 element_id 에서 사용) | {reused:,} |",
            f"| step1 rows (공간 크로스 IoU) | {len(iou):,} |",
            f"| - cross-element rows | {cross:,} |",
            f"| step1 크기 | {kb(iou_path):,.1f} KB |",
            f"| timeline rows | {len(tl):,} |",
            f"| timeline 크기 | {kb(tl_path):,.1f} KB |",
            f"| **shape pairs rows** | **{len(sp):,}** |",
            f"| **shape pairs 크기** | **{kb(sp_path):,.1f} KB** |",
            f"| shapes/element: mean | {shapes_per_element.mean():.2f} (max {shapes_per_element.max()}) |",
            f"| elements/shape: mean | {elements_per_shape.mean():.2f} (max {elements_per_shape.max()}) |",
            "",
        ])

    lines.append("## 총합 (배포용 인덱스)")
    lines.append("")
    lines.append("| 파일 | 용량 |")
    lines.append("|---|---|")
    total_tl = sum(kb(OUT_DIR / f"_timeline_v3_{lv}.parquet") for lv in LEVELS)
    total_sp = sum(kb(OUT_DIR / f"_shape_pairs_v3_{lv}.parquet") for lv in LEVELS)
    lines.extend([
        f"| 전체 timeline | **{total_tl:,.1f} KB** |",
        f"| 전체 shape pairs | **{total_sp:,.1f} KB** |",
        f"| **합계** | **{(total_tl+total_sp)/1024:.2f} MB** |",
        "",
        "## v2 대비 차이",
        "",
        "- v2: element-local shape_id, 같은 emdcd 내에서만 비교 → 재할당·분동·합동 놓침",
        "- v3: global shape_id, 공간 크로스 → 재할당 추적 + 모든 서로 다른 shape 쌍 직접 저장",
        "- v3 의 shape_pairs 는 연쇄(chain)가 필요 없음 — 어느 두 shape 든 직접 조회",
        "",
    ])

    out_path = OUT_DIR / "_measure_v3_report.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"report: {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
