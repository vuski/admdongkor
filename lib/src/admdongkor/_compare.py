"""두 시점 emd 비교 (compare).

기준:
    - emdcd (행안부 10자리) 공통 집합을 같은 emd 로 봄
    - 두 version 의 shape_id 가 같으면 same (완전 동일)
    - 다르면 shape_pairs 의 iou 조회 → threshold 이상이면 same 로 승격 (미세 경계)
    - 한쪽 version 에만 있는 emdcd 는 diff + status (only_in_a / only_in_b)

이름/코드만 바뀐 경우 (공간 동일) 는 shape_id 같아서 same. threshold 를 1.0 으로
주면 엄격히 완전 동일 shape 만 same.
"""

from __future__ import annotations

import pandas as pd

from ._match import _load_shape_pairs, _load_timeline

_OUT_COLS = [
    "version_key", "emdcd", "emdnm",
    "sggcd", "sggnm", "sidocd", "sidonm",
    "shape_id",
]

_DIFF_COLS = _OUT_COLS + ["status", "iou"]


class CompareResult:
    """`compare()` 반환 컨테이너. `.same()` / `.diff()` 로 각 DataFrame 조회.

    Attributes:
        va, vb: 비교한 두 버전 키 (list 입력 순서)
        threshold: iou threshold
    """

    def __init__(self, same_df: pd.DataFrame, diff_df: pd.DataFrame,
                 va: str, vb: str, threshold: float):
        self._same = same_df
        self._diff = diff_df
        self.va = va
        self.vb = vb
        self.threshold = threshold

    def same(self) -> pd.DataFrame:
        """경계·공간이 (threshold 이상) 같은 emd 들. 각 emdcd 당 2 rows (va, vb)."""
        return self._same.copy()

    def diff(self) -> pd.DataFrame:
        """변화 있는 emd 들. 컬럼 `status`:
            - `changed`   : 두 version 모두 존재, shape 다름 (iou < threshold)
            - `only_in_a` : va 에만 존재
            - `only_in_b` : vb 에만 존재
        각 emdcd 당 1 (only_*) 또는 2 (changed) rows.
        """
        return self._diff.copy()

    def __repr__(self) -> str:
        return (f"<CompareResult va={self.va} vb={self.vb} threshold={self.threshold} "
                f"same={len(self._same)//2} diff={self._diff.emdcd.nunique()}>")


def _project_timeline(tl_v: pd.DataFrame) -> pd.DataFrame:
    """timeline 에서 공개 컬럼만 뽑기."""
    return tl_v.rename(columns={"element_id": "emdcd", "name": "emdnm"})[
        ["version_key", "emdcd", "emdnm",
         "sggcd", "sggnm", "sidocd", "sidonm", "shape_id"]
    ]


def compare(versions: list[str], threshold: float = 0.99) -> CompareResult:
    """두 시점 emd 비교.

    Args:
        versions: `[va, vb]` 정확히 2개. 버전 키 문자열.
        threshold: shape_id 가 다를 때 `shape_pairs.iou >= threshold` 면 same 으로 승격.
            기본 0.99 (미세 경계 변화 무시). 1.0 으로 주면 엄격히 shape_id 일치만 same.

    Returns:
        CompareResult. `.same()` / `.diff()` 로 DataFrame 조회.
    """
    if not isinstance(versions, list) or len(versions) != 2:
        raise ValueError("versions must be a list of exactly 2 version keys")
    va, vb = versions
    if not isinstance(va, str) or not isinstance(vb, str):
        raise TypeError("versions items must be str")
    if not (0.0 <= threshold <= 1.0):
        raise ValueError("threshold must be in [0.0, 1.0]")

    tl = _load_timeline("emd")
    sp = _load_shape_pairs("emd")

    tl_a = tl[tl.version_key == va]
    tl_b = tl[tl.version_key == vb]
    if tl_a.empty:
        raise ValueError(f"unknown version in timeline: {va!r}")
    if tl_b.empty:
        raise ValueError(f"unknown version in timeline: {vb!r}")

    # 공통 emdcd — shape_id 비교
    merged = tl_a[["element_id", "shape_id"]].merge(
        tl_b[["element_id", "shape_id"]],
        on="element_id", suffixes=("_a", "_b"), how="inner",
    )

    # 기본 same/diff 분류
    merged["_exact_same"] = merged.shape_id_a == merged.shape_id_b

    # 다른 것들에 대해 shape_pairs 에서 iou 조회
    diff_rows = merged[~merged._exact_same].copy()
    if not diff_rows.empty:
        diff_rows["_sid_lo"] = diff_rows[["shape_id_a", "shape_id_b"]].min(axis=1)
        diff_rows["_sid_hi"] = diff_rows[["shape_id_a", "shape_id_b"]].max(axis=1)
        pair_iou = sp[["shape_id_a", "shape_id_b", "iou"]].rename(
            columns={"shape_id_a": "_sid_lo", "shape_id_b": "_sid_hi"}
        )
        diff_rows = diff_rows.merge(pair_iou, on=["_sid_lo", "_sid_hi"], how="left")
        diff_rows["iou"] = diff_rows["iou"].fillna(0.0)
        diff_rows["_promoted_same"] = diff_rows["iou"] >= threshold
    else:
        diff_rows["iou"] = pd.Series(dtype=float)
        diff_rows["_promoted_same"] = pd.Series(dtype=bool)

    same_ids = set(merged.loc[merged._exact_same, "element_id"]) | \
               set(diff_rows.loc[diff_rows["_promoted_same"] if not diff_rows.empty else [],
                                 "element_id"])
    changed_ids = set(merged.element_id) - same_ids
    # changed 에 대한 iou map (로그용으로 diff 반환에 포함)
    changed_iou = {}
    if not diff_rows.empty:
        for r in diff_rows[~diff_rows._promoted_same].itertuples(index=False):
            changed_iou[r.element_id] = float(r.iou)

    only_a_ids = set(tl_a.element_id) - set(tl_b.element_id)
    only_b_ids = set(tl_b.element_id) - set(tl_a.element_id)

    # same 결과: 양쪽 version 의 row 를 모두 포함
    same_a = _project_timeline(tl_a[tl_a.element_id.isin(same_ids)])
    same_b = _project_timeline(tl_b[tl_b.element_id.isin(same_ids)])
    same_df = pd.concat([same_a, same_b], ignore_index=True).sort_values(
        ["emdcd", "version_key"]
    ).reset_index(drop=True)

    # diff: changed 는 양쪽, only_* 는 한쪽
    diff_parts: list[pd.DataFrame] = []

    if changed_ids:
        ca = _project_timeline(tl_a[tl_a.element_id.isin(changed_ids)]).assign(status="changed")
        cb = _project_timeline(tl_b[tl_b.element_id.isin(changed_ids)]).assign(status="changed")
        ca["iou"] = ca.emdcd.map(changed_iou).fillna(0.0)
        cb["iou"] = cb.emdcd.map(changed_iou).fillna(0.0)
        diff_parts += [ca, cb]

    if only_a_ids:
        oa = _project_timeline(tl_a[tl_a.element_id.isin(only_a_ids)])
        oa = oa.assign(status="only_in_a", iou=float("nan"))
        diff_parts.append(oa)

    if only_b_ids:
        ob = _project_timeline(tl_b[tl_b.element_id.isin(only_b_ids)])
        ob = ob.assign(status="only_in_b", iou=float("nan"))
        diff_parts.append(ob)

    if diff_parts:
        diff_df = pd.concat(diff_parts, ignore_index=True)[_DIFF_COLS]
        diff_df = diff_df.sort_values(
            ["status", "emdcd", "version_key"]
        ).reset_index(drop=True)
    else:
        diff_df = pd.DataFrame(columns=_DIFF_COLS)

    return CompareResult(same_df, diff_df, va, vb, threshold)
