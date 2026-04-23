"""parquet name 필드(sidonm/sggnm/emdnm) 내부 whitespace 제거.

GeoParquet 메타(geo, pandas)와 컬럼 타입(large_string, binary)을 보존한 채
pyarrow compute 로 문자열 치환만 수행. geometry 바이트는 건드리지 않음.

실행 후 자동 검증:
  - row 수 동일
  - geom(또는 geometry) 컬럼 바이트 완전히 동일
  - schema metadata(geo/pandas) 동일
  - 타겟 필드에 \\s 가 더 이상 남아있지 않음
"""
from __future__ import annotations
import glob
import re
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

TARGET_COLS = ("sidonm", "sggnm", "emdnm")
WS_REGEX = r"\s+"  # space / tab / \r / \n ...

_ws = re.compile(r"\s")


def strip_ws_in_file(path: str) -> dict:
    """1개 파일 처리. 반환: 변경 통계 dict."""
    table = pq.read_table(path)
    orig_meta = table.schema.metadata  # dict (or None)
    orig_num_rows = table.num_rows

    # geom/geometry 원본 바이트 스냅샷 (검증용)
    geom_col_name = "geom" if "geom" in table.column_names else (
        "geometry" if "geometry" in table.column_names else None
    )
    geom_orig = table.column(geom_col_name).combine_chunks() if geom_col_name else None

    per_col_changes: dict[str, int] = {}

    for col in TARGET_COLS:
        if col not in table.column_names:
            continue
        idx = table.schema.get_field_index(col)
        field = table.schema.field(idx)  # 타입(large_string) 유지용
        arr = table.column(col).combine_chunks()

        # 치환
        cleaned = pc.replace_substring_regex(arr, pattern=WS_REGEX, replacement="")

        # 타입 보존: large_string -> large_string 유지
        if cleaned.type != field.type:
            cleaned = cleaned.cast(field.type)

        # 변경된 행 수 카운트 (null 고려)
        # diff = (orig != cleaned) & orig.is_valid()
        eq = pc.equal(arr, cleaned)
        # eq 가 null 인 위치는 양쪽 다 null -> 변경 아님
        not_eq = pc.fill_null(pc.invert(eq), False)
        n_changed = int(pc.sum(pc.cast(not_eq, pa.int64())).as_py() or 0)

        if n_changed > 0:
            per_col_changes[col] = n_changed
            # set_column 은 field 객체를 받아 타입/이름 유지
            table = table.set_column(idx, field, cleaned)

    if not per_col_changes:
        return {"path": path, "changed": False}

    # schema metadata 명시적으로 주입 (set_column 이 보존해도 안전장치)
    if orig_meta is not None:
        new_schema = table.schema.with_metadata(orig_meta)
        table = table.replace_schema_metadata(orig_meta)
    else:
        new_schema = table.schema

    # 쓰기: 원본과 동일 (1 row group, SNAPPY)
    pq.write_table(
        table,
        path,
        compression="snappy",
        row_group_size=max(orig_num_rows, 1),  # 전체를 1 row group 으로
    )

    # ----- 검증 -----
    verified = pq.read_table(path)
    errs: list[str] = []
    if verified.num_rows != orig_num_rows:
        errs.append(f"row count mismatch {verified.num_rows} != {orig_num_rows}")

    # geom 바이트 동일성
    if geom_col_name is not None:
        geom_after = verified.column(geom_col_name).combine_chunks()
        if not geom_orig.equals(geom_after):
            errs.append(f"{geom_col_name} bytes changed!")

    # geo meta 보존
    after_meta = verified.schema.metadata or {}
    if orig_meta:
        for k in (b"geo", b"pandas"):
            if k in orig_meta and orig_meta[k] != after_meta.get(k):
                errs.append(f"metadata[{k!r}] changed or lost")

    # 타겟 컬럼 공백 잔여 검증
    for col in TARGET_COLS:
        if col not in verified.column_names:
            continue
        arr = verified.column(col).combine_chunks().to_pylist()
        for v in arr:
            if v is not None and _ws.search(v):
                errs.append(f"{col} still has whitespace: {v!r}")
                break

    return {
        "path": path,
        "changed": True,
        "per_col": per_col_changes,
        "errors": errs,
    }


def main() -> int:
    paths = sorted(glob.glob("parquet/*.parquet")) + sorted(
        glob.glob("parquet/simplified/*.parquet")
    )
    paths = [p for p in paths if "_index" not in p]

    total = 0
    changed = 0
    any_errors = 0
    for p in paths:
        res = strip_ws_in_file(p)
        total += 1
        if res.get("changed"):
            changed += 1
            errs = res.get("errors", [])
            status = "OK" if not errs else "ERR"
            print(f"[{status}] {p}  {res['per_col']}")
            if errs:
                any_errors += 1
                for e in errs:
                    print(f"       - {e}")

    print(f"\nscanned={total}  changed={changed}  files_with_errors={any_errors}")
    return 0 if any_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
