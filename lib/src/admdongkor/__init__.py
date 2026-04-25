"""admdongkor — 한국 행정경계(읍면동/시군구/시도) 1975–2026 시계열 지도 다운로더."""

from __future__ import annotations

import os as _os

from ._cache import cache_dir
from .api import (
    changelog,
    compare,
    data_version,
    find,
    get,
    get_list,
    match_adm,
    report_issue,
    versions,
)

__version__ = "0.6.1"
__all__ = [
    "get", "get_list", "versions", "find", "match_adm", "compare",
    "changelog", "data_version", "report_issue", "cache_dir", "__version__",
]

# import 시 인덱스 최신화. 네트워크 실패는 조용히 넘어감 —
# 실제 find()/match_adm() 호출에서 캐시 없으면 명시 에러가 뜬다.
# 끄려면: ADMDONGKOR_NO_AUTO_UPDATE=1
# 로컬 고정: ADMDONGKOR_DATA_DIR=/path/to/index
if _os.environ.get("ADMDONGKOR_NO_AUTO_UPDATE") != "1":
    try:
        from . import _cache as _c
        _c.ensure_latest(quiet=True)
    except Exception:
        pass
