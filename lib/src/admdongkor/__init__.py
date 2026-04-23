"""admdongkor — 한국 행정경계(읍면동/시군구/시도) 1975–2026 시계열 지도 다운로더."""

from __future__ import annotations

from ._cache import cache_dir
from .api import compare, find, get, get_list, match_adm, versions

__version__ = "0.5.3"
__all__ = ["get", "get_list", "versions", "find", "match_adm", "compare",
           "cache_dir", "__version__"]
