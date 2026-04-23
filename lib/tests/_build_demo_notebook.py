"""demo.ipynb 를 생성하는 스크립트. NotebookEdit insert 버그 우회를 위해 JSON 직접 작성.

실행:
    python tests/_build_demo_notebook.py
"""

from __future__ import annotations

import json
from pathlib import Path


def code_cell(src: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src.splitlines(keepends=True),
    }


def md_cell(src: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": src.splitlines(keepends=True),
    }


CELLS = [
    md_cell(
        "# admdongkor — demo & manual verification\n"
        "\n"
        "이 노트북은 자동 pytest 가 못 잡는 부분(실제 네트워크 다운로드, 지도 시각, "
        "한글 표시 등)을 **사람 눈으로** 확인하는 용도. 셀을 위→아래 순서로 실행.\n"
    ),
    md_cell("## 0. 한글 폰트 설정 (matplotlib plot 용)\n"),
    code_cell(
        "import sys\n"
        "import matplotlib\n"
        "import matplotlib.pyplot as plt\n"
        "\n"
        "if sys.platform == 'win32':\n"
        "    matplotlib.rcParams['font.family'] = 'Malgun Gothic'\n"
        "elif sys.platform == 'darwin':\n"
        "    matplotlib.rcParams['font.family'] = 'AppleGothic'\n"
        "else:\n"
        "    # Linux: 'NanumGothic' 가 있으면 그걸로, 없으면 fallback\n"
        "    from matplotlib import font_manager\n"
        "    for cand in ['NanumGothic', 'Noto Sans CJK KR', 'UnDotum']:\n"
        "        if any(cand in f.name for f in font_manager.fontManager.ttflist):\n"
        "            matplotlib.rcParams['font.family'] = cand\n"
        "            break\n"
        "\n"
        "# 음수 기호(-)가 네모로 깨지는 것 방지\n"
        "matplotlib.rcParams['axes.unicode_minus'] = False\n"
        "print(f'matplotlib font: {matplotlib.rcParams[\"font.family\"]}')\n"
    ),
    md_cell("## 1. 버전 & 캐시 위치 확인\n"),
    code_cell(
        "import admdongkor as adk\n"
        "\n"
        "print('version     :', adk.__version__)\n"
        "print('cache dir   :', adk.cache_dir())\n"
        "print('total keys  :', len(adk.get_list()))\n"
        "print('first 3     :', adk.get_list()[:3])\n"
        "print('last 3      :', adk.get_list()[-3:])\n"
        "print('2025 keys   :', adk.get_list(year=2025))\n"
    ),
    md_cell("## 2. `find()` — 행정구역명으로 버전 검색\n"),
    code_cell(
        "# 단일 토큰: 모든 레벨 substring\n"
        "df = adk.find('종로')\n"
        "print(f'rows: {len(df)}, versions: {df.version_key.nunique()}')\n"
        "df.head(10)\n"
    ),
    code_cell(
        "# 2 토큰: 자동으로 sgg 만 — 종로구 안의 읍면동은 줄줄이 안 나옴\n"
        "adk.find('서울특별시 종로구').head()\n"
    ),
    code_cell(
        "# 3 토큰: 자동으로 emd 만\n"
        "adk.find('서울특별시 종로구 사직동').head()\n"
    ),
    code_cell(
        "# 공백 무시 매칭: sggnm 이 '수원시권선구' 로 붙어 저장돼 있어도 매치\n"
        "adk.find('수원시 권선구').head()\n"
    ),
    code_cell(
        "# emd 행에는 code7 (통계청 7자리) 과 code8 이 같이 나옴\n"
        "# 2023-10-01 통계청 7→8자리 전환 전후 비교\n"
        "df = adk.find('사직동', year=[2023])\n"
        "df[['version_key', 'code', 'code7', 'code8']]\n"
    ),
    code_cell(
        "# 완전 일치 (단일 토큰만)\n"
        "adk.find('종로구', exact=True).head()\n"
    ),
    code_cell(
        "# level 명시로 자동 필터 override (종로구 내 모든 읍면동)\n"
        "df = adk.find('서울특별시 종로구', level='emd')\n"
        "print(f'rows: {len(df)}')\n"
        "df.head()\n"
    ),
    code_cell(
        "# 연도 범위\n"
        "adk.find('세종', year=[2010, 2012])\n"
    ),
    code_cell(
        "# 체이닝 메서드 — 버전 키를 바로 뽑기\n"
        "# 여주군은 2013-09-23 여주시로 승격됐으니 first~last 가 1975~2012 여야 정상\n"
        "r = adk.find('여주군')\n"
        "print('versions():', r.versions())\n"
        "print('first()  :', r.first())\n"
        "print('last()   :', r.last())\n"
    ),
    md_cell("## 3. `get()` — 지도 다운로드 & 첫 그림\n"
            "\n"
            "기본은 **light** (단순화, 약 0.5–2.4MB). 반환 CRS 는 항상 EPSG:5179 — "
            "`detail=True` 로 원본 해상도도 로드 가능. light 저장 포맷은 4326 이지만 "
            "파이썬 get() 은 자동으로 5179 로 재투영해 반환.\n"),
    code_cell(
        "import time\n"
        "\n"
        "t0 = time.time()\n"
        "sido = adk.get('20250401', 'sido')          # light 기본\n"
        "t_first = time.time() - t0\n"
        "\n"
        "t0 = time.time()\n"
        "sido2 = adk.get('20250401', 'sido')\n"
        "t_cache = time.time() - t0\n"
        "\n"
        "print(f'first download : {t_first:.2f}s')\n"
        "print(f'cache hit      : {t_cache:.3f}s ({t_first/max(t_cache, 0.001):.0f}x faster)')\n"
        "print(f'CRS            : EPSG:{sido.crs.to_epsg()}  (기본 = 5179)')\n"
        "print(f'rows           : {len(sido)}')\n"
        "sido.head()\n"
    ),
    code_cell(
        "# detail=True 로 원본 해상도 로드. 둘 다 반환 CRS 는 5179.\n"
        "sido_full = adk.get('20250401', 'sido', detail=True)\n"
        "print(f'light  rows={len(sido)},      crs=EPSG:{sido.crs.to_epsg()}')\n"
        "print(f'detail rows={len(sido_full)}, crs=EPSG:{sido_full.crs.to_epsg()}')\n"
        "\n"
        "# 다른 CRS 원하면 crs= 로\n"
        "sido_wgs = adk.get('20250401', 'sido', crs='EPSG:4326')\n"
        "print(f'crs=4326      : EPSG:{sido_wgs.crs.to_epsg()}')\n"
    ),
    code_cell(
        "# 지도 그림 (한국 모양이 나와야 정상)\n"
        "ax = sido.plot(figsize=(8, 10), edgecolor='black', linewidth=0.3)\n"
        "ax.set_title('시도 — 20250401')\n"
        "ax.set_axis_off()\n"
    ),
    md_cell("## 4. 읍면동 지도 — 가장 큰 파일\n"),
    code_cell(
        "emd = adk.get('20250401', 'emd')\n"
        "print(f'rows: {len(emd)}, cols: {list(emd.columns)}')\n"
        "emd.head(3)\n"
    ),
    code_cell(
        "# 서울만 뽑아 읍면동 경계 찍기 (전국 emd 3500개 plot 하면 버벅임)\n"
        "seoul = emd[emd.sidocd == '11']\n"
        "ax = seoul.plot(figsize=(10, 10), edgecolor='grey', linewidth=0.3, facecolor='none')\n"
        "ax.set_title(f'서울 읍면동 — 20250401 ({len(seoul)} emds)')\n"
        "ax.set_axis_off()\n"
    ),
    md_cell("## 5. force_refresh 로 캐시 갱신\n"
            "\n"
            "light 파일은 `cache_dir()/simplified/` 아래, detail 파일은 그 위 `cache_dir()/` 에 저장됨.\n"),
    code_cell(
        "f = adk.cache_dir() / 'simplified' / 'sido_20250401_light.parquet'\n"
        "before = f.stat().st_mtime\n"
        "adk.get('20250401', 'sido', force_refresh=True)\n"
        "after = f.stat().st_mtime\n"
        "print(f'mtime before : {before}')\n"
        "print(f'mtime after  : {after}')\n"
        "print(f'updated      : {after > before}')\n"
    ),
    md_cell("## 6. 캐시 폴더 현황\n"),
    code_cell(
        "files = sorted(adk.cache_dir().rglob('*.parquet'))\n"
        "total = sum(f.stat().st_size for f in files)\n"
        "print(f'cache dir : {adk.cache_dir()}')\n"
        "print(f'files     : {len(files)}')\n"
        "print(f'total     : {total / 1024 / 1024:.1f} MB')\n"
        "for f in files:\n"
        "    rel = f.relative_to(adk.cache_dir())\n"
        "    print(f'  {f.stat().st_size / 1024 / 1024:6.2f} MB  {rel}')\n"
    ),
    md_cell("## 7. 시계열 — 같은 지역을 여러 해 비교\n"),
    code_cell(
        "# 세종시가 들어간 연도들의 sido 지도. 2012 이전엔 충청남도로 포함됨\n"
        "import matplotlib.pyplot as plt\n"
        "\n"
        "keys = ['20111231', '20121231', '20131231', '20181106']\n"
        "fig, axes = plt.subplots(1, len(keys), figsize=(16, 6))\n"
        "for ax, key in zip(axes, keys):\n"
        "    g = adk.get(key, 'sido')\n"
        "    g.plot(ax=ax, edgecolor='black', linewidth=0.3, facecolor='lightgrey')\n"
        "    sejong = g[g.sidonm.astype(str).str.contains('세종', na=False)]\n"
        "    if not sejong.empty:\n"
        "        sejong.plot(ax=ax, facecolor='tomato', edgecolor='darkred')\n"
        "    ax.set_title(key)\n"
        "    ax.set_axis_off()\n"
        "plt.suptitle('시도 경계 — 세종시 등장 시점 비교')\n"
        "plt.tight_layout()\n"
    ),
]


def main() -> Path:
    nb = {
        "cells": CELLS,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.12",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    out = Path(__file__).parent / "demo.ipynb"
    out.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    print(f"wrote {main()}")
