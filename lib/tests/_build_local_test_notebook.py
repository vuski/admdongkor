"""업로드 전 로컬 검증 노트북 (local_test.ipynb) 생성 스크립트.

이 노트북은 **PyPI 업로드 전에** 라이브러리 전 기능을 로컬에서 돌려보는 용도.

동작 방식:
    - emd/sgg/sido parquet 은 이미 리포 master 브랜치에 올라가 있으므로 원격에서 정상 다운로드
    - `_index.parquet` 만 아직 push 안 됐을 수 있으니, 노트북 첫 셀에서 리포의 parquet/
      폴더에 있는 것을 **캐시 폴더로 복사**해서 시드
    - 그래서 `adk.find()` 는 로컬 시드된 인덱스로, `adk.get()` 은 원격 다운로드로 동작

local_test.ipynb 는 .gitignore 에 있어 레포에 올라가지 않음. 생성 스크립트만 올림.

실행:
    python tests/_build_local_test_notebook.py
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
        "# admdongkor — 업로드 전 로컬 검증 노트북\n"
        "\n"
        "이 노트북은 **PyPI 에 올리기 직전** 전 기능을 로컬에서 돌려보는 용도.\n"
        "\n"
        "- `_index.parquet` + timeline/shape_pairs → **패키지에 embed** (네트워크 0)\n"
        "- `emd/sgg/sido` 지도 parquet → 원격 다운로드 (GitHub raw)\n"
        "\n"
        "이 파일은 `.gitignore` 에 올라 git 에 추적되지 않음. 재생성은\n"
        "`python tests/_build_local_test_notebook.py`.\n"
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
        "    from matplotlib import font_manager\n"
        "    for cand in ['NanumGothic', 'Noto Sans CJK KR', 'UnDotum']:\n"
        "        if any(cand in f.name for f in font_manager.fontManager.ttflist):\n"
        "            matplotlib.rcParams['font.family'] = cand\n"
        "            break\n"
        "\n"
        "matplotlib.rcParams['axes.unicode_minus'] = False\n"
        "print(f'matplotlib font: {matplotlib.rcParams[\"font.family\"]}')\n"
        "\n"
        "import admdongkor as adk\n"
    ),
    md_cell("## 1. 버전 & 캐시 위치\n"),
    code_cell(
        "print('version     :', adk.__version__)\n"
        "print('cache dir   :', adk.cache_dir())\n"
        "print('total keys  :', len(adk.get_list()))\n"
        "print('first 3     :', adk.get_list()[:3])\n"
        "print('last 3      :', adk.get_list()[-3:])\n"
        "print('2025 keys   :', adk.get_list(year=2025))\n"
    ),
    md_cell("## 2. `find()` — 로컬 시드된 인덱스로 검색\n"),
    code_cell(
        "# 단일 토큰 — 모든 레벨 substring\n"
        "df = adk.find('종로')\n"
        "print(f'rows: {len(df)}, versions: {df.version_key.nunique()}, levels: {df.level.value_counts().to_dict()}')\n"
        "df.head(10)\n"
    ),
    code_cell(
        "# 2 토큰 — 자동으로 sgg. 종로구 안 읍면동 안 나옴\n"
        "adk.find('서울특별시 종로구').head()\n"
    ),
    code_cell(
        "# 3 토큰 — 자동으로 emd\n"
        "adk.find('서울특별시 종로구 사직동').head()\n"
    ),
    code_cell(
        "# 공백 무시 매칭 — '수원시권선구' 한덩어리와 매치\n"
        "adk.find('수원시 권선구').head()\n"
    ),
    code_cell(
        "# emd 반환에 code7/code8 + 상위 sgg/sido 맥락\n"
        "adk.find('사직동', year=[2023])[['version_key','sidonm','sggnm','name','code','code7','code8']]\n"
    ),
    code_cell(
        "# 완전 일치 (단일 토큰만)\n"
        "adk.find('종로구', exact=True).head()\n"
    ),
    code_cell(
        "# 연도 범위\n"
        "adk.find('세종', year=[2010, 2015])\n"
    ),
    code_cell(
        "# 체이닝 메서드\n"
        "r = adk.find('여주군')\n"
        "print('versions():', r.versions())\n"
        "print('first()  :', r.first())\n"
        "print('last()   :', r.last())\n"
    ),
    code_cell(
        "# 에러 케이스들 — 전부 ValueError 떠야 정상\n"
        "for query, kwargs in [\n"
        "    ('종로', {'year': [2000, 2005, 2010]}),\n"
        "    ('', {}),\n"
        "    ('a b c d', {}),\n"
        "    ('서울 종로', {'exact': True}),\n"
        "]:\n"
        "    try:\n"
        "        adk.find(query, **kwargs)\n"
        "        print(f'FAIL: {query!r} {kwargs}')\n"
        "    except ValueError as e:\n"
        "        print(f'OK: {query!r} {kwargs} -> {e}')\n"
    ),
    md_cell("## 3. `get()` — 원격 다운로드\n"),
    code_cell(
        "import time\n"
        "\n"
        "# sido 작은 파일로 첫 다운로드 + 캐시 히트 측정\n"
        "# (이미 캐시되어 있으면 첫 번째도 빠름. 지우고 다시 재기)\n"
        "target = adk.cache_dir() / 'simplified' / 'sido_20250401_light.parquet'\n"
        "if target.exists():\n"
        "    target.unlink()\n"
        "\n"
        "t0 = time.time()\n"
        "sido = adk.get('20250401', 'sido')\n"
        "t_first = time.time() - t0\n"
        "\n"
        "t0 = time.time()\n"
        "sido2 = adk.get('20250401', 'sido')\n"
        "t_cache = time.time() - t0\n"
        "\n"
        "print(f'first download : {t_first:.2f}s')\n"
        "print(f'cache hit      : {t_cache:.3f}s ({t_first/max(t_cache, 0.001):.0f}x faster)')\n"
        "print(f'CRS            : EPSG:{sido.crs.to_epsg()}')\n"
        "print(f'rows           : {len(sido)}')\n"
        "sido.head()\n"
    ),
    code_cell(
        "ax = sido.plot(figsize=(8, 10), edgecolor='black', linewidth=0.3)\n"
        "ax.set_title('시도 — 20250401 (원격 다운로드)')\n"
        "ax.set_axis_off()\n"
    ),
    md_cell("## 4. 읍면동 — 대용량 원격 다운로드\n"),
    code_cell(
        "emd = adk.get('20250401', 'emd')\n"
        "print(f'rows: {len(emd)}, cols: {list(emd.columns)}')\n"
        "emd.head(3)\n"
    ),
    code_cell(
        "seoul = emd[emd.sidocd == '11']\n"
        "ax = seoul.plot(figsize=(10, 10), edgecolor='grey', linewidth=0.3, facecolor='none')\n"
        "ax.set_title(f'서울 읍면동 — 20250401 ({len(seoul)} emds)')\n"
        "ax.set_axis_off()\n"
    ),
    md_cell("## 5. `force_refresh=True` — 원격에서 다시 받기\n"),
    code_cell(
        "f = adk.cache_dir() / 'simplified' / 'sido_20250401_light.parquet'\n"
        "before = f.stat().st_mtime\n"
        "adk.get('20250401', 'sido', force_refresh=True)\n"
        "after = f.stat().st_mtime\n"
        "print(f'mtime before : {before}')\n"
        "print(f'mtime after  : {after}')\n"
        "print(f'updated      : {after > before}')\n"
    ),
    md_cell("## 6. 캐시 현황 + 총 용량\n"),
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
    md_cell(
        "## 7. 시계열 시각 — 세종시 등장 전후\n"
        "\n"
        "2012-07-01 세종특별자치시 신설 전후 시도 경계 비교. (붉은색 = 세종)\n"
    ),
    code_cell(
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
    md_cell(
        "## 8. `match_adm()` — 영역 기반 시계열 매칭\n"
        "\n"
        "base 시점 region 영역에 걸치는 target 시점 읍면동 + weight 반환.\n"
        "핵심 검증 케이스: **2025 대구광역시 영역 → 2011 구성 읍면동**.\n"
        "2023년 군위군이 대구로 편입됐으므로, 2011 기준으로 보면 **경북 군위군 8개 읍면** 이\n"
        "현재 대구 영역 안에 들어가 있어야 함.\n"
    ),
    code_cell(
        "# 2025 대구(sidocd=27) 영역을 2011 시점으로 역매칭\n"
        "r = adk.match_adm(base='20251231', region='27', target='20111231')\n"
        "print(f'type: {type(r).__name__}, rows: {len(r)}')\n"
        "print(f'columns: {list(r.columns)}')\n"
        "r.head(10)\n"
    ),
    code_cell(
        "# 경북 군위군(sggcd=47720) 행들만 확인 — 8개 읍면 전부 weight ~1.0 이어야\n"
        "gunwi = r[r.sggcd == '47720']\n"
        "print(f'군위군 읍면 매칭: {len(gunwi)}개')\n"
        "gunwi[['emdcd', 'emdnm', 'sggnm', 'sidonm', 'weight']]\n"
    ),
    code_cell(
        "# sgg 단위 집계 — 대구 내 모든 sgg + 군위군\n"
        "r.sgg()\n"
    ),
    code_cell(
        "# sido 단위 집계 — 대구 ~100% + 경북 소수 (= 군위 면적 / 경북 전체)\n"
        "r.sido()\n"
    ),
    code_cell(
        "# 시각화: 2011 시점 지도에 매칭된 emd 영역 표시\n"
        "# (원격 다운로드 필요. emd_20111231.parquet 이 크니 시간 걸릴 수 있음)\n"
        "emd_2011 = adk.get('20111231', 'emd')\n"
        "sido_2011 = adk.get('20111231', 'sido')\n"
        "matched = emd_2011[emd_2011.emdcd.isin(r.emdcd)].copy()\n"
        "matched = matched.merge(r[['emdcd','weight']], on='emdcd')\n"
        "\n"
        "fig, ax = plt.subplots(figsize=(10, 11))\n"
        "sido_2011.plot(ax=ax, edgecolor='black', linewidth=0.4, facecolor='lightgrey')\n"
        "matched.plot(ax=ax, column='weight', cmap='Reds',\n"
        "             edgecolor='darkred', linewidth=0.3,\n"
        "             legend=True, vmin=0, vmax=1, alpha=0.8)\n"
        "# 경북 군위군 하이라이트 (파란 테두리)\n"
        "gunwi_geom = matched[matched.sggcd == '47720']\n"
        "gunwi_geom.plot(ax=ax, facecolor='none', edgecolor='navy', linewidth=1.5)\n"
        "ax.set_title('2011 읍면동 중 2025 대구 영역에 속하는 것\\n(파란 테두리 = 경북 군위군, 2023 대구 편입 예정)')\n"
        "ax.set_axis_off()\n"
    ),
    code_cell(
        "# 역방향: 2011 경북 군위군(sggcd=47720) → 2025 어느 영역으로 갔나\n"
        "r2 = adk.match_adm(base='20111231', region='47720', target='20251231')\n"
        "print(f'2025 매칭 emd: {len(r2)}개')\n"
        "r2.head(10)\n"
    ),
    code_cell(
        "# 군위군은 2025 에 대구 군위군(sggcd=27720) 으로 그대로 승계 → sggnm 이 '군위군' 인 emd 들이 나와야\n"
        "print('sgg 집계:')\n"
        "r2.sgg()\n"
    ),
    code_cell(
        "# 여러 target 시점을 한 번에 — 2011 vs 2020 vs 2024\n"
        "r3 = adk.match_adm(\n"
        "    base='20251231', region='27',\n"
        "    target=['20111231', '20201001', '20241231'],\n"
        ")\n"
        "print('target 별 행 수:')\n"
        "print(r3.groupby('version_key').size())\n"
        "print()\n"
        "print('target 별 sido 집계:')\n"
        "r3.sido()\n"
    ),
    code_cell(
        "# min_weight 필터 효과\n"
        "print(f'필터 없음         : {len(r):>4} rows, 최소 weight = {r.weight.min():.4f}')\n"
        "for th in (0.01, 0.1, 0.5, 0.9):\n"
        "    rf = adk.match_adm(base='20251231', region='27', target='20111231', min_weight=th)\n"
        "    print(f'min_weight={th}  : {len(rf):>4} rows')\n"
    ),
    code_cell(
        "# emd 단일 코드 쿼리 — 대구 동구 한 동의 시계열\n"
        "# 2014년에 통합동 편성 변화가 있었는지 확인\n"
        "r4 = adk.match_adm(\n"
        "    base='20251231', region='2714076000',\n"
        "    target=['20111231', '20141231', '20201001'],\n"
        ")\n"
        "print(f'rows: {len(r4)}')\n"
        "r4\n"
    ),
    md_cell(
        "## 9. 검증 체크리스트\n"
        "\n"
        "- [ ] §1: 61 개 버전 / 2025 5개 나옴\n"
        "- [ ] §2: 종로 검색 결과 지역명이 한글로 제대로 표시\n"
        "- [ ] §2: ValueError 메시지 한글 잘 보임\n"
        "- [ ] §3: 첫 다운로드 `.parquet` 캐시 히트보다 확연히 느림 (수십 배)\n"
        "- [ ] §3 지도: 대한민국 모양 17개 광역시도 보임\n"
        "- [ ] §4 서울 지도: 읍면동 경계 촘촘히\n"
        "- [ ] §5: mtime updated True\n"
        "- [ ] §7 시계열: 2012-12-31 부터 세종(빨강) 등장, 2011 엔 없음\n"
        "- [ ] §8 match_adm: 2025 대구 → 2011 매칭에 **경북 군위군 8개 읍면** 포함\n"
        "- [ ] §8 match_adm 지도: 대구 + 군위군(파란 테두리) 영역이 붉게 칠해짐\n"
        "- [ ] §8 sgg/sido 집계가 이치에 맞음 (대구 ≈1.0, 경북 소수)\n"
        "- [ ] §8 역방향 쿼리(2011 군위 → 2025) 도 매칭 나옴\n"
        "\n"
        "전부 체크되면 PyPI 업로드 준비 완료.\n"
    ),
]


def _annotate_cell_numbers(cells: list[dict]) -> list[dict]:
    """각 셀 맨 앞에 '# cell N' (code) 또는 '<!-- cell N -->' (markdown) 주석 삽입."""
    out = []
    for i, c in enumerate(cells, start=1):
        new = {k: v for k, v in c.items() if k != "source"}
        if c["cell_type"] == "code":
            marker = f"# ── cell {i} ──\n"
        else:
            marker = f"<!-- cell {i} -->\n"
        new["source"] = [marker] + list(c["source"])
        out.append(new)
    return out


def main() -> Path:
    nb = {
        "cells": _annotate_cell_numbers(CELLS),
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
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
    out = Path(__file__).parent / "local_test.ipynb"
    out.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    print(f"wrote {main()}")
