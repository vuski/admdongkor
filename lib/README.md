# admdongkor

한국 행정경계(읍면동/시군구/시도) **1975–2026 시계열** 지도 다운로더 + **영역 기반 시계열 매칭**.

- 62 개 버전의 `GeoDataFrame` 다운로드 (기본 **단순화 light 버전**, `detail=True` 로 원본 해상도. 반환 CRS 는 항상 EPSG:5179)
- 이름·코드로 **버전 검색** (`find`)
- **시점 간 경계 매칭** (`match_adm`) — "2025 대구 영역 ≡ 2011 기준 어느 emd들?"
- **두 시점 diff** (`compare`) — 경계 바뀐 emd / 소멸 / 신설

```bash
pip install admdongkor
```

검색 인덱스·시계열 매칭 인덱스는 **설치 후 첫 `import` 시 자동 다운로드** 되어
사용자 캐시 디렉토리에 저장됩니다 (약 5.8MB). 이후 import 할 때마다 GitHub
manifest 와 비교해 **필요시에만 갱신** — 데이터 수정(예: 과거 시군구 이름
정정)이 있을 때 `pip install -U` 없이도 최신 인덱스가 반영됩니다.

| 환경변수                         | 용도                                                        |
| -------------------------------- | ----------------------------------------------------------- |
| `ADMDONGKOR_NO_AUTO_UPDATE=1`    | import 시 자동 갱신 끔 (CI / 재현성)                         |
| `ADMDONGKOR_DATA_DIR=/path/...`  | 로컬 인덱스 경로 고정 (네트워크 우회, 오프라인 배포용)          |
| `ADMDONGKOR_CACHE_DIR=/path/...` | 캐시 위치 override (기본: OS 관례 폴더)                     |

> **스키마 영속성**: 인덱스 파일명에 붙은 `v3` 는 스키마 버전 태그입니다. 구 파일 (`_index.parquet`, `timeline_v3_*`, `shape_pairs_v3_*`) 은 **영구 유지**되며, 스키마 변경이 있을 때는 `v4_*` 처럼 **새 파일명으로 병렬 추가**됩니다. 따라서 배포된 라이브러리는 설치 시점의 스키마를 그대로 계속 쓸 수 있고, 데이터 내용 수정(이름 정정 등)만 자동 반영됩니다.

---

## 빠른 시작

```python
import admdongkor as adk

# 1. 어떤 버전이 있는지
adk.versions()                # 62개 전체 (list 서브클래스)
adk.versions().head()         # 처음 5개
adk.versions().tail()         # 최근 5개
adk.versions(2025)            # 해당 연도만

# 2. 지명으로 버전 찾기
adk.find("종로")                          # 모든 레벨 substring
adk.find("서울특별시 종로구")              # 자동으로 sgg
adk.find("서울특별시 종로구 사직동")         # 자동으로 emd
adk.find("수원시 권선구")                   # "수원시권선구" 도 매치

# 3. 지도 로드 (반환은 항상 EPSG:5179)
adk.get("20250401", "emd")                    # light (약 2.4MB)
adk.get("20250401", "emd", detail=True)       # 원본 해상도 (약 11MB)
adk.get("20250401", "emd", crs="EPSG:4326")   # WGS84 로 재투영

# 4. 영역 시계열 매칭 — "2025 대구 영역 → 2011 구성 읍면동"
r = adk.match_adm(base="20251231", region="27", target="20111231")
r              # emd 단위 + weight
r.sgg()        # sgg 단위 집계
r.sido()       # sido 단위 집계

# 5. 두 시점 diff
c = adk.compare(["20251231", "20111231"])
c.same()       # 경계 유지된 emd
c.diff()       # 경계 바뀐 / 소멸 / 신설
```

---

## API

### `versions(year: int | None = None) -> VersionList`

버전 키 목록. `list` 서브클래스라 인덱싱·슬라이스 그대로 되고, 편의 메서드 `.head()` / `.tail()` 추가.

```python
adk.versions()            # ['19751231', ..., '20260201']
adk.versions().head()     # 처음 5개
adk.versions().tail(3)    # 마지막 3개
adk.versions(2023)        # ['20230101', '20230401', ...]
```

`get_list()` 는 동일 기능의 plain `list[str]` 반환 버전 (호환성 유지).

### `find(name, level=None, exact=False, year=None) -> FindResult`

행정구역명 substring 검색 (NFC 정규화, 대소문자·공백 무시).

**계층 검색** — 공백 토큰 수에 따라 자동 level 필터:

| 쿼리                               | 자동 level | 설명                   |
| ---------------------------------- | ---------- | ---------------------- |
| `find("종로")`                     | 전체       | 모든 레벨 substring    |
| `find("서울특별시 종로구")`        | `sgg`      | 해당 시도의 그 sgg 만  |
| `find("서울특별시 종로구 사직동")` | `emd`      | 그 emd 만              |
| `find("수원시 권선구")`            | `sgg`      | "수원시권선구" 도 매치 |

`level=` 명시 시 자동보다 우선 (`find("서울특별시 종로구", level="emd")` → 종로구 내 emd 전체).
`exact=True` 는 단일 토큰 전용, `name` 단독 완전일치.
`year=[2025]` 단일 / `year=[2000, 2005]` 범위 (inclusive).

**반환 컬럼:**
`version_key, level, sidonm, sggnm, name, code, code7, code8, sggcd, sidocd`

`FindResult` (pd.DataFrame 서브클래스) 체이닝:

```python
adk.find("여주군").versions()   # 매치된 고유 version_key 리스트
adk.find("여주군").first()      # '19751231'
adk.find("여주군").last()       # '20121231'
```

### `get(key, level="emd", *, detail=False, crs=None, force_refresh=False) -> GeoDataFrame`

특정 버전의 지도. 첫 호출 시 GitHub raw 에서 다운로드해 로컬 캐시.

| 인자            | 설명                                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `key`           | 버전 키 문자열. `adk.versions()` 참조                                                                                    |
| `level`         | `"emd"` / `"sgg"` / `"sido"`                                                                                             |
| `detail`        | `False` (기본) = **light** (mapshaper 18.7% 단순화, 약 0.5–2.4MB). `True` = **원본** (약 11MB/emd). 반환 CRS 는 둘 다 EPSG:5179 |
| `crs`           | 재투영 대상. `None` (기본) 이면 EPSG:5179. 문자열/int 모두 허용                                                       |
| `force_refresh` | 캐시 무시 재다운로드                                                                                                     |

```python
# 기본 (light, EPSG:5179) — 가볍게 지도 받아 면적·거리 계산까지 바로
adk.get("20250401", "emd")                     # ≈2.4MB
adk.get("20250401", "sgg")                     # ≈1MB
adk.get("20250401", "sido")                    # ≈0.5MB

# 원본 해상도
adk.get("20250401", "emd", detail=True)        # ≈11MB, EPSG:5179

# 다른 CRS 로 재투영
adk.get("20250401", "sido", crs="EPSG:4326")   # WGS84
adk.get("20250401", "sido", crs="EPSG:3857")   # Web Mercator
```

> **light 파일은 `parquet/simplified/{level}_{key}_light.parquet`** 으로 배포됩니다. 저장 포맷 CRS 는 EPSG:4326 (웹지도 호환용) 이지만 파이썬 `get()` 은 기본값으로 **EPSG:5179 로 재투영해 반환** — 원본과 동일한 기준이라 면적·거리·버퍼 계산이 그대로 동작합니다. npm/JS 쪽에서는 파일을 직접 읽어 4326 으로 바로 사용 가능.
>
> light 의 sgg/sido 는 단순화된 emd 를 dissolve 로 생성하여 **레벨 간 경계가 정확히 맞물립니다**. 1975/1980/1985 버전은 원본에 sgg/sido 코드가 없어 이름 기반 dissolve — 해당 sgg/sido 행의 `sggcd`/`sidocd` 는 `<NA>`.

### `match_adm(*, base, region, target, min_weight=0.0) -> MatchResult`

**영역 기반 시계열 매칭**. `base` 시점 `region` 경계를 기준으로, `target` 시점에서
그 영역에 걸치는 emd + weight 반환.

| 인자         | 설명                                                                                |
| ------------ | ----------------------------------------------------------------------------------- |
| `base`       | 기준 버전 키                                                                        |
| `region`     | 코드. **2자리**=sido / **5자리**=sgg / **7자리**=통계청 emd / **10자리**=행안부 emd |
| `target`     | 버전 키 하나 또는 리스트                                                            |
| `min_weight` | 이 값 미만 weight 제외. 기본 0 (필터 없음)                                          |

`weight` 의미: `area(target_emd ∩ base_region) / area(target_emd)` — "target emd 의 몇 %가 base 영역에 속하는가". 인구 집계용.

```python
# 2025 대구 영역 → 2011 구성 읍면동
r = adk.match_adm(base="20251231", region="27", target="20111231")
# → 경북 군위군 8개 읍면 + 당시 대구 전체 emd, 각각 weight
```

**레벨 변환 메서드** (면적가중 평균):

```python
r.emd()    # 기본 (DataFrame)
r.sgg()    # sgg 단위 — "이 sgg 의 몇 %가 base 영역에"
r.sido()   # sido 단위
```

**반환 컬럼:** `version_key, emdcd, emdnm, sggcd, sggnm, sidocd, sidonm, area, weight`

### `compare(versions: list[str], threshold=0.99) -> CompareResult`

두 시점 emd 비교. `emdcd` 기준으로:

- `.same()` — 공간 동일 (IoU ≥ threshold). 각 emdcd 당 2 rows
- `.diff()` — 변화 있는 emd, `status` 컬럼:
  - `changed` — 두 시점 모두 존재, 경계 달라짐
  - `only_in_a` — 첫 번째 버전에만 존재
  - `only_in_b` — 두 번째 버전에만 존재

```python
c = adk.compare(["20251231", "20111231"])
c.same().head()
c.diff()[c.diff().status == "changed"].nsmallest(5, "iou")  # 가장 많이 변한 emd top 5
```

### `cache_dir() -> Path`

지도 parquet 캐시 폴더. OS 별 자동 결정, `ADMDONGKOR_CACHE_DIR` 환경변수로 override.
인덱스 parquet 은 이 폴더의 `index/` 서브디렉토리에 저장됨.

- Windows: `%LOCALAPPDATA%\admdongkor\`
- macOS/Linux: `$XDG_CACHE_HOME/admdongkor/` 또는 `~/.cache/admdongkor/`

### `data_version() -> str | None`

현재 로컬 캐시에 반영된 인덱스 data_version (예: `"2026.04.25"`). 캐시 없으면 `None`.

### `changelog() -> DataFrame`

인덱스 수정 이력. 컬럼: `version`, `changes`. 최신이 맨 위. manifest 에 포함된
`history` 를 그대로 보여준다.

```python
adk.changelog()
#       version                               changes
# 0  2026.04.25  1980 경상북도 대구시수성구 이름 수정
# 1  2026.04.20  1975 대전시 prefix 추가
```

---

## 버전 키 규칙

- (1975–2015): `YYYY1231`
- (2012–2026): `YYYYMMDD`

## 스키마

### `emd_*.parquet`

| 컬럼                | 설명                               |
| ------------------- | ---------------------------------- |
| `emd7`              | 통계청 7자리 (없으면 `<NA>`)       |
| `emd8`              | 통계청 8자리 (없으면 `<NA>`)       |
| `emdcd`             | 행안부 10자리 (1990 이전은 `<NA>`) |
| `emdnm`             | 읍면동명                           |
| `sggcd` / `sggnm`   | 행안부 5자리 시군구                |
| `sidocd` / `sidonm` | 행안부 2자리 시도                  |
| `area`              | m²                                 |
| `geom`              | polygon/multipolygon (EPSG:5179)   |

### `sgg_*.parquet`

`sggcd, sggnm, sidocd, sidonm, area, geom`

### `sido_*.parquet`

`sidocd, sidonm, area, geom`

> ### ⚠️ 1975 / 1980 / 1985 시점의 코드 체계 주의
>
> 이 세 시점은 행안부 10자리 체계가 도입되기 전이라 `emd7` (통계청 7자리) 만 존재하고 **`emdcd` / `sggcd` / `sidocd` 는 `<NA>`** 로 들어있다. 통계청 `emd7` 은 `sido(2) + sgg(3) + emd(2)` 구조로, **행안부 10자리와 체계 자체가 다르다** — 앞 2자리만으로 현재의 sido 와 1:1 매칭되지 않는다.
>
> 예: 1985 `emd7` 이 `"31…"` 으로 시작하는 row 는 당시의 **경기도**(통계청 코드), 2025 `sidocd="31"` 은 **울산광역시**(행안부 코드). 같은 문자열이지만 가리키는 지역이 다르다.
>
> - 코드로 직접 join 하지 말 것. 연도를 반드시 함께 붙여서 다루고, 시계열 매칭은 `match_adm` 이 공간 IoU + `version_key` 로 풀어준다.
> - 이름으로 검색(`find`) / 공간 매칭(`match_adm`) 은 정상 동작. emd/sgg/sido 단위 집계도 공간 기반이라 안전.
> - `sggcd` / `sidocd` 는 이 세 시점에서 `<NA>` 로만 보일 것이고, 이건 "행안부 체계가 아직 없다" 는 의미이지 누락이 아님.

---

## 용례 예시

`examples/example.ipynb` 에서 Colab/Jupyter 로 곧바로 돌려볼 수 있는 시나리오 모음:

- 원하는 연도의 서울 지도 받아 그리기
- 2023 군위 편입 전/후 비교
- 2012 세종시 등장 시계열
- 인구 데이터를 특정 영역(예 2025 대구) 경계로 시계열 집계

---

## 데이터 출처

[vuski/admdongkor](https://github.com/vuski/admdongkor) 레포의 `parquet/` 디렉토리.
통계청 shapefile (1975–2015) + 행안부·통계청 GeoJSON (2012–2026) 을 통일 스키마로 정리.

## 라이선스

MIT
