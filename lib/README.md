# admdongkor

한국 행정경계(읍면동/시군구/시도) 1975–2026 시계열 지도 다운로더.
GitHub 에 올라간 parquet 파일을 런타임에 받아 **GeoDataFrame** 으로 돌려준다.

```bash
pip install admdongkor
```

## 빠른 시작

```python
import admdongkor as adk

# 1. 어떤 버전이 있는지 본다
adk.get_list()                   # ['19751231', '19801231', ..., '20260201']
adk.get_list(year=2025)          # ['20250101', '20250401', ...]

# 2. 지명으로 버전 찾기
adk.find("종로")                          # 모든 레벨 substring
adk.find("서울특별시 종로구")               # 자동으로 sgg 만 (읍면동 안 나옴)
adk.find("서울특별시 종로구 사직동")         # 자동으로 emd 만
adk.find("수원시 권선구")                   # "수원시권선구" 한덩어리도 매치

# 3. 지도 로드 (EPSG:5179)
gdf = adk.get("20250401", "emd")     # 읍면동
sgg = adk.get("20250401", "sgg")     # 시군구
sido = adk.get("20250401", "sido")   # 시도
```

## API

### `get_list(year: int | None = None) -> list[str]`

버전 키 목록. `year` 를 주면 해당 연도만 필터.

### `find(name, level=None, exact=False, year=None) -> pd.DataFrame`

행정구역명으로 버전 검색. 대소문자·공백 무시 substring 매칭.

**계층 검색** — 공백으로 토큰을 나누면 자동으로 레벨이 좁혀진다:

| 쿼리 | 자동 level | 설명 |
|---|---|---|
| `find("종로")` | 전체 | 모든 레벨 substring |
| `find("서울특별시 종로구")` | `sgg` | 해당 시도 안의 그 sgg 만 |
| `find("서울특별시 종로구 사직동")` | `emd` | 그 emd 만 |
| `find("수원시 권선구")` | `sgg` | "수원시권선구" 처럼 저장된 sgg 매치 |

`level=` 을 명시하면 자동보다 우선한다 (`find("서울특별시 종로구", level="emd")` 는
종로구 내 읍면동 전체 반환). `exact=True` 는 단일 토큰 쿼리에서만 유효하며 `name`
컬럼 단독 완전일치.

`year=[2025]` 는 단일, `year=[2000, 2005]` 는 범위(inclusive).

**반환 컬럼** (10개):

| 컬럼 | 설명 |
|---|---|
| `version_key` | 버전 키 (예: `"20250401"`) |
| `level` | `"sido"` / `"sgg"` / `"emd"` |
| `sidonm` | 상위 시도명 (sgg/emd 행에서 채워짐) |
| `sggnm` | 상위 시군구명 (emd 행에서만) |
| `name` | 해당 행의 이름 |
| `code` | 행안부 코드 (sido 2자리 / sgg 5자리 / emd 10자리) |
| `code7` | 통계청 7자리 (emd 레벨 한정) |
| `code8` | 통계청 8자리 (emd 레벨 한정) |
| `sggcd` | 상위 시군구 코드 (emd 한정) |
| `sidocd` | 상위 시도 코드 (sgg/emd 한정) |

### `get(key, level="emd", *, force_refresh=False) -> GeoDataFrame`

특정 버전의 지도를 `GeoDataFrame` 으로 반환. CRS 는 EPSG:5179 (Korea 2000 / Unified CS).
`level` 은 `"emd"` / `"sgg"` / `"sido"` 중 하나. `force_refresh=True` 로 캐시 무시 재다운로드.

### `cache_dir() -> Path`

받은 parquet 이 쌓이는 디렉토리. OS 별로 자동 결정되며 `ADMDONGKOR_CACHE_DIR`
환경변수로 override 가능.

## 버전 키 규칙

- Shapefile 기반 (1975–2015): `YYYY1231` — 예: `"19751231"`
- GeoJSON 기반 (2012–2026): `YYYYMMDD` — 예: `"20260201"`

## 캐시

- Windows: `%LOCALAPPDATA%\admdongkor\`
- macOS/Linux: `$XDG_CACHE_HOME/admdongkor/` 또는 `~/.cache/admdongkor/`
- 환경변수 `ADMDONGKOR_CACHE_DIR` 로 override

## 스키마

### emd_*.parquet

| 컬럼 | 설명 |
|---|---|
| `emd7` | 통계청 7자리 (없으면 `<NA>`) |
| `emd8` | 통계청 8자리 (없으면 `<NA>`) |
| `emdcd` | 행안부 10자리 (1990 이전은 `<NA>`) |
| `emdnm` | 읍면동명 |
| `sggcd` / `sggnm` | 행안부 5자리 시군구 |
| `sidocd` / `sidonm` | 행안부 2자리 시도 |
| `area` | m² |
| `geom` | polygon/multipolygon (EPSG:5179) |

### sgg_*.parquet
`sggcd, sggnm, sidocd, sidonm, area, geom`

### sido_*.parquet
`sidocd, sidonm, area, geom`

## 데이터 출처

[vuski/admdongkor](https://github.com/vuski/admdongkor) 레포의 `parquet/` 디렉토리.
통계청 행정구역 shapefile (1975–2015) + 행안부·통계청 GeoJSON (2012–2026) 을
통일 스키마로 정리한 산출물.

## 라이선스

MIT
