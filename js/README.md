# admdongkor (npm)

대한민국 행정동 경계 — 1975 년부터 현재까지 **60 개 이상 시점**의 읍면동(emd) / 시군구(sgg) / 시도(sido) 경계를 조회 · 검색 · 비교하는 npm 라이브러리.

파이썬 [`admdongkor`](https://pypi.org/project/admdongkor/) 패키지와 **같은 parquet 파일**을 공유한다. 지도 데이터는 GeoJSON 으로 바로 변환해 돌려주고 (CRS: **EPSG:4326 WGS84**), Leaflet / MapLibre / deck.gl / OpenLayers 에 그대로 투입 가능.

## 설치

```bash
npm install admdongkor
```

Node 18+ 필요 (글로벌 `fetch` 사용). 브라우저에서도 동작.

> v0.2.0+ 부터 인덱스 parquet 은 `https://raw.githubusercontent.com/vuski/admdongkor/master/dist/data/` 에서 받아온다. 이 URL 과 파일명(`_index_v3.parquet`, `timeline_v3_*`, `shape_pairs_v3_*`) 은 **영구 stable** — 한 번 빌드·배포한 번들도 데이터 수정(예: 과거 시군구 이름 정정)을 자동으로 반영받는다. 스키마 변경이 필요하면 `v4_*` 처럼 새 파일명으로 병렬 추가되고, 구 번들은 영향받지 않는다.

## Quick Start

```ts
import * as adk from "admdongkor";

// 시점 목록
adk.versions(); // ["19751231", ..., "20260401"]
adk.versions(2025); // 2025 년만

// 지도 호출 — GeoJSON FeatureCollection (EPSG:4326)
const sido = await adk.get("20250401", "sido");

// 이름으로 시점 검색
const rows = await adk.find("판교");

// 코드로 검색 — 숫자만 넣으면 자동으로 코드 검색
await adk.find("1111051500"); // 청운효자동 (행안부 10자리)
await adk.find("11110"); // 종로구 + 하위 읍면동 전부 (prefix)
await adk.findOffices("2920083000"); // 출장소 — 지도 없음, 코드 조회 전용

// 두 시점 경계 비교
const r = await adk.compare(["20111231", "20260401"]);
console.log(r.diff.length, "개 emd 변화");

// 과거 영역을 현재 경계로 매칭 (가중치)
const m = await adk.matchAdm({
  base: "20151231",
  region: "11110", // 2015 종로구 sgg 코드
  target: "20260401",
});
```

> named import (`import { get, find } from "admdongkor"`) 도 동일하게 지원한다. 다른 라이브러리와 이름 충돌이 걱정되면 위처럼 namespace 로 받아 `adk.get` / `adk.find` 로 쓰는 걸 권장. (파이썬 패키지 `import admdongkor as adk` 와 같은 스타일.)

---

## API

### `versions(year?)`

시점 키 목록 반환.

| 인자   | 타입                  | 설명                              |
| ------ | --------------------- | --------------------------------- |
| `year` | `number \| undefined` | 4 자리 연도 (int). 생략하면 전체. |

**반환**: `string[]` — 오름차순 정렬된 `YYYYMMDD` 문자열.

```ts
versions(); // ["19751231", ..., "20260401"]
versions(2025); // ["20250101", "20250401", "20250701", "20251001", "20251231"]
versions(1999); // [] — 해당 연도 데이터 없음
```

---

### `get(key, level?, options?)`

지도를 **GeoJSON FeatureCollection** 으로 받는다. deck.gl / Leaflet / MapLibre 에 바로 투입.

| 인자              | 타입                       | 기본           | 설명                                                       |
| ----------------- | -------------------------- | -------------- | ---------------------------------------------------------- |
| `key`             | `string`                   | —              | `versions()` 목록 중 하나                                  |
| `level`           | `"emd" \| "sgg" \| "sido"` | `"emd"`        |                                                            |
| `options.detail`  | `boolean`                  | `false`        | `false` = light (단순화, 빠름). `true` = 원본 (emd ~11MB). |
| `options.baseUrl` | `string`                   | GitHub raw     | CDN / 자체 호스팅으로 교체 가능                            |
| `options.fetch`   | `typeof fetch`             | 글로벌 `fetch` | 테스트·프록시 주입용                                       |
| `options.signal`  | `AbortSignal`              | —              | 취소                                                       |

**반환 타입** — `AdmFeatureCollection` (표준 GeoJSON):

```ts
{
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: { sidocd: "11", sidonm: "서울특별시", area: 605160000 },
      geometry: { type: "MultiPolygon", coordinates: [[[[126.88, 37.46], ...]]] }
    },
    // ...
  ]
}
```

레벨별 `properties` 필드:

| 레벨   | properties                                                     |
| ------ | -------------------------------------------------------------- |
| `sido` | `sidocd, sidonm, area`                                         |
| `sgg`  | `sggcd, sggnm, sidocd, sidonm, area`                           |
| `emd`  | `emd7, emd8, emdcd, emdnm, sggcd, sggnm, sidocd, sidonm, area` |

- `area` 는 m² (EPSG:5179 기준 계산값)
- geometry 는 `Polygon` 또는 `MultiPolygon`, CRS 는 EPSG:4326

> #### ⚠️ 1975 / 1980 / 1985 시점의 코드 체계 주의
>
> 이 세 시점(`"19751231"`, `"19801231"`, `"19851231"`) 은 행안부 10자리 체계가 도입되기 전이라 emd properties 에 `emd7` (통계청 7자리) 만 채워지고 **`emdcd` / `sggcd` / `sidocd` 는 `null`** 이다. 통계청 `emd7` 은 `sido(2) + sgg(3) + emd(2)` 구조이며 **행안부 10자리와 체계가 다르다** — 앞 2자리만으로 현재의 sido 와 1:1 매칭되지 않는다.
>
> 예: 1985 년 `emd7` 이 `"31…"` 으로 시작하는 feature 는 당시의 **경기도**(통계청 코드), 2025 년 `sidocd === "31"` 은 **울산광역시**(행안부 코드). 같은 문자열이지만 다른 지역이다.
>
> - `matchAdm` 은 공간 IoU + `version_key` 로 매칭하므로 이 시점도 안전하게 쓸 수 있다. 단 `region` 인자에는 연도와 일관된 체계의 코드를 넣어야 한다.
> - `find` 의 이름 검색은 정상 동작 (NFC 정규화 + substring).
> - 코드로 직접 join 하는 코드를 작성할 때는 이 세 시점을 반드시 분기 처리할 것.

#### light 용량 참고

| 레벨 | light (detail=false) | 원본 (detail=true) |
| ---- | -------------------- | ------------------ |
| sido | ~0.5 MB              | ~1.5 MB            |
| sgg  | ~1 MB                | ~4 MB              |
| emd  | ~2.4 MB              | ~11 MB             |

웹에서 쓸 때는 light 가 기본. 정밀 분석이 필요하면 `detail: true`.

---

### `getParquet(key, level?, options?)`

**parquet 바이트(`ArrayBuffer`) 그대로** 받는다. 파싱 안 함.

| 용도                                      | 설명                                         |
| ----------------------------------------- | -------------------------------------------- |
| Web Worker                                | transferable 로 넘겨 main thread 부담 최소화 |
| IndexedDB / Cache API                     | 바이트 그대로 저장해 재방문 시 즉시 렌더     |
| parquet-wasm / `@geoarrow/deck.gl-layers` | Arrow 네이티브 파이프라인에 직접 투입        |

```ts
const buf = await adk.getParquet("20260401", "emd");
worker.postMessage(buf, [buf]); // zero-copy transfer
```

데이터 포맷: geo-parquet spec, geometry WKB, EPSG:4326, snappy 압축.

---

### `find(name, options?)`

행정구역명 substring 검색 **또는 코드 검색**. **인덱스 parquet 전체**를 훑는다.

**숫자로만 이루어진 쿼리는 자동으로 코드 검색**이 된다 — 행정구역명 중 숫자만인 것은 없어 이름 검색과 충돌하지 않는다. `by` 로 강제 가능.

| 쿼리                                  | 해석 | 결과                                     |
| ------------------------------------- | ---- | ---------------------------------------- |
| `find("종로구")`                      | 이름 | 이름에 '종로구' 가 들어가는 전 레벨      |
| `find("1111051500")`                  | 코드 | 그 읍면동 (행안부 10자리)                |
| `find("11110")`                       | 코드 | 시군구 11110 + 하위 읍면동 전부 (prefix) |
| `find("11110", { exact: true })`      | 코드 | 자릿수 완전일치 — 종로구만               |
| `find("11110", { by: "name" })`       | 이름 | 이름 검색 강제 (결과 없음)               |

코드 검색은 **prefix 매칭**이라 자릿수를 정확히 맞추지 않아도 된다. `"11"` → 시도 11 + 시군구 `11xxx` + 읍면동 `11xxxxxxxx` 전부. 매칭 대상은 `code`(행안부) / `code7` / `code8`(통계청) 셋 다 — 1975~85 는 행안부 코드가 없어 통계청 7자리가 유일한 키다.

**계층 검색** — 이름 검색일 때 공백 토큰 수로 레벨 자동 추정:

| 쿼리 형태 | 자동 필터      | 예시                         |
| --------- | -------------- | ---------------------------- |
| 1 토큰    | 없음 (전 레벨) | `"판교"`, `"종로"`           |
| 2 토큰    | `sgg`          | `"서울특별시 종로구"`        |
| 3 토큰    | `emd`          | `"서울특별시 종로구 사직동"` |

| 옵션                         | 타입                  | 설명                                                                  |
| ---------------------------- | --------------------- | --------------------------------------------------------------------- |
| `level`                      | `Level`               | 자동 필터보다 우선. 수동 강제.                                        |
| `exact`                      | `boolean`             | 이름이면 `name` 단독 완전일치 (단일 토큰 전용). 코드면 자릿수 완전일치 |
| `year`                       | `number[]`            | `[2025]` 단일 연도 / `[2000, 2005]` inclusive range                   |
| `by`                         | `"name"` \| `"code"`  | 검색 방식 강제. 생략하면 자동 판별                                    |
| `baseUrl`, `fetch`, `signal` | —                     | 다른 함수와 동일                                                      |

**반환**: `FindRow[]` — 각 매치는 `(version_key, level, name, code, sidonm, sggnm, ...)` 필드.

```ts
import * as adk from "admdongkor";

const rows = await adk.find("여주군");
adk.findFirst(rows); // "19751231"   (가장 이른 시점)
adk.findLast(rows); // "20130701"   (가장 늦은 시점)
adk.findVersions(rows); // 매치된 고유 version_key 목록 (정렬됨)
```

첫 `find()` 호출 시 인덱스(~1.7MB) 를 한 번 fetch 하고 프로세스 메모리에 캐시한다. `clearIndexCache()` 로 해제.

---

### `findOffices(query, options?)`

출장소(出張所) 조회. **코드를 넣었을 때 그게 어디인지 알려주는 용도.**

출장소는 행안부 행정동 코드 체계에는 존재하지만 **경계 지도가 없다** (geojson/shp 에 나오지 않음). 그래서 `get()` 으로 지도를 받을 수 없고 `find()` 결과에도 포함되지 않는다.

`version_key` 대신 `created` / `abolished` (YYYYMMDD) 로 유효 기간을 표현한다. `abolished` 가 `null` 이면 현존.

```ts
await adk.findOffices("2920083000"); // 광주 광산구 임곡출장소 (1995-01-01 ~ 1998-10-15 말소)
await adk.findOffices("28"); // 인천 소속 출장소 전부 (prefix)
await adk.findOffices("영종"); // 이름으로 — 인천 영종출장소의 변천 이력
```

**반환**: `OfficeRow[]` — `code, name, sggnm, sidonm, sggcd, sidocd, level, created, abolished`

| 옵션                         | 타입                 | 설명                                       |
| ---------------------------- | -------------------- | ------------------------------------------ |
| `exact`                      | `boolean`            | 코드 완전일치 / 이름 완전일치              |
| `by`                         | `"name"` \| `"code"` | 검색 방식 강제. 생략하면 자동 판별         |
| `baseUrl`, `fetch`, `signal` | —                    | 다른 함수와 동일                           |

2026-07 기준 392건 (현존 75 / 말소 317). `level` 은 코드 뒤 5자리가 `00000` 이면 `"sgg"`, 아니면 `"emd"`.

> 데이터 버전 `2026.08.02` 부터 제공 (`_offices.parquet`, 22KB). 그 이전 데이터를 가리키는 `baseUrl` 을 쓰면 예외 대신 빈 배열을 반환한다.

---

### `compare(versions, options?)`

두 시점의 emd 경계를 비교. 공통 emdcd 의 shape_id 가 같으면 `same`, 다르면 `shape_pairs` 의 IoU 조회 → `threshold` 이상이면 same 승격, 미만이면 `changed`. 한쪽에만 있으면 `only_in_a` / `only_in_b`.

```ts
const r = await compare(["20111231", "20260401"], { threshold: 0.99 });

r.same; // 경계 유지된 emd (각 emd 당 2 rows: va + vb)
r.diff; // 변화 있는 emd. status 컬럼:
//   "changed"    — 양쪽 존재, 경계 다름
//   "only_in_a"  — va 에만 존재 (소멸)
//   "only_in_b"  — vb 에만 존재 (신설)
```

| 옵션        | 기본   | 설명                                                                                   |
| ----------- | ------ | -------------------------------------------------------------------------------------- |
| `threshold` | `0.99` | shape_id 다를 때 IoU ≥ threshold 면 same 승격. `1.0` 이면 엄격히 shape_id 일치만 same. |

활용: "2011 → 2026 사이 대구에서 경계 바뀐 동들" 같은 변화 탐지 / 인터랙티브 시각화.

---

### `matchAdm(options)`

**과거·미래 영역 매칭** — base 시점의 행정구역 경계에 target 시점 emd 들이 얼마나 걸치는지 (weight) 반환. 경계가 바뀌어 단순 코드 매칭이 안 될 때 씀.

```ts
// 2015 종로구 → 2025 는 어느 emd 들?
const m = await matchAdm({
  base: "20151231",
  region: "11110",
  target: "20251231",
});

m.emd; // MatchEmdRow[]. weight 는 해당 target emd 의 면적 중 base region 에 속한 비율 (0–1)
await m.sgg(); // sgg 단위 집계. weight = Σ(emd_weight × emd_area) / sgg_total_area
await m.sido(); // sido 단위 집계
```

| 옵션        | 타입                 | 설명                                                                 |
| ----------- | -------------------- | -------------------------------------------------------------------- |
| `base`      | `string`             | 기준 시점                                                            |
| `region`    | `string`             | 2자리(시도) / 5자리(시군구) / 7자리(통계청 emd) / 10자리(행안부 emd) |
| `target`    | `string \| string[]` | 단일 또는 여러 target 시점                                           |
| `minWeight` | `number`             | 이 값 미만 weight 제외 (기본 0)                                      |

활용:

- **인구 재집계**: 2026 기준 통계를 2010 경계로 돌려 비교
- **서비스 지역 마이그레이션**: 과거 배달/택시 서비스 영역을 현재 행정구역에 재매핑
- **시계열 패널 데이터 정합**: 행정구역 통폐합으로 끊긴 시계열 이어붙이기

---

### `dataVersion(options?)` / `changelog(options?)`

원격 인덱스의 **데이터 버전** + **수정 이력** 조회. 과거 행정경계 데이터는 오타·영역 정정 등이 계속 반영되므로, 이 API 로 "내 번들이 현재 어떤 버전의 데이터를 바라보는지" 확인할 수 있다.

```ts
import { dataVersion, changelog } from "admdongkor";

await dataVersion();
// "2026.04.25"

await changelog();
// [
//   { version: "2026.04.25", changes: "1980 경상북도 대구시수성구 이름 수정" },
//   { version: "2026.04.20", changes: "1975 대전시 prefix 추가" },
//   ...
// ]
```

둘 다 내부적으로 `manifest.json` 한 번만 fetch 하고 프로세스 메모리에 캐시. `clearManifestCache()` 로 해제.

인덱스 parquet 자체는 canonical URL (`dist/data/`) 을 영구 stable 로 유지하므로, 번들을 한 번 빌드해 배포해두면 라이브러리 업그레이드 없이도 데이터 수정이 자동 반영된다 — 파이썬 `admdongkor` 0.6.0+ 과 같은 원리.

> 스키마 변경 (예: `timeline_v3_*` → `v4_*`) 은 **새 파일명으로만 추가**되고 구 파일은 freeze 된다. 이미 배포된 번들은 빌드 시점의 스키마를 그대로 계속 바라봄 — 호환성 파괴 없음.

---

## 프레임워크별 예시

### Leaflet

```ts
import L from "leaflet";
import * as adk from "admdongkor";

const fc = await adk.get("20250401", "sido");
L.geoJSON(fc, { style: { color: "#2563eb", weight: 1 } }).addTo(map);
```

### MapLibre GL

```ts
import maplibregl from "maplibre-gl";
import * as adk from "admdongkor";

const fc = await adk.get("20250401", "sgg");
map.addSource("adm", { type: "geojson", data: fc });
map.addLayer({
  id: "adm-line",
  type: "line",
  source: "adm",
  paint: { "line-color": "#2563eb", "line-width": 1 },
});
```

### deck.gl — GeoJsonLayer (간단)

```ts
import { GeoJsonLayer } from "@deck.gl/layers";
import * as adk from "admdongkor";

const fc = await adk.get("20250401", "emd");
new GeoJsonLayer({
  id: "adm",
  data: fc,
  stroked: true,
  filled: true,
  getFillColor: [37, 99, 235, 40],
  getLineColor: [37, 99, 235, 200],
  lineWidthUnits: "pixels",
  getLineWidth: 1,
});
```

### deck.gl — @geoarrow/deck.gl-layers (대용량 · Arrow 네이티브)

emd 원본(`detail: true`, ~11MB) 을 GeoJSON 으로 파싱하면 메모리가 3~5 배로 부풀고 초기 렌더도 느려진다. Arrow 컬럼형 구조를 그대로 GPU 버퍼에 매핑하는 [`@geoarrow/deck.gl-layers`](https://github.com/geoarrow/deck.gl-layers) 를 쓰면 이 과정을 생략할 수 있다.

```bash
npm install apache-arrow parquet-wasm @geoarrow/deck.gl-layers
```

```ts
import { tableFromIPC } from "apache-arrow";
import { readParquet } from "parquet-wasm";
import { GeoArrowPolygonLayer } from "@geoarrow/deck.gl-layers";
import * as adk from "admdongkor";

// 1. parquet 바이트 받기 (파싱 안 함)
const buf = await adk.getParquet("20250401", "emd", { detail: true });

// 2. parquet → Arrow IPC → Arrow Table
const wasmTable = readParquet(new Uint8Array(buf));
const table = tableFromIPC(wasmTable.intoIPCStream());

// 3. Arrow Table 을 deck.gl 레이어에 그대로 투입
new GeoArrowPolygonLayer({
  id: "adm-arrow",
  data: table,
  getGeometry: table.getChild("geometry")!,
  stroked: true,
  filled: true,
  getFillColor: [37, 99, 235, 40],
  getLineColor: [37, 99, 235, 200],
  getLineWidth: 1,
});
```

파일이 geo-parquet spec 이라 `geometry` 컬럼은 WKB. `@geoarrow/deck.gl-layers` 는 WKB · WKT · geoarrow native encoding 을 전부 지원하므로 별도 변환 불필요.

---

## 데이터 출처 · 라이선스

- 원본 데이터: [github.com/vuski/admdongkor](https://github.com/vuski/admdongkor) — 행정안전부 공개자료 가공
- 라이선스: MIT
- 같은 데이터를 쓰는 파이썬 패키지: [PyPI `admdongkor`](https://pypi.org/project/admdongkor/)
- 라이브 데모: [admdongkor.vw-lab.com](https://admdongkor.vw-lab.com)
