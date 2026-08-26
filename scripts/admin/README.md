# 관리자 스크립트 사용법

admdongkor 는 **parquet 를 input 으로 받아** 인덱스·시계열·web timeline bin 산출물을
만드는 역할만 맡는다. GeoJSON → parquet 변환은 **preprocessing 레포**
(`adk-master/preprocessing/`) 에서 수행한다.

0.6.0+ 부터 인덱스 parquet 은 PyPI wheel 에 embed 되지 않고 `dist/data/` 로 publish
된다. 라이브러리는 import 시 GitHub raw 에서 manifest 비교 후 필요한 파일만 캐시에
받아 쓴다.

---

## 한 줄 요약

parquet 가 `admdongkor/parquet/` 에 이미 있다는 전제로:

```powershell
python scripts\admin\rebuild_all.py
python scripts\admin\publish_data.py --changes "<수정 내용 한 줄>"
git add dist/data/ ; git commit -m "data: <yyyy.mm.dd> - <수정 내용>" ; git push
```

- `rebuild_all.py` — `_versions.py` + 인덱스 + 시계열 인덱스 재빌드
- `publish_data.py` — 재빌드된 인덱스를 `dist/data/` 로 복사 + `manifest.json` / `CHANGELOG.md` 갱신

> **`_offices.parquet` 은 `rebuild_all.py` 가 만들지 않는다.** 출장소는 경계
> 지도가 없어 지도 파이프라인 밖에 있다 — 행안부 `KIKcd_H` xlsx 에서
> `preprocessing/scripts/build_offices.py` 로 직접 생성한다. `publish_data.py`
> 는 이미 만들어진 파일을 `dist/data/` 로 복사만 한다. 갱신 절차는
> [.readme/ADMIN_WORKFLOW.md](../../../.readme/ADMIN_WORKFLOW.md) 시나리오 C.

**예상 소요 시간**: 약 **35분** (rebuild_all 의 phase 3 시계열 인덱스가 대부분).
publish_data 는 수 초.

---

## 사전 준비: parquet 생성

신규 GeoJSON 이 들어오면 먼저 preprocessing 에서 parquet 를 만든다:

```powershell
cd z:\Github\adk-master\preprocessing
python scripts\rebuild_emd_unified.py --version 20260501
python scripts\rebuild_sgg_sido.py 20260501
# 결과: data/geom/{emd,sgg,sido}_20260501.parquet
```

생성된 parquet 를 admdongkor 로 반영:

```powershell
Copy-Item data\geom\emd_20260501.parquet,data\geom\sgg_20260501.parquet,data\geom\sido_20260501.parquet ..\admdongkor\parquet\
```

(향후 preprocessing 이 안정화되면 직접 admdongkor/parquet/ 로 쓰도록 통합 예정)

---

## rebuild_all.py

### 전체 재빌드
```powershell
python scripts\admin\rebuild_all.py
```

### 특정 단계만
```powershell
# phase 2: _versions.py + find 인덱스만
python scripts\admin\rebuild_all.py --only-phase 2

# phase 3: 시계열 인덱스만 (워커 50)
python scripts\admin\rebuild_all.py --only-phase 3

# phase 4: 인덱스 산출물을 lib/src/admdongkor/data/ 로 모으기만
python scripts\admin\rebuild_all.py --only-phase 4
```

### 실행 계획 미리보기
```powershell
python scripts\admin\rebuild_all.py --dry-run
```

### 워커 수 조절
```powershell
# 기본 50. 시스템이 버거우면 줄이기
python scripts\admin\rebuild_all.py --only-phase 3 --workers 30
```

### 실행 후 변경되는 파일

- `parquet/_index.parquet` (갱신)
- `parquet/timeline_v3_*.parquet` × 3 (갱신)
- `parquet/shape_pairs_v3_*.parquet` × 3 (갱신)
- `lib/src/admdongkor/data/*.parquet` (재빌드 중간 산출 — publish 전 단계)
- `lib/src/admdongkor/data/_index_v3.parquet` (`_index.parquet` 의 canonical 별칭)
- `lib/src/admdongkor/_versions.py` (자동 재생성)

---

## publish_data.py

`rebuild_all.py` 로 만든 인덱스를 `dist/data/` 로 복사하고 `manifest.json` /
`CHANGELOG.md` 를 갱신한다. 이 단계까지 push 해야 **사용자 라이브러리가 다음 import
때 자동으로 반영**받는다.

```powershell
python scripts\admin\publish_data.py --changes "1980 경상북도 대구시수성구 이름 수정"
# 옵션:
#   --data-version 2026.04.25   명시 지정 (기본: 오늘 날짜 YYYY.MM.DD)
#   --dry-run                    실제 쓰기 없이 계획만 출력
```

실행 후 커밋·푸시:

```powershell
cd z:\Github\adk-master\admdongkor
git add dist/data/
git commit -m "data: 2026.04.25 - 1980 경상북도 대구시수성구 이름 수정"
git push
```

**푸시는 사용자 판단으로.** 스크립트가 자동 push 하지 않는다.

---

## 파일 구성

| 파일 | 역할 |
|---|---|
| `rebuild_all.py` | 메인 래퍼. `_versions.py` + 인덱스 재빌드. |
| `publish_data.py` | `lib/src/admdongkor/data/*.parquet` → `dist/data/` + `manifest.json` / `CHANGELOG.md` |
| `build_light_parquet.py` | parquet → simplified(`*_light.parquet`) × mapshaper 18.7%. 독도 등 미세 섬은 단순화 후 원본 part 로 복원 |
| `fix_ulleung_geojson.py` | **원본 geojson** 의 울릉도 보정 + 독도 정리. 파일당 3줄만 수정, 멱등 |
| `fix_ulleung.py` | 1회성 보수 — 울릉도 위치 보정(그룹별 평행이동) + 독도 정리. **원본** parquet 대상, 재실행 안전 |
| `patch_dokdo_light.py` | (사용 완료) light 에 독도 주입. 현재는 `build_light_parquet.py` 가 자동 처리 |
| `build_timeline.py` | web 프론트의 `web/public/timeline/v/<key>/geom.bin` + `meta.parquet` 생성 |

상위 `scripts/` 의 `measure_v3_*.py` 들은 phase 3 에서 호출됨 (관리자가 직접 돌릴 일 없음).

---

## PyPI 재배포가 필요한 경우

**데이터만 수정했을 때** (이름 오타 정정, 경계 수정 등):
- `publish_data.py` 실행 + `dist/data/` push 만 하면 됨. **PyPI 재배포 불필요.**

**lib 소스가 바뀌었을 때** (`lib/src/admdongkor/*.py` 수정):
- `pyproject.toml` 버전 bump → `python -m build` → `twine upload` 필요.

---

## 트러블슈팅

- **phase 3 중단**: 60 workers 로 돌려 시스템 다운된 적 있음 (2026-04-23). `--workers 30` 으로 낮춰서 재시도.
- **parquet 가 없다는 에러**: preprocessing 에서 parquet 를 먼저 생성해서 `admdongkor/parquet/` 로 복사했는지 확인.
- **publish_data 의 "missing index file"**: `rebuild_all.py` phase 2~4 를 먼저 돌려야 `lib/src/admdongkor/data/` 에 인덱스가 채워짐.
