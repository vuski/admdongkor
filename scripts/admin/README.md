# 관리자 스크립트 사용법

4개월 주기로 새 행정경계 GeoJSON 이 배포되면 이 폴더의 스크립트로 parquet + 인덱스를 재빌드한다.

전체 가이드: [`.claude/ADMIN_WORKFLOW.md`](../../.claude/ADMIN_WORKFLOW.md)

---

## 한 줄 요약

```bash
python scripts/admin/rebuild_all.py --version 20260501
```

이 명령 하나로:
1. `raw/geojson/20260501/HangJeongDong_ver20260501.geojson` 을 읽어
2. `parquet/{emd,sgg,sido}_20260501.parquet` 생성
3. `lib/src/admdongkor/_versions.py` 자동 재생성
4. `parquet/_index.parquet` 재빌드 (find() 용)
5. `parquet/timeline_v3_*.parquet`, `shape_pairs_v3_*.parquet` 재빌드 (시계열 매칭용)

까지 전부 수행. 관리자는 커밋만 하면 됨.

**예상 소요 시간**: 새 버전 1개 추가 시 **약 35분** (phase 3 시계열 인덱스가 대부분).

---

## 사전 준비

새 GeoJSON 배포 파일을 레포의 `raw/geojson/<YYYYMMDD>/` 폴더에 배치:

```
Z:/Github/admdongkor/
└── raw/geojson/
    └── 20260501/
        └── HangJeongDong_ver20260501.geojson
```

`raw/` 는 `.gitignore` 대상이라 레포에 커밋되지 않음.

---

## 실행

### 일반 실행 (신규 버전 1개)
```bash
python scripts/admin/rebuild_all.py --version 20260501
```

### 여러 버전 한꺼번에
```bash
python scripts/admin/rebuild_all.py --version 20260501 20260901
```

### parquet 은 이미 있고 인덱스만 재빌드
```bash
python scripts/admin/rebuild_all.py --skip-phase1
```

### 특정 단계만
```bash
# phase 2: _versions.py + find 인덱스만
python scripts/admin/rebuild_all.py --only-phase 2

# phase 3: 시계열 인덱스만 (워커 50)
python scripts/admin/rebuild_all.py --only-phase 3

# phase 4: 중간 산출물을 parquet/ 으로 이동만
python scripts/admin/rebuild_all.py --only-phase 4
```

### 실행 계획 미리보기
```bash
python scripts/admin/rebuild_all.py --version 20260501 --dry-run
```

### 워커 수 조절
```bash
# 기본 50. 시스템이 버거우면 줄이기
python scripts/admin/rebuild_all.py --only-phase 3 --workers 30
```

---

## 실행 후

```bash
git status
```

변경될 파일:
- `parquet/{emd,sgg,sido}_<NEW_VERSION>.parquet` (신규)
- `parquet/_index.parquet` (갱신)
- `parquet/timeline_v3_*.parquet` × 3 (갱신)
- `parquet/shape_pairs_v3_*.parquet` × 3 (갱신)
- `lib/src/admdongkor/_versions.py` (자동 재생성)

커밋 메시지 예:
```
parquet 20260501 추가 + 인덱스 재빌드
```

**푸시는 사용자 판단으로.** 자동 푸시 안 함.

---

## 파일 구성

| 파일 | 역할 |
|---|---|
| `rebuild_all.py` | 메인 래퍼. 이것만 실행하면 됨. |
| `build_unified_parquet.py` | GeoJSON → 3 parquet 변환 (phase 1 내부 호출) |
| `geojson_loader.py` | GeoJSON 로더 + 스키마 감지 + EPSG:5179 재투영 |
| `dissolve.py` | emd → sgg/sido dissolve + hairline hole 제거 |

상위 `scripts/` 의 `measure_v3_*.py` 들은 phase 3 에서 호출됨 (관리자가 직접 돌릴 일 없음).

---

## 트러블슈팅

- **phase 1 실패, "no .geojson in ..."**: `raw/geojson/<version>/` 에 `.geojson` 파일이 있는지 확인
- **phase 3 중단**: 60 workers 로 돌려 시스템 다운된 적 있음 (2026-04-23). `--workers 30` 으로 낮춰서 재시도
- **단일 GeoJSON 파일 경로 직접 지정**:
  ```bash
  python scripts/admin/build_unified_parquet.py --version 20260501 --geojson /path/to/file.geojson
  ```

---

## 과거 데이터 (1975–2015 shapefile) 재빌드가 필요할 때

이 스크립트는 **GeoJSON 전용**. shapefile 기반 과거 데이터는 이미 처리 완료됐고, 재빌드가 필요하면 `Z:/Github/admdongkor-timeseries/scripts/rebuild_emd_unified.py` 를 사용 (별도 의존성 있음).
