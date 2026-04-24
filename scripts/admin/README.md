# 관리자 스크립트 사용법

admdongkor 는 **parquet 를 input 으로 받아** 인덱스/시계열/embed 산출물을 만드는 역할만 맡는다. GeoJSON → parquet 변환은 **preprocessing 레포**(`adk-master/preprocessing/`) 에서 수행한다.

---

## 한 줄 요약

parquet 가 이미 있다는 전제로:

```bash
python scripts/admin/rebuild_all.py
```

이 명령 하나로:
1. `lib/src/admdongkor/_versions.py` 자동 재생성
2. `parquet/_index.parquet` 재빌드 (find() 용)
3. `parquet/timeline_v3_*.parquet`, `shape_pairs_v3_*.parquet` 재빌드 (시계열 매칭용)
4. embed 산출물을 `lib/src/admdongkor/data/` 로 배포

까지 전부 수행. 관리자는 커밋만 하면 됨.

**예상 소요 시간**: 약 **35분** (phase 3 시계열 인덱스가 대부분).

---

## 사전 준비: parquet 생성

신규 GeoJSON 이 들어오면 먼저 preprocessing 에서 parquet 를 만든다:

```bash
# adk-master/preprocessing/ 에서
python scripts/rebuild_emd_unified.py --version 20260501
python scripts/rebuild_sgg_sido.py 20260501
# 결과: data/geom/{emd,sgg,sido}_20260501.parquet
```

생성된 parquet 를 admdongkor 로 반영:

```bash
cp preprocessing/data/geom/{emd,sgg,sido}_20260501.parquet admdongkor/parquet/
```

(향후 preprocessing 이 안정화되면 직접 admdongkor/parquet/ 로 쓰도록 통합 예정)

---

## 실행

### 전체 재빌드
```bash
python scripts/admin/rebuild_all.py
```

### 특정 단계만
```bash
# phase 2: _versions.py + find 인덱스만
python scripts/admin/rebuild_all.py --only-phase 2

# phase 3: 시계열 인덱스만 (워커 50)
python scripts/admin/rebuild_all.py --only-phase 3

# phase 4: embed 산출물 배포만
python scripts/admin/rebuild_all.py --only-phase 4
```

### 실행 계획 미리보기
```bash
python scripts/admin/rebuild_all.py --dry-run
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
- `parquet/_index.parquet` (갱신)
- `parquet/timeline_v3_*.parquet` × 3 (갱신)
- `parquet/shape_pairs_v3_*.parquet` × 3 (갱신)
- `lib/src/admdongkor/data/*.parquet` (embed)
- `lib/src/admdongkor/_versions.py` (자동 재생성)

**푸시는 사용자 판단으로.** 자동 푸시 안 함.

---

## 파일 구성

| 파일 | 역할 |
|---|---|
| `rebuild_all.py` | 메인 래퍼. 이것만 실행하면 됨. |
| `build_light_parquet.py` | parquet → simplified(`*_light.parquet`) × mapshaper 18.7% |

상위 `scripts/` 의 `measure_v3_*.py` 들은 phase 3 에서 호출됨 (관리자가 직접 돌릴 일 없음).

---

## 트러블슈팅

- **phase 3 중단**: 60 workers 로 돌려 시스템 다운된 적 있음 (2026-04-23). `--workers 30` 으로 낮춰서 재시도
- **parquet 가 없다는 에러**: preprocessing 에서 parquet 를 먼저 생성해서 `admdongkor/parquet/` 로 복사했는지 확인
