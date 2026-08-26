# 배터리 이상탐지 모델 — 인프라 인수인계

> **이 저장소에 붙인 결과 (2026-08-26).** 아래는 모델팀이 준 원문이다. 이 저장소에
> 실제로 어떻게 들어왔는지는 [`docs/pipeline-overview.md`](../docs/pipeline-overview.md) 7절에 있고,
> 원문과 다른 곳은 다음 넷이다.
>
> | 원문 | 이 저장소 |
> |---|---|
> | `deploy/battery_detector.py` | 저장소 루트의 [`battery_detector.py`](../battery_detector.py) — 내용은 그대로 |
> | `battery.raw` → `battery.score` / `battery.alarm` 3토픽 | 기존 `battery.pack.measurement` → `battery.pack.verdict` 2토픽. `battery_detector` 의 FastAPI·aiokafka 부분은 쓰지 않고 **`DetectorPool` 만 가져다 쓴다**(아래 '설계 메모' 가 그렇게 하라고 한 방식) |
> | `BD_SOURCE_HZ=1.0` | **`0.2`.** 이 파이프라인은 `database.py` 가 5초 구간마다 첫 행만 남겨 발행하므로 토픽에 이미 5초/행이 흐른다. `1.0` 을 넣으면 25초/행이 되어 조용히 틀린다 |
> | `make_bundle.py` / `smoke_test.py` | 받지 않았다. 대신 [`tests/test_detector.py`](../tests/test_detector.py) 가 같은 것을 확인한다(정상 팩 중앙값 2.529 재현, 결함 주입 시 지목, 방전 차단, stride) |

`Kafka(원시) → FastAPI(추론) → Kafka(결과) → Streamlit(대시보드)` 구성에 붙이는 배포 코드다.

```
deploy/
├── battery_detector.py   ★ 배포 코드 전부 (설정·스키마·전처리·검출기·서비스)
├── make_bundle.py        아티팩트 4개 → 번들 1개로 묶는 도구
├── smoke_test.py         인프라 없이 파이프라인 확인
├── requirements.txt
└── .env.example
```

인프라에 넘길 것은 **3덩어리**다.

| | 무엇 |
|---|---|
| `battery_detector.py` | 이 파일 하나 (약 600줄) |
| `battery_model_*.bundle` | 모델 아티팩트 1개 (3.3 MB) |
| `src/` | 모델 코드 `step*.py` — 복제하지 않고 import 한다 |

`src/` 를 번들에 넣지 않은 이유는, 넣으면 학습 코드와 배포 코드가 갈라지고
`manifest` 의 `source_sha8` 검증이 무의미해지기 때문이다. 기동 시 이 해시를 대조한다.

---

## 반드시 먼저 읽을 것 — 세 가지 함정

이 시스템은 **잘못 붙여도 예외가 나지 않고 조용히 틀린다.** 아래 셋이 그렇다.

### 1. 입력은 5초에 1행이어야 한다

학습 데이터는 전부 5초/행이다(`manifest` 의 `target_sec_per_row: 5.0`).
현장 BMS 는 1 Hz 로 보내므로 **5행 중 1행만 써야 한다.**

코드의 '초' 상수가 사실은 '행'이라, 1 Hz 를 그대로 넣으면 모든 시간 창이 5배 줄어든다.

| 상수 | 코드 표기 | 실제(5초/행) | 1 Hz 로 넣으면 |
|---|---|---|---|
| `SLOPE_HALF = 30` (V2 창) | "60초" | **300초** | 60초 |
| `persist = 2` | "10초" | 10초 | **2초** ← 오탐 급증 |
| `warmup = 60` | "300초" | 300초 | 60초 |

`StreamGate` 가 처리한다. **`BD_SOURCE_HZ` 만 정확히 넣으면 된다.**
평균이 아니라 솎아내기다 — 평균을 내면 전압 스파이크가 사라져 성질이 달라진다.

### 2. 메시지 키는 반드시 `pack_id`

모델은 상태를 들고 있다(V2 링버퍼 61행, T1 오프셋, 지속 카운터).
**같은 팩의 연속 행이 같은 프로세스에 순서대로** 들어가야 한다.

- Kafka 는 **같은 파티션 안에서만** 순서를 보장한다 → 키를 `pack_id` 로
- `uvicorn --workers 1` 로 띄운다 → 워커가 여럿이면 상태가 조각난다
- 확장은 워커 수가 아니라 **파티션 수**로 한다 (파티션 N개 = 인스턴스 N개, 같은 `group_id`)

### 3. `scikit-learn` 버전 고정

번들 안에 sklearn 의 PCA / IsolationForest 객체가 그대로 들어 있다.
버전이 다르면 로드가 깨지거나 조용히 다르게 동작한다. `requirements.txt` 를 그대로 쓸 것.

기동 시 `source_sha8` 와 실제 `src/` 를 대조한다(`BD_VERIFY_HASH=1`).
불일치면 **기동을 거부한다.** 끄지 말 것.

---

## 배포

### 번들 만들기 (개발 쪽에서 1회)

```bash
python -m deploy.make_bundle
# -> models/battery_model_20260825_165511_b_option.bundle  (3.3 MB)

python -m deploy.make_bundle --inspect models/battery_model_*.bundle   # 내용 확인
```

번들에 들어가는 것:

| | |
|---|---|
| `model` | `model_chg_op.pkl` — PCA 10성분 + IsolationForest + 스케일 상수 |
| `reference_csv` | SOC 기준표 원문 (9피처 × 127구간 med/sigma) |
| `alarm` | `rule` 11.47 / `score` 33.72 — `BD_SCORE_KEY` 로 골라 쓴다 |
| `manifest` | 코드 해시 · 학습 팩 · 파라미터 |

가이드 스펙 모델(`model_chg.pkl`, 주성분 471개)은 넣지 않았다. 과적합으로 폐기된 쪽이다.

### 설치

```bash
pip install -r deploy/requirements.txt
cp deploy/.env.example .env      # 값을 채운다
```

### 확인

인프라에 붙이기 전에 로컬에서 전 경로를 확인한다.

```bash
python -m deploy.smoke_test            # 폴더 모드
python -m deploy.smoke_test --bundle   # 번들 모드
```

현재 결과 (두 모드 모두 ALL PASS, 결과 동일):

```
[1] dir:20260825_165511_b_option / 점수 rule / 임계 11.47
[2] 전처리 게이트 — 5초 격자 · 과도구간 · 세션 · 중복            PASS
[3] src(step9.replay) 대비 중앙값 2.529 = 2.529                  PASS
[4] M08CV01 에 -20 mV 주입 → 원인 지목 top1 100%                 PASS
[5] 판정 1행당 5.40 ms (5000 ms 예산) — 여유 926배               PASS
```

### 기동

```bash
uvicorn battery_detector:app --host 0.0.0.0 --port 8000 --workers 1
```

`--workers 1` 은 필수다. 위 함정 2 참조.

---

## 토픽

### 입력 `battery.raw` — BMS 1행 (1 Hz)

```json
{"pack_id": 1002, "ts": 1756..., "seq": 48211,
 "cells": [3.912, ...176개],  "temps": [24.1, ...32개],
 "soc": 47.3, "current": -45.2}
```

- `current` 필수 — 통전 판정(`|I| > 1.0 A`)과 과도구간 제외에 쓴다. 충전은 음수.
- `seq` 는 선택. 있으면 중복·역순 판별에 `ts` 대신 쓴다.

### 출력 `battery.score` — 판정 행마다 (대시보드 추이용)

```json
{"pack_id": 1002, "ts": ..., "soc": 47.3, "score": 2.53, "z_max": 2.53,
 "threshold": 11.47, "alarm": false, "warmup": false, "session_row": 128}
```

- **`soc` 는 필수 필드다.** 검출률이 발생 SOC 에 따라 2~6배 갈리므로,
  SOC 없이 집계한 운영 통계는 해석이 불가능하다.
- **`warmup: true` 구간은 온도 판정(T2/T3/T5)이 보류 중**이다.
  대시보드가 이 구간을 "정상"으로 표시하면 오해를 부른다.
- 보존기간 짧게(수 시간). 발행 빈도는 `BD_SCORE_EVERY` 로 조절(1=5초, 12=1분).

### 출력 `battery.alarm` — 알람 발생 시에만

```json
{"pack_id": 1002, "ts": ..., "soc": 42.7, "score": 14.84, "threshold": 11.47,
 "cause": "V9:M08CV01", "fault_type": "용량불량",
 "diagnosis": "용량불량(M08CV01, conf 0.82)",
 "top3": [["V9:M08CV01", 0.663], ["V1:M08CV01", 0.064], ["V8:M08CV01-CV02", 0.059]]}
```

- `cause` 는 SPE 기여도 1위 열. `피처:부품` 형식이라 정비 대상이 바로 나온다.
- 보존기간 길게. 알람은 드물고(0.21건/시간/팩) 중요하다.

---

## Streamlit 연결 — Kafka 를 직접 구독하지 말 것

Streamlit 은 상호작용마다 스크립트를 통째로 재실행한다. Kafka consumer 를 직접 붙이면
rerun 마다 consumer 가 재생성되고, 여러 브라우저 세션이 같은 consumer group 을 공유해
서로 메시지를 뺏어간다.

```
battery.score / battery.alarm  →  sink 워커  →  Redis / TimescaleDB
                                                      ↑ 조회
                                                  Streamlit (폴링)
```

Streamlit 은 `st_autorefresh` 로 5~10초 폴링하고 저장소에서 읽는다.

지연은 신경 쓸 필요 없다. 알람 자체에 `persist 2행 = 10초` 가 내장돼 있어서
Kafka 홉 몇 번과 폴링 5초는 전체 대비 무의미하다.

---

## 운영

### HTTP 엔드포인트 (관측·제어 전용)

| | |
|---|---|
| `GET /health` | consumer 살아있는지, 누적 오류·스킵 수 |
| `GET /stats` | 모델 정보 + 전 팩 상태 |
| `GET /packs/{id}` | 팩 하나의 상태 (받은 행·판정 행·알람 수·warmup 잔여) |
| `POST /packs/{id}/reset` | 팩 상태 강제 초기화 (다시 warmup 부터) |

**추론 경로가 아니다.** 행을 HTTP 로 받지 않는다.

### 재시작 시 warmup 공백

프로세스가 죽으면 팩별 상태가 날아가고 **재기동 후 300초간 온도 판정이 보류**된다.
배포할 때마다 이 공백이 생긴다.

상태는 팩당 약 90 KB(링버퍼 61×176 float64 가 대부분)라 100팩이면 9 MB다.
Redis 체크포인트로 없앨 수 있지만 **처음부터 필요하진 않다.** 먼저 `warmup` 플래그를
대시보드에 "판정 준비 중"으로 정직하게 노출하고, 배포 빈도가 문제가 될 때 도입한다.

### 알람 물량 예측

실측 오탐률 **0.210 건/시간**, 30팩 중 2팩에 몰려서 발생했다.
팩 100대면 시간당 약 21건이다. 알림 채널 설계 시 이 수치를 기준으로 잡는다.

### 상시 감시할 지표

`|z| > 6` 비율. 현재 정상 기준 **0.018%** 다.
이게 서서히 오르면 새 팩·계절·셀 노후화로 분포가 옮겨간 것이고,
**SOC 기준표 재학습 시점**이라는 신호다. 오탐률보다 먼저 움직이는 선행 지표다.

### 적용 범위

모델은 **충전 구간, SOC 26~89%** 로 학습됐다. `StreamGate` 가 `|I| > 1.0 A` 로 게이트하고,
SOC 가 표 범위를 벗어나면 기준표가 양 끝 값을 그대로 쓴다(외삽 금지).
방전 구간에 쓰려면 `dchg` 모드로 별도 학습이 필요하다.

---

## 설계 메모

**검출기는 Kafka·FastAPI 를 전혀 모른다.** 전송 계층을 바꿔도 `DetectorPool` 아래는 그대로다.

```python
from battery_detector import DetectorPool

pool = DetectorPool()
res = pool.feed(pack_id=1002, ts=..., cells=[...], temps=[...],
                soc=47.3, current=-45.2)
# res is None  -> 이 행은 판정에 쓰지 않았다 (솎임·과도구간·비통전·중복)
# res.alarm    -> 알람 여부, res.cause -> 원인 부품
```

`fastapi`/`aiokafka` 가 없으면 서비스 부분은 정의되지 않고 검출기만 동작한다.
배치 재생이나 테스트에 그대로 쓸 수 있다.

**추론을 스레드로 넘기지 않는다.** 순서 보장이 정확성 조건이라 동시 실행이 위험하고,
1행당 5 ms 로 5000 ms 예산 대비 926배 여유라 그럴 이유도 없다.

**전처리 순서는 STEP 1 과 같다** — 통전 판정 → 과도구간 제외 → 솎아내기.
전류 급변 직후 셀 전압은 내부저항 때문에 계단처럼 튀는데, 이건 셀 불량이 아니라
물리 현상이라 남겨두면 전 팩에서 오탐이 난다.
