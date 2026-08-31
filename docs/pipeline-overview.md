# 파이프라인 인사이트 — 무엇이 어떤 주기로 어떻게 흐르는가

> 팀원용 요약. 코드를 열지 않고도 "지금 이 시스템이 어떻게 돌아가는가" 를 알 수 있게 정리했다.
> 근거가 되는 파일과 상수 이름을 같이 적어 뒀으니, 숫자를 바꿀 일이 생기면 그 자리를 고치면 된다.
>
> 기준: 2026-08-26, 브랜치 `feature/frontend`
>
> **2026-08-26 갱신:** 자리표(placeholder) 판정을 실제 이상탐지 모델로 갈아 끼웠다.
> 바뀐 곳은 7절(판정 로직), 4.5절(판정 메시지 — `schema_version` 2.1.0), 11절(함정)이다.

---

## 1. 한 장으로 보는 전체 흐름

```
 db/data/*.csv (원본 102개 파일, 480,949행)
        │  load_raw.py           ← 손대지 않고 그대로 적재 (구조 변환만)
        ▼
 Postgres  pack_measurement
        │  database.py           ← 배제 + 통전 필터 + 5초 정규화를 "읽을 때" 적용 → 38,058행
        ▼
 sensor_generator.py             ← 3초에 1건씩 Kafka 메시지로 변환·발행
        │
        ▼
 Kafka  topic: battery.pack.measurement   (schema_version 1.2.0)
        │
        ├──────────────▶ api (main.py)  group.id = api-measurement-consumer
        │                    │  detector.judge()  ← 이상탐지 모델 (정상도 발행한다)
        │                    │                      models/battery_anomaly.pkl
        │                    │                      팩 단위 판정. 30행마다 누적분으로 재판정
        │                    ▼
        │               Kafka  topic: battery.pack.verdict  (schema_version 2.1.0)
        │                    │
        └──────────────▶ streamlit (app.py) ◀────────┘
             group.id = streamlit-dashboard-<uuid8>  (측정 → 차트·수신현황)
             group.id = streamlit-verdict-<uuid8>    (판정 → 타일색·판정카드·알림)
                         │
                         ▼
                 3초마다 스스로 다시 그린다 (st.fragment run_every="3s")
```

**설계의 핵심 규칙 하나:** 판정 권한은 `api` 한 곳뿐이다.
Streamlit 은 스스로 판단하지 않고 **받은 판정을 그대로 칠하기만** 한다.
(2026-08-24 결정 — 화면이 직접 판단하면 모델 도입 후 "타일은 정상인데 알림은 이상" 처럼 갈라진다.)

---

## 2. 주기 한눈에 보기 ⏱ — 가장 자주 묻는 것

| 무엇 | 주기 | 정의된 곳 |
|---|---|---|
| **원본 데이터의 측정 간격** | 1초 / 5초 혼재 → **5초로 통일** | `database.py` `RESAMPLE_SECONDS = 5` |
| **Kafka 발행 (측정)** | **3초에 1건** | `sensor_generator.py` `SEND_INTERVAL_SECONDS = 3` (`--interval` 로 변경) |
| **판정 (api)** | 측정이 **도착하는 즉시** (주기 없음) | `main.py` `handle_measurement` — 컨슈머 콜백에서 동기 실행 |
| **모델이 기대하는 격자** | **5초 / 행** (`RESAMPLE_SECONDS` 와 같아야 한다) | `detector.MEASUREMENT_SECONDS` |
| **Kafka 발행 (판정)** | 충전 측정 1건당 판정 1건 → 실질 **3초에 1건**. 방전·비통전·과도구간은 발행 없음 | `main.py` `VERDICT_TOPIC` |
| 판정 1건 처리 시간 | 약 **5.6 ms** (예산 5,000 ms 대비 900배 여유) | 측정 실측. 컨슈머 콜백에서 동기 실행해도 밀리지 않는 근거 |
| **Streamlit 화면 갱신** | **3초** | `app.py` `REFRESH_EVERY = "3s"` |
| 컨슈머 폴링 루프 | 1초 타임아웃으로 회전 | `consumer.py` `self._consumer.poll(1.0)` |
| 토픽 메타데이터 갱신 | 5초 (기본 5분은 너무 길다) | `consumer.py` `topic.metadata.refresh.interval.ms` |
| 헬스체크 | api·streamlit 10초 / postgres 5초 / broker 10초 | `docker-compose.yml` |

### 이 숫자들의 관계 (중요)

- **데이터는 5초짜리인데 3초마다 보낸다** → 재생 속도가 실제보다 약 **1.67배 빠르다.**
  "데이터가 몇 초짜리인가(5초)" 와 "얼마나 빨리 재생하는가(3초)" 는 별개의 값이다.
- **발행 3초 = 화면 갱신 3초** 로 맞춰 뒀다. 화면이 새 메시지 1건을 정확히 한 번 받아 그린다.
  발행을 3초보다 빠르게 하면, 판정 카드와 모듈 타일은 "구간별 **마지막** 판정" 하나만 보여주므로
  앞 건이 화면에 뜨기 전에 다음 건에 덮인다.
- **차트의 시간 축은 발행 주기가 아니라 데이터 간격으로 센다.**
  `WINDOW_CHOICES` 의 "10분" = 120건 × 5초. 그 120건이 화면에 다 차는 데 걸리는 실제 시간은
  120건 × 3초 = 6분이다.
- **전량 재생 시간**: 38,058건 × 3초 ≈ **32시간**.
  구간(팩+충/방전) 하나는 최대 2,517건이므로 약 2시간이다. 데모에서는 `--limit` 이나
  `--serial` / `--mode` 로 잘라 쓴다.

---

## 3. 파일별 역할

| 파일 | 하는 일 | 한 줄 요약 |
|---|---|---|
| `load_raw.py` | CSV → Postgres | 원본을 **거르지 않고** 그대로 적재. 구조 변환(날짜 합치기, 파일명→mode, 176/32컬럼→배열 2개)만 한다 |
| `database.py` | Postgres → dict 스트림 | 배제 규칙과 5초 정규화를 **읽는 시점에** 적용. 기준이 바뀌어도 600MB 재적재가 없다 |
| `sensor_generator.py` | dict → Kafka 메시지 | 명세(v1.2.0) 형태로 접어서 3초에 1건 발행 |
| `src/…/consumer.py` | Kafka → 버퍼 | api·streamlit 이 **함께 쓰는** 컨슈머. 측정용/판정용 두 벌 |
| `battery_anomaly.py` (루트) | 모델 | 모델팀 인수인계본. 오토인코더 2개 + 로버스트 통계. Kafka·FastAPI 를 모른다 |
| `pack_loader.py` (루트) | 모델 | 측정 → `PackData`. 학습과 추론이 같은 전처리를 거치게 한다 |
| `train_anomaly.py` (루트) | 모델 | 정상 50팩 학습 + 데모 9팩 검증 |
| `old/` | — | 2026-08-27 이전의 행 단위 모델. 어디에서도 import 하지 않는다 |
| `src/…/detector.py` | 판정 | 측정 메시지 ↔ 모델 사이의 **번역**. 판정 안 한 행은 `None` |
| `main.py` (api) | 구독 → 판정 → 발행 | 파이프라인의 심장. HTTP 는 들여다보는 창일 뿐 |
| `app.py` (streamlit) | 화면 | 측정으로 차트를, 판정으로 색을 칠한다. 판단은 하지 않는다 |

---

## 4. 두 개의 토픽 — 무엇을 실어 보내는가

토픽은 둘뿐이고, 성격이 정반대다. **측정은 무겁고 판정은 가볍다.**
실측 평균 크기는 측정 **2,119바이트**, 판정 **408바이트** — 건수는 같지만 대역폭은 판정 쪽이 1/5 다.

| | `battery.pack.measurement` | `battery.pack.verdict` |
|---|---|---|
| 보내는 쪽 | `sensor_generator.py` | api (`main.py`) |
| schema_version | **1.2.0** | **2.1.0** |
| 키 | `serial_number` UTF-8 문자열 (`"1000"`) | 같음 |
| 헤더 | `mode` = `chg` / `dchg` | `state` = `normal` / `warning` / `anomaly` |
| 파티션 | 3 | 3 |
| 필드 수 | 메타 7 + 그룹 5 + `label` | 12 + `detail` |
| 실측 크기 | 2,119 B (2,041~2,137) | 408 B |
| 값 | 센서가 잰 것 전부 | 판단과 지목만 |
| 건수 | 38,058건 (충전만·통전 구간만) | 그중 판정된 행만 |

키를 반드시 넣는 이유: 같은 키는 같은 파티션으로 가므로 **팩 1대의 시계열 순서가 보장된다.**
키가 없으면 라운드로빈으로 흩어져 `measured_at` 순서가 깨진다.
`mode` 를 키에 넣지 않는 것도 같은 이유다 — `1000_chg` 와 `1000_dchg` 가 다른 파티션으로
갈라진다(실측 6팩 중 4팩).

---

### 4.1 측정 메시지 — `battery.pack.measurement`

원본 CSV 는 **231개 컬럼이 한 줄에 평평하게** 늘어서 있다. 그대로 옮기지 않고
원본의 논리 구조대로 묶었다 — **요약 23 + 셀 176 + 온도 32**.

```
{
  schema_version   메시지 명세 버전     "1.2.0" 고정
  event_id         메시지 고유 ID       UUID. 중복 처리를 막는 근거
  produced_at      발행 시각            UTC. measured_at 과 다르다
  serial_number    팩 식별자            메시지 키와 같은 값
  measured_at      측정 시각            KST(+09:00)
  mode             충전/방전            CSV 에 없고 파일명에만 있던 값
  seq              재생 순번            (serial_number, mode) 안에서 0부터

  pack        { voltage, current, power }                        팩 전체 상태
  soc         { rsoc_min/max/avg, usoc_min/max/avg }             충전 상태
  limits      { chg_p_max, dchg_p_max, chg_i_max, dchg_i_max }   BMS 허용 한계
  cell        { v_min, v_max, dv, voltages[16][11] }             셀 전압 176개
  temperature { t_min, t_max, t_avg, values[16][2] }             온도 32개

  label            결함 라벨            현재 항상 null
}
```

#### `pack` — 팩 전체 상태

| 필드 | 단위 | 원본 컬럼 | 관측 범위 | 설명 |
|---|---|---|---|---|
| `voltage` | V | `Voltage` | 606.2 ~ 721.5 | 팩 총 전압 |
| `current` | A | `Current` | −121.5 ~ 137.2 | **음수 = 충전, 양수 = 방전** |
| `power` | **kW** | `Power` | −78.97 ~ 97.13 | `voltage × current / 1000` 파생값 |

> **`soh` 는 싣지 않는다** (v1.2.0 에서 제거). DB 실측 재확인(2026-08-25):
> 480,949행 중 **480,948행이 `0`, 1행이 `NULL`, 0 이 아닌 값은 0행.** 102개 파일 전부
> `min(soh) = max(soh) = 0` 이다. NULL 1행은 `1012_dchg.csv` 의 잘린 불완전 행이라
> `database.py` 의 배열 NULL 조건에서 이미 빠진다 — 즉 **발행 대상 38,058건 안에서는
> 전 행이 0** 이다. DB 에는 원시값이 남아 있으니, BMS 가 값을 채워 주는 날이 오면
> 명세에 다시 넣으면 된다.

#### `soc` — 충전 상태

`RSOC` 는 실제 용량 기준, `USOC` 는 사용자 표시용이다. **화면의 도넛은 `usoc_avg` 를 쓴다.**

| 필드 | 단위 | 원본 컬럼 | 관측 범위 |
|---|---|---|---|
| `rsoc_min` / `rsoc_max` / `rsoc_avg` | % | `RSOCmin/max/avg` | 0 ~ 92.00 |
| `usoc_min` / `usoc_max` / `usoc_avg` | % | `USOCmin/max/avg` | 0 ~ 100 |

하한 `0` 은 결측이 아니라 **만방(완전 방전) 시점에 BMS 가 실제로 내는 값**이다. 그대로 쓴다.

#### `limits` — BMS 허용 한계

현재 시점에 BMS 가 허용하는 충방전 상한. **이상 판정의 기준선으로 쓸 수 있다.**

| 필드 | 단위 | 원본 컬럼 | 관측 범위 |
|---|---|---|---|
| `chg_p_max` | kW | `ChgPmax` | 0 ~ 73.59 |
| `dchg_p_max` | kW | `DchgPmax` | 0 ~ 144.31 |
| `chg_i_max` | A | `ChgImax` | 0 ~ 107.0 |
| `dchg_i_max` | A | `DchgImax` | 0 ~ 200.0 |

#### `cell` — 셀 전압 (메시지의 대부분을 차지한다)

**`dv` 가 셀 불균형의 1차 지표이며, 결함 판정의 핵심 후보다.**

| 필드 | 단위 | 원본 컬럼 | 관측 범위 | 설명 |
|---|---|---|---|---|
| `v_min` | V | `Vmin` | 3.436 ~ 4.090 | BMS 보고 셀 최저 전압 |
| `v_max` | V | `Vmax` | 3.449 ~ 4.110 | BMS 보고 셀 최고 전압 |
| `dv` | **mV** | `DV` | 2 ~ 37 | 셀 전압 편차. 클수록 불균형 |
| `voltages` | V | `M01CV01`~`M16CV11` | 3.435 ~ 4.110 | **176개 셀 전압 전체.** `[16][11]` 2차원 |

> **주의: `v_min` / `v_max` 는 `voltages` 의 최소/최대와 일치하지 않을 수 있다.**
> BMS 가 별도로 보고하는 값이라 대부분 ±1mV 이내지만 **최대 64mV 어긋나는 행**이 있다.
> 셀 편차를 정밀하게 계산해야 한다면 `voltages` 에서 직접 구한다 —
> `detector._worst_cell()` 이 그렇게 하고 있다.

#### `temperature` — 온도

| 필드 | 단위 | 원본 컬럼 | 관측 범위 | 설명 |
|---|---|---|---|---|
| `t_min` / `t_max` / `t_avg` | °C | `Tmin` / `Tmax` / `Tavg` | 24 ~ 43 | 요약값 |
| `values` | °C | `M01T01`~`M16T02` | 25 ~ 43 | **32개 센서 온도 전체.** `[16][2]` 2차원 |

#### `label` — 결함 라벨

`"normal"` / `"warning"` / `"defect"` / `null`.
**원본에 정답 라벨이 없으므로 현재는 항상 `null` 이다.** 규칙 파생이나 라벨 확보 후에 채운다.

---

### 4.2 값 규약 — 메시지를 다루기 전에 알아야 할 4가지

**① 전류 부호: 음수가 충전이다.** 일반적인 직관과 반대다.

| `mode` | `current` | `power` | 전압 추이 |
|---|---|---|---|
| `chg` (충전) | **음수** | 음수 | 상승 |
| `dchg` (방전) | **양수** | 양수 | 하강 |

480,948행 중 위반이 3행(0.0006%)뿐이라 이 규약이 맞다.
부호를 뒤집어 해석하면 충전과 방전이 통째로 바뀌어 **판정 로직 전체가 틀어진다.**

**② 단위가 섞여 있다.** `voltage` 는 V, `current` 는 A 인데 `power` 만 **kW** 다.
`dv` 도 `v_min` / `v_max` 는 V 인데 혼자 **mV** 다. 섞어 쓰면 1000배 오차가 난다.

**③ 파생 관계.** 두 필드는 다른 필드에서 계산된 값이고, 전 행에서 성립을 확인했다.

```
power  =  voltage × current / 1000        (최대 오차 0.078 kW)
dv     =  (v_max - v_min) × 1000          (오차 0)
```

검증에 쓸 수 있다. 측정을 만들거나 고치는 쪽은 파생 컬럼을 반드시 같이
고쳐야 한다 — 한 행 안에서 이 관계가 깨지면 메시지가 스스로 모순된다.
(데모 팩 생성기가 Voltage/Vmin/Vmax/DV 를 다시 맞추는 이유가 이것이다)

**④ 시간대.** 원본 `Date` / `Time` 에 시간대 정보가 없어 **KST(+09:00)** 로 보고 오프셋을 붙인다.
`produced_at` 과 `detected_at` 만 UTC 다.

---

### 4.3 배열 인덱싱 — 176셀을 어떻게 찾는가

팩 하나는 **모듈 16개 × 셀 11개 = 176셀**, 온도 센서는 **모듈당 2개 = 32개**.

```
cell.voltages[m][c]      ↔  M{m+1:02d}CV{c+1:02d}      m: 0~15, c: 0~10
temperature.values[m][s] ↔  M{m+1:02d}T{s+1:02d}       m: 0~15, s: 0~1
```

바깥 인덱스가 **모듈**, 안쪽이 **셀 / 센서**다.

```python
# 3번 모듈(M03) 7번 셀(CV07) 전압
v = msg["cell"]["voltages"][2][6]

# 모듈별 평균 전압 — 모듈 단위 이상 탐지의 출발점
per_module = [sum(cells) / len(cells) for cells in msg["cell"]["voltages"]]
```

평평한 176칸이 아니라 2차원으로 둔 이유가 이것이다. `[2*11+6]` 같은 인덱스 계산이 사라진다.

> **컨슈머 쪽에서는 다시 평평해진다.** `consumer.flatten()` 이 `16×11 → 176`,
> `16×2 → 32` 로 펴서 DB 조회 결과와 **같은 모양의 dict** 로 돌려준다.
> 화면과 판정 코드가 기존 코드를 그대로 쓸 수 있게 하려는 것이다.
> 그래서 `detector` 는 평평한 176개 배열을 받는다.

---

### 4.4 실제로 흐른 측정 메시지

라이브 토픽에서 그대로 뜬 첫 메시지다 (`1000_chg.csv` 첫 행. 배열만 줄였다).

```json
{
  "schema_version": "1.2.0",
  "event_id": "018f3a2c-6b41-7c9e-a5d2-3f8e1b0c7d94",
  "produced_at": "2026-08-25T02:14:07.311Z",
  "seq": 0,
  "serial_number": 1000,
  "measured_at": "2020-08-04T15:51:49+09:00",
  "mode": "chg",
  "pack":   { "voltage": 641.3, "current": 0.0, "power": 0.0 },
  "soc":    { "rsoc_min": 33.43, "rsoc_max": 34.29, "rsoc_avg": 33.84,
              "usoc_min": 33.0,  "usoc_max": 34.0,  "usoc_avg": 34.0 },
  "limits": { "chg_p_max": 68.62, "dchg_p_max": 128.27,
              "chg_i_max": 107.0, "dchg_i_max": 200.0 },
  "cell":   { "v_min": 3.643, "v_max": 3.645, "dv": 2.0,
              "voltages": [[3.646, 3.645, 3.645, "…11개"], "…15개 모듈 더"] },
  "temperature": { "t_min": 31.0, "t_max": 32.0, "t_avg": 31.0,
                   "values": [[31.9, 32.0], "…15개 모듈 더"] },
  "label": null
}
```

```
key   = b'1000'            헤더 = [('mode', b'chg')]
크기   = 2,119바이트 (평균)   이 중 대부분이 cell.voltages 176개다
```

---

### 4.5 판정 메시지 — `battery.pack.verdict`

측정을 판정한 결과 1건. **정상도 전부 발행한다.**
다만 **측정 전 건이 판정되지는 않는다** — 아래 "판정하지 않는 행" 참고.

| 필드 | 타입 | 설명 |
|---|---|---|
| `schema_version` | string | `"2.1.0"` 고정 |
| `verdict_id` | UUID | 판정 고유 ID. **알림 중복 제거**에 쓴다 |
| `detected_at` | ISO 8601 (UTC) | api 가 판정한 시각 |
| `event_id` | UUID | **판정 대상 측정의 `event_id`.** 알림에서 원본 측정으로 되짚어 간다 |
| `serial_number` | integer | 팩 식별자. 메시지 키와 같다 |
| `mode` | `chg` / `dchg` | |
| `measured_at` | ISO 8601 (KST) | 센서 측정 시각. 측정 메시지에서 그대로 옮긴다 |
| `seq` | integer | 측정의 재생 순번 |
| `state` | `normal` / `warning` / `anomaly` | **모델이 내는 판정 그 자체.** 화면이 이 값으로 색을 고른다 |
| `module` | 1~16 \| **null** | 문제 모듈(M01~M16). 지목이 없으면 `null` |
| `cell` | 1~11 \| **null** | 문제 셀(CV01~CV11). 지목이 없으면 `null` |
| `fault_type` | string | **2.1.0 추가.** 불량 유형. `anomaly` 일 때만 채워진다 |
| `warmup` | boolean | **2.1.0 추가.** `true` 면 **판정이 아직 확정 전**(2026-08-27 뜻이 바뀌었다 — 아래) |
| `detail` | string | 사람이 읽는 요약. `"용량불량 M08 CV01"` / `"이상 없음"` |
| `model` | `{name, version}` | 이 판정을 낸 모델. 과거 알림이 어느 모델의 판단인지 남는다 |

**나가지 않는 것: `score` / `threshold` / `module_scores`**
(2026-08-25 결정, 2026-08-26 유지)
모델이 안에서 재구성 오차와 임계값을 쓰지만 그것은 모델의 사정이고, 토픽으로
나가는 것은 판정과 지목뿐이다. 화면이 쓰는 것은 `state` 와 `module` 둘뿐이라,
타일 16개 중 지목된 하나만 상태색이고 나머지는 중립색이다.

#### `state` 세 값이 모델의 무엇인가 (2026-08-27 개정)

**판정 단위가 행에서 팩(충전 세션)으로 바뀌면서 기준도 바뀌었다.** 예전 모델에는
'지속 조건(2 판정행)' 이 있었지만 새 모델에는 없다. 대신 **SOC 16칸이 얼마나
찼는가** 가 판정의 신뢰도를 가른다.

모델은 세션 전체의 곡선을 SOC 16칸으로 접어서 본다. 세션 초반에는 높은 SOC 칸이
통째로 비어 있고, 모델은 빈 칸을 앞뒤 값으로 보간해 채운다 — 그 보간값으로 낸
점수는 지어낸 값이다. 실측으로 세션 절반(칸 8/16)을 넘기면 판정이 더 뒤집히지
않았다.

| `state` | 모델의 상태 | 지목 |
|---|---|---|
| `normal` | 이상 없음 | 없음 |
| `warning` | 이상이 잡혔으나 SOC 칸이 8/16 미만이라 **미확정** | **있다** |
| `anomaly` | SOC 칸이 충분히 찬 상태에서 이상 | 있다 |

**`warning` 에도 지목이 실린다** — 예전에는 비어 있었다. 새 모델은 판정과 지목을
같이 내므로 지울 이유가 없고, 화면이 미확정 구간에도 어느 모듈을 의심하는지
보여줄 수 있는 편이 낫다.

SOC 칸이 4/16 에 못 미치면 `judge()` 가 아예 `None` 을 준다(4.5절).

#### 판정하지 않는 행

모델은 **학습 때와 같은 전처리를 거친 행만** 받는다. 아래는 판정 자체를 하지 않고,
**아무것도 발행하지 않는다.**

| | 왜 |
|---|---|
| 방전 (`mode=dchg`) | 모델은 충전 구간(SOC 26~89%)으로만 학습됐다 |
| 비통전 (`\|I\| ≤ 1.0 A`) | 충전이 멈춘 구간은 적용 범위 밖 |
| 과도구간 (전류 급변 직후 5행) | 셀 전압이 내부저항 때문에 계단처럼 튄다. 셀 불량이 아니라 물리 현상이라 남기면 전 팩에서 오탐 |
| 중복 | Kafka 는 at-least-once. 같은 행이 두 번 들어가면 모델의 링버퍼가 왜곡된다 |

**판정하지 않은 것에 `normal` 을 발행하지 않는다.** 그러면 컨슈머가
"아직 판정 전" 과 "정상" 을 구분할 수 없다. 화면은 판정이 매 측정마다 오지
않는다고 보고 마지막 판정을 들고 있어야 한다(`VerdictBuffer.latest_for` 가 이미 그렇다).

`/stats` 의 `skipped` 로 그 수를 볼 수 있고, `model.packs[].seen` / `used` 로 팩마다 볼 수 있다.

```json
{ "schema_version": "2.1.0",
  "verdict_id": "7c1e9a04-2b83-4f61-9d27-5ae0c3f81b44",
  "detected_at": "2026-08-26T02:14:07.412Z",
  "event_id":    "018f3a2c-6b41-7c9e-a5d2-3f8e1b0c7d94",
  "serial_number": 1003, "mode": "chg",
  "measured_at": "2020-08-07T16:25:38+09:00", "seq": 2007,
  "state": "anomaly", "module": 9, "cell": 3,
  "fault_type": "센싱와이어불량", "warmup": false,
  "detail": "센싱와이어불량 M09CV03",
  "model": { "name": "battery-anomaly-ae", "version": "7.505-0.809-2.169" } }
```

```json
{ "…": "정상일 때는 지목이 비어 있다. warmup 이면 판정이 아직 확정 전이다",
  "state": "normal", "module": null, "cell": null,
  "fault_type": "", "warmup": true, "detail": "이상 없음(판정 확정 전)" }
```

> **`warmup: true` 를 그냥 '정상' 으로 칠하면 안 된다.**
> SOC 16칸이 8칸을 넘기 전까지는 근거가 모자라 **판정이 뒤집힐 수 있다.** 실제로
> DEMO08(센서불량)은 세션 초반에 `용접불량 M02` 로 나왔다가 확정 시점에
> `센서불량 M14` 로 바뀐다. 화면에는 "판정 확정 전" 으로 정직하게 드러낸다.
> api 를 재시작하면 팩별 누적 버퍼가 날아가므로 이 구간이 다시 생긴다.

> **`module` / `cell` 은 1부터다** — 사람이 읽는 M03 / CV07 번호다.
> 배열 인덱스(0부터)와 1 차이가 나므로, 화면에서 타일을 짚을 때 `verdict["module"] - 1` 을 쓴다.
>
> **`verdict_id` 와 `detected_at` 은 `detector.judge()` 가 만들지 않고 발행 시점에 api 가 붙인다.**
> judge 안에서 만들면 같은 입력에 다른 출력이 나와 테스트가 불가능해지기 때문이다.

---

### 4.6 측정 1건이 화면의 색 하나가 되기까지

```
DB 행 (평평한 dict)
  │ sensor_generator.build_message()   16×11 로 접고 메타 7필드를 붙인다
  ▼
측정 메시지 2,119 B ──▶ Kafka ──▶ consumer.flatten()   다시 176개로 편다
                                       │
                                       ├──▶ MeasurementBuffer   화면의 차트
                                       │
                                       └──▶ detector.judge()    판정
                                                │
                                       판정 메시지 408 B ──▶ Kafka
                                                │
                                       VerdictBuffer ──▶ 타일 색 · 판정 카드 · 알림
```

같은 `event_id` 가 측정 메시지와 판정 메시지 양쪽에 실려 있어,
화면의 알림에서 원본 측정으로 되짚어 갈 수 있다.

---

## 5. 컨슈머 그룹 설계 — 왜 그룹 이름이 다 다른가

Kafka 는 **그룹 단위로 오프셋을 관리**한다. 그룹이 다르면 각자 전 건을 받고(fan-out),
같은 그룹이면 파티션을 나눠 갖는다(분산).

| 소비자 | group.id | 이유 |
|---|---|---|
| api | `api-measurement-consumer` | 고정 |
| streamlit (측정) | `streamlit-dashboard-<uuid8>` | api 와 달라야 양쪽 다 전 건을 받는다 |
| streamlit (판정) | `streamlit-verdict-<uuid8>` | 위와 같음 |

**streamlit 만 그룹 이름에 UUID 꼬리를 붙이는 이유**: 대시보드 프로세스가 둘 이상일 때
(개발 중 로컬 테스트 + 컨테이너, 또는 대시보드 2대) 그룹이 같으면 파티션을 나눠 갖게 되어
각자 일부 팩만 보게 된다. 화면은 늘 전체를 봐야 한다.
브로커 파티션은 3개다 (`KAFKA_NUM_PARTITIONS: 3`).

`auto.offset.reset` 은 양쪽 다 **earliest** 다. latest 로 두면 화면을 켠 뒤
새 메시지가 올 때까지 빈 화면이고, 구독 전에 발행된 것은 영영 못 본다.

---

## 6. 데이터 품질 방어선

| 문제 | 어떻게 막는가 | 어디에 보이는가 |
|---|---|---|
| **중복 수신** (Kafka 는 at-least-once) | `event_id` / `verdict_id` 를 최근 20,000개 기억(`SEEN_LIMIT`)해 걸러낸다 | 화면 수신 현황 `중복 n` |
| **유실** | `seq` 가 건너뛰면 센다 (`gaps`) | 화면 수신 현황 `유실 n` |
| **메모리 누수** | 구간마다 최근 3,000건만 (`PER_SECTION`. 구간 하나 최대 2,517건을 통째로 담는 크기) | – |
| **깨진 메시지** | try/except 로 세고 넘어간다. 한 건이 소비 루프를 세우면 안 된다 | `/stats` 의 `consumer_errors` |
| **발행 실패** | `acks=all` + `enable.idempotence` + `on_delivery` 콜백 | `/stats` 의 `publish_errors` |
| **센서 미응답값** | `voltage/v_min/v_max = 0`, `t_min/t_max/t_avg = -40` 인 행을 조회에서 제외 | `database.py` `SENTINEL_*` |
| **종료 시 큐 유실** | `try/finally` 로 반드시 `flush()` — produce 는 큐잉일 뿐이다 | – |

> **`producer.poll(0)` 을 안 부르면 발행 실패를 아무도 모른다.** 콜백은 poll/flush 중에만 실행된다.
> `/stats` 는 조회할 때 먼저 `poll(0)` 을 한 번 부른다 — 마지막 발행 뒤에는 poll 할 계기가 없어
> `published` 가 뒤처지기 때문이다.

---

## 7. 판정 로직 — 이상탐지 모델

`detector.py` / 모델 `battery-anomaly-ae`

### 무엇이 어디에 있는가

| | |
|---|---|
| `models/battery_anomaly.pkl` | 모델 아티팩트 1개 — AE 2개(MLPRegressor) + 스트림별 임계값 + 보정 점수 |
| `battery_anomaly.py` (저장소 루트) | 모델팀 인수인계본. 곡선 생성·검출기·임계 보정. **Kafka·FastAPI 를 전혀 모른다** |
| `pack_loader.py` (저장소 루트) | 측정 → `PackData`. **학습(CSV)과 추론(Kafka 행)이 같은 전처리를 거치게 한다** |
| `train_anomaly.py` (저장소 루트) | 정상 50팩 학습 + 데모 9팩 검증 |
| `src/battery_pack_defect_detection/detector.py` | 이 저장소의 접착부. 측정 누적 + 재판정 + 메시지 번역 |
| `old/` | 2026-08-27 이전의 행 단위 모델 일체. 어디에서도 import 하지 않는다 |

### 한 팩이 판정되기까지

**행 하나만 보고는 아무 말도 할 수 없다.** 모델은 충전 세션 전체의 곡선을 본다.

```
측정 1건 (176셀 · 32온도 · SOC)
  │
  ├ 방전인가                                  맞으면 → 판정 안 함
  ├ 역순으로 온 행인가                          맞으면 → 판정 안 함
  ▼
팩별 누적 버퍼에 쌓는다   (세션 공백 300초를 넘으면 비우고 다시 시작)
  │
  ├ 재판정 차례인가 (30행 = 2.5분마다)          아니면 → 판정 안 함
  ├ 100행 이상 · SOC 칸 4/16 이상인가           아니면 → 판정 안 함
  ▼
누적분 전체로 PackData 재구성 → SOC 16칸으로 접는다
  ▼
  ├ 센싱와이어불량 : 로버스트 z (팩 내부 비교)      임계 7.505
  ├ 용접불량     : AE 재구성 오차 (모듈 편차)     임계 0.809
  └ 센서불량     : AE 재구성 오차 (온도)         임계 2.169
  ▼
걸린 스트림 중 임계 대비 가장 큰 곳을 지목      (예: M09CV03)
  ▼
SOC 칸 8/16 미만이면 warning(미확정), 넘었으면 anomaly
```

재판정 1회에 약 **7 ms**(최대 12 ms). 예산(5초)의 700분의 1이라 컨슈머 콜백에서
그대로 돌린다. 816행짜리 팩 하나에 판정 22건이 나온다.

### 바깥과의 계약

```python
judge(row, history) -> dict | None      # None = 이 행은 판정하지 않았다
predict(row, history) -> (verdict, model) | None

verdict = {"state": "normal|warning|anomaly",
           "module": 1~16 | None, "cell": 1~11 | None,
           "fault_type": str, "warmup": bool}
```

- `state` 는 셋 중 하나여야 한다. `judge()` 가 검사해서 아니면 즉시 예외를 던진다 —
  모델 교체 시 가장 흔한 사고가 라벨 불일치라서다.
- **`None` 은 정상이 아니라 "판정 안 함" 이다.** 4.5절 "판정하지 않는 행" 참고.
- `history` 는 받지만 쓰지 않는다. `detector.py` 가 팩별 누적 버퍼를 직접 들고
  있어서 앞선 측정을 다시 받을 필요가 없다.
  인터페이스는 부르는 쪽을 고치지 않으려고 그대로 뒀다.

### 상태가 있다는 것의 의미

모델 자체는 상태가 없다. 대신 **`detector.py` 가 팩마다 측정을 쌓아 둔다.**
여기서 따라오는 제약이 셋이다.

1. **같은 팩의 연속 행이 같은 프로세스에 순서대로** 들어가야 한다.
   측정 토픽의 키가 `serial_number` 라 파티션 안에서 순서가 보장되고,
   컨슈머 스레드가 하나라 순서대로 들어간다. `uvicorn --workers 1` 을 유지할 것.
   확장은 워커 수가 아니라 **파티션 수**로 한다(파티션 N개 = 인스턴스 N개, 같은 `group_id`).
2. **재시작하면 누적이 날아간다.** 재기동 후에는 그 팩의 SOC 칸을 처음부터 다시
   채워야 하고, 100행·4칸을 넘을 때까지 판정이 없다. 배포할 때마다 이 공백이 생긴다.
3. **충전 세션이 끝나면 알아서 비워진다**(`measured_at` 공백 300초). 되감은
   재생처럼 강제로 끊어야 하면 `POST /packs/{serial_number}/reset`.

### 운영 지표 (2026-08-27 실측, 정상 50팩 보정 / 데모 9팩 검증)

| | |
|---|---|
| 오탐률 | 스트림당 2% (팩 1/50) — 운영점이 그렇다(`FP_RANK=1`). **세 스트림을 OR 로 묶은 통합 오탐은 6%**(팩 3/50)다. 이쪽이 실제 수치다 |
| 검출률 | 데모 9팩 **9/9**. 용접 8·12 mV, 센싱와이어 8 mV, 용량 25 mV, 센서 2.5 °C·고착 |
| 검출 한계 | 용접 2 mV 는 안 걸린다(DEMO09). 정상 팩의 자체 모듈 편차 폭(2.7~2.8 mV)과 구분되지 않는다 |
| 임계 | 셀 7.505 / 용접 0.809 / 센서 2.169 |
| 적용 범위 | 충전 구간, SOC 37.1~88.8%(모델의 격자 구간). 방전은 별도 학습이 필요하다 |

> 통합 오탐률의 신뢰구간은 표본 50팩 기준으로 넓다. 정상 팩이 늘어나면
> `train_anomaly.py` 로 재보정할 것.

---

## 8. 화면(Streamlit) 구성

```
┌─ 왼쪽 (1) ──────────┬─ 본문 (3.4) ─────────────────────────────────┐
│ 날짜 / 시각 카드      │ 헤더: PACK n · 충전/방전 · Kafka 실시간 수신 중 │
│ 충전 / 방전 선택      │ ┌ 판정 카드 (2) ────────┬ 충전량 도넛 (1) ──┐ │
│ 팩 목록 (2열, 스크롤)  │ │ 이상 / 주의 / 정상      │ usoc_avg %        │ │
│ 차트 표시 구간        │ │ M03 CV07 이탈 · seq    │ + 판정 배지        │ │
│ 파이프라인 수신 현황   │ └─────────────────────┴──────────────────┘ │
│  측정 n건 · n초 전    │ 모듈 타일 16개 (누르면 아래 차트가 그 모듈로)     │
│  판정 n건 · n초 전    │ ┌ M03 셀 전압 11채널 ────┬ M03 온도 2채널 ──┐ │
│  이상/주의/정상       │ │ (편차 보기 토글)        │                  │ │
│  유실 · 중복          │ └─────────────────────┴──────────────────┘ │
│                     │ 최근 알림 8건 (이상/주의만)                     │
└────────────────────┴────────────────────────────────────────────┘
```

**동작 원리**: Kafka 컨슈머는 백그라운드 스레드에서 쉬지 않고 버퍼를 채우고,
`@st.fragment(run_every="3s")` 가 붙은 `dashboard()` 가 3초마다 버퍼의 최신 내용을 다시 읽어 그린다.
사람이 새로고침할 필요가 없다. 컨슈머 스레드는 `@st.cache_resource` 안에서 만들어지므로
브라우저 탭이 여러 개여도, 화면이 몇 번을 다시 그려져도 **프로세스당 하나**다.

### 화면을 만들며 내린 판단들

- **커서 슬라이더 없음** — 스트림이 곧 재생 위치라 화면은 늘 버퍼의 끝(최신)을 본다.
- **도넛이 진행도 노릇도 한다** — 구간이 끝나는 자리가 SoC 로 정해져 있다
  (충전 34% → 100%, 방전 100% → 6%). 대신 반드시 두 가지를 붙인다:
  가운데 "충전량" 라벨(없으면 41% 를 진행도로 오해한다)과 방향 표기(`충전 중 ↑` / `방전 중 ↓`).
- **모듈 타일에 숫자를 쓰지 않는다** — 모델이 모듈별 점수를 내지 않으므로 짚힌 타일에만 상태 글자.
- **차트 툴팁은 선이 아니라 x축(시각) 기준 최근접** — 11개 선이 3.6~4.1V 에 뭉쳐 있어
  선 하나를 겨냥하기 어렵다. 커서를 대충 올려도 그 시각의 전 채널 값이 한 툴팁에 모인다.
- **"편차 보기" 토글** — 절대 전압 대신 모듈 평균과의 차이(mV). 뭉친 선이 벌어져 이탈이 바로 보인다.
- **팩 선택은 라디오가 아니라 버튼** — 라디오는 동그라미를 CSS 로 숨겨야 하는데
  Streamlit 이 내부 DOM 을 바꾸면 그 셀렉터가 깨진다.
- **시각은 `KST` 상수로 고정** — 컨테이너에 TZ 가 없어 `datetime.now()` 가 UTC 를 돌려준다
  (실측: 화면 05:06 / 실제 14:06).
- 색·타이포는 goorm Reference Design System 토큰을 그대로 쓴다 (`app.PALETTE` / `app.TONES`).

---

## 9. 데이터 규모

```
480,949행  pack_measurement (원시 CSV 102개 파일)
  -1,523행  sentinel(169) / 불완전 행(5) / 1043_dchg(1,349)
     -방전  EXCLUDE_DCHG — 방전 50구간 전량 (2026-08-26 결정)
  -2,517행  1043_chg (시간축 파손: 1,349행의 고유 타임스탬프가 35개뿐)
     ÷ 5    5초 정규화 (1초 파일 72개만 해당, 5초 파일 30개는 그대로)
 80,313건   ← 여기까지가 2026-08-25 기준
     -정지  |current| ≤ 1.0 A 인 행 (CURRENT_ON_AMPS, 2026-08-26 추가)
 38,058건   ← Kafka 로 나가는 메시지 (충전만)
```

**정지 행 42,255건이 빠진다(52.6%).** 대부분은 충전 완료(SOC 90) 후의 유지 구간이다 —
`1018_chg` 는 10,454행 중 9,497행이 그 구간이고, `1043_chg`·`1044_chg` 도 90% 가까이가 그렇다.
모델이 어차피 판정하지 않는 행이라(적용 범위 밖) 발행해 봐야 판정 없는 측정만 쌓인다.
원본에 `0 < |current| ≤ 1.0` 인 행은 **한 건도 없다** — 걸러지는 것은 전부 정확히 `0 A` 다.

- 팩 구성: **모듈 16개 × 셀 11개 = 176셀**, 모듈당 온도 센서 2개 → 32개
- 팩 번호 1000~1050, 구간은 충전 50 + 방전 50 = **100구간**
- **재생 순서는 충전 전량 → 방전 전량.** 정렬은 `database.py` 의 `ORDER BY mode, serial_number, …`
  한 곳에만 있다 (문자열 정렬로 `chg < dchg`).
  - 이 순서에서 `measured_at` 은 단조 증가하지 않는다. 충전 마지막(1050, 2021-03)에서
    방전 첫 구간(1000, 2020-08)으로 되돌아간다. **팩 하나만 놓고 보면 시계열은 온전하다.**
  - 화면 초반에 방전 목록이 비어 있는 것도 이 때문이며, 정상이다.

---

## 10. 실행 방법

```bash
# 0. 전체 기동
docker compose up -d

# 1. (최초 1회) 원본 CSV 적재
docker compose exec dev python load_raw.py

# 2. 측정 발행 시작 — 3초에 1건
docker compose exec dev python sensor_generator.py
docker compose exec dev python sensor_generator.py --serial 1000 --mode chg --limit 100
docker compose exec dev python sensor_generator.py --interval 1     # 빠르게 재생

# 3. 이상 판정이 화면에 뜨는지 확인 - 데모 팩에 고장이 들어 있어 그냥 재생하면 된다
docker compose exec dev python sensor_generator.py --serial 9003 --interval 0.05

# 4. 환경 점검
docker compose exec dev pytest
```

| 주소 | 무엇 |
|---|---|
| http://localhost:8501 | Streamlit 대시보드 |
| http://localhost:3000/stats | 수신·판정·발행 현황 (어디서 새는지 여기서 보인다) |
| http://localhost:3000/health | api + 컨슈머 스레드 생존 |
| http://localhost:3000/sections | 지금까지 받은 (팩, 구간) 목록과 건수 |
| http://localhost:3000/verdicts/recent?limit=20 | 최근 이상/주의 판정 |
| http://localhost:8080 | kafka-ui — 토픽과 메시지 직접 확인 |

### 건강한 상태의 판별법

`/stats` 에서 **`received` == `judged` 합계 == `published`** 면 정상이다.

- `published` 가 뒤처진다 → 브로커 쪽 문제
- `judged` 가 뒤처진다 → 판정 예외 (`consumer_errors` 로 드러난다)
- 화면의 "마지막 수신 n초 전" 이 계속 커진다 → generator 나 api 가 멈췄다

---

## 11. 알아두면 좋은 함정

1. **`Producer()` 는 연결하지 않는다.** 설정만 들고 즉시 돌아오고, 접속은 첫 발행 때
   백그라운드 스레드가 한다. 주소가 틀려도 이 함수는 "성공" 한다 — 에러는 한참 뒤 `on_delivery` 로 온다.
   "예외가 안 났으니 연결됐다" 고 믿으면 안 된다.
2. **`mode` 를 키에 넣으면 안 된다.** 파티션 배정에 관여하는 것은 키뿐이다. 키에 이어붙이면
   (`1000_chg`) 같은 팩의 충전과 방전이 다른 파티션으로 갈라진다(실측 6팩 중 4팩). 그래서 헤더에 뒀다.
3. **판정 토픽에 손으로 쓴 판정을 밀어 넣지 않는다.** 화면은 칠해지지만 정작
   확인하려던 것 — api 가 이 측정을 이상으로 보는가 — 은 확인되지 않는다.
4. **`PYTHONPATH=/workspace/src`** 가 없으면 세 컨테이너 모두
   `import battery_pack_defect_detection` 이 실패한다. src 레이아웃인데 이미지에는
   의존성만 굽기(`--no-install-project`) 때문이다.
5. **의존성만 바뀌면 이미지를 다시 굽지 않아도 된다.** 각 컨테이너가 뜰 때 `uv sync --locked` 로
   맞춘다. 받는 쪽은 `git pull` 후 `docker compose up -d`. 이미지 재빌드는 `Dockerfile` 이 바뀔 때만.
6. **`db/init/*.sql` 은 Postgres 최초 기동 시 1회만 실행된다.** 이미 볼륨이 있으면
   `docker compose down -v` 로 지워야 다시 적용된다.

### 모델 쪽 함정 — 어겨도 예외가 안 나고 조용히 틀린다

8. **`judged` 가 `received` 보다 훨씬 작은 것은 정상이다.** 팩 단위 모델이라
   30행마다 한 번만 판정한다(`detector.REPREDICT_EVERY_ROWS`). 816행짜리 팩
   하나에 판정 22건이다. 예전 행 단위 모델처럼 1:1 로 나오지 않는다.
9. **`scikit-learn` 은 `==1.9.0` 으로 고정한다.** `battery_anomaly.pkl` 안에 sklearn 의
   MLPRegressor 객체가 pickle 로 그대로 들어 있어서, 버전이 다르면 로드가 깨지거나
   조용히 다르게 동작한다. `pyproject.toml` 의 `>=` 로 되돌리지 말 것.
10. **`uvicorn --workers 1` 을 유지한다.** `detector.py` 가 팩마다 측정을 쌓아 둬서
    워커가 여럿이면 같은 팩의 행이 여러 프로세스로 흩어져 곡선이 조각난다. 7절 참고.
11. **`database.py` 의 통전 필터를 빼면 안 된다.** 예전 모델은 비통전 행이 섞여
    들어와도 `StreamGate` 가 스스로 걸러냈지만, **새 모델에는 그 방어가 없다.**
    정지 행이 곡선에 섞이면 SOC 칸 평균이 오염되고 **예외는 나지 않는다.**
    학습도 같은 상수를 쓴다(`pack_loader` 가 `database.CURRENT_ON_AMPS` 를 import).
12. **`judge()` 가 `None` 을 주면 아무것도 발행하지 않는다.** '정상' 으로 바꿔 발행하면
    화면이 "아직 판정 전" 과 "정상" 을 구분할 수 없다. 4.5절 참고.
13. **`warmup: true` 인 판정은 뒤집힐 수 있다.** SOC 칸이 8/16 을 넘기 전까지는
    근거가 모자란다. 그 구간의 지목을 확정된 것처럼 다루면 정비 대상이 엉뚱해진다.
14. **통전 필터와 세션 경계는 한 쌍이다.** `database.py` 가 정지 행을 발행하지 않게 되면서
    "정지가 이어진다 → 충전 세션이 끝났다" 를 알아채던 길이 막혔다.
    그 자리를 `detector.SESSION_GAP_SECONDS` 가 대신한다 —
    `measured_at` 공백이 300초를 넘으면 누적 버퍼를 비운다.
    **`CURRENT_ON_AMPS` 필터를 켜고 이 처리를 빼면** 같은 팩의 다음 충전이 앞 세션에
    이어 붙어 SOC 축이 두 번 왕복한다. **예외는 나지 않는다.**
    (`tests/test_detector.py::test_session_gap_starts_a_new_pack` 가 이것을 잡는다)
15. **모델을 다시 학습하면 임계가 바뀐다.** `models/battery_anomaly.pkl` 을 갈아
    끼우면 `/stats` 의 `model.version`(임계 세 개를 이어 붙인 문자열)이 달라진다.
    과거 판정과 비교할 때 이 값을 먼저 확인할 것.

---

## 12. 숫자를 바꾸고 싶다면

| 바꾸고 싶은 것 | 고칠 자리 |
|---|---|
| 발행 속도 | `sensor_generator.SEND_INTERVAL_SECONDS` 또는 실행 시 `--interval` |
| 화면 갱신 주기 | `app.REFRESH_EVERY` (발행 주기와 맞추는 것을 권장) |
| 판정 임계값 | pkl 안에 들어 있다. 직접 고치지 말고 `battery_anomaly.FP_RANK`(오탐 운영점)를 바꿔 `train_anomaly.py` 를 다시 돌린다 |
| 재판정 주기 | `detector.REPREDICT_EVERY_ROWS` (30행 = 2.5분) |
| 판정 시작 조건 | `detector.MIN_ROWS` / `MIN_COVERAGE` / `STABLE_COVERAGE` |
| **모델 교체** | 새 pkl 을 `models/` 에 두고 `docker-compose.yml` 의 `BD_ANOMALY_MODEL` 을 바꾼다 |
| 모델 입력 주기 | `detector.MEASUREMENT_SECONDS`. **`database.RESAMPLE_SECONDS` 와 반드시 같이 바꾼다** (`tests/test_detector.py` 가 어긋나면 실패한다) |
| 데모 팩 | `database.DEMO_SERIALS` / `DEMO_PACKS`. 재생은 `sensor_generator.py`(기본), 원본은 `--original` |
| 차트 창 선택지 | `app.WINDOW_CHOICES` (값은 건수, × 5초가 실제 시간) |
| 버퍼 보관량 | `consumer.PER_SECTION`(측정) / `consumer.ALERT_LIMIT`(알림) |
| 제외 구간 | `database.EXCLUDE_CHG` / `EXCLUDE_DCHG` — 재적재 불필요 |
| 통전 기준(정지 행 제외) | `database.CURRENT_ON_AMPS`. **학습도 이 상수를 그대로 쓴다**(`pack_loader`). 끄면 판정이 달라진다 — 새 모델에는 자체 통전 게이트가 없다 |
| 세션 경계 기준 | `detector.SESSION_GAP_SECONDS` — `load()` 가 모델의 `idle_reset_rows × 5초` 로 채운다. 손으로 고치지 말고 모델 설정을 고칠 것 |
| 색·타이포 | `app.PALETTE` / `app.TONES` |

---

## 13. 앞으로

- ~~`detector.predict()` 에 학습된 모델 연결~~ — **2026-08-26 완료.**
- **화면이 `warmup` 을 표시하도록** — 지금은 판정 메시지에 실려만 있고 `app.py` 가 쓰지 않는다.
  이 구간의 `normal` 은 온도를 아직 안 본 것이라, 그냥 '정상' 으로 칠하면 오해를 부른다.
  "판정 준비 중" 배지가 필요하다.
- **화면이 `fault_type` 을 표시하도록** — 판정 카드의 `detail` 에는 이미 들어가 있지만,
  유형별 집계나 필터가 필요해지면 별도 필드로 쓰는 편이 낫다.
- **재시작 시 누적 공백 없애기** — 누적 버퍼를 Redis 에 체크포인트하면 없앨 수 있지만
  처음부터 필요하진 않다. 배포 빈도가 문제가 될 때 도입한다.
- **방전 구간 판정** — 지금은 판정하지 않는다. `dchg` 모드로 별도 학습이 필요하다.
- `label` 필드가 아직 `None` 이다. 정답 라벨 확보 / 규칙 파생은 다음 단계의 주제.
- 보류 중인 **결측 의심 구간 34개** — 다시 빼기로 하면 `database.py` 의 두 집합에 되돌려 넣으면 된다
  (`docs/kafka-message-spec.md` 8.5절에 목록과 근거가 남아 있다).

---

### 참고 문서

- [`docs/kafka-message-spec.md`](kafka-message-spec.md) — 메시지 필드 전체 명세, 값 규약, 변경 이력
- [`kafkadata.json`](../kafkadata.json) — 측정 메시지 JSON Schema
- [`verdictdata.json`](../verdictdata.json) — 판정 메시지 JSON Schema
- [`docs/ae_model.md`](ae_model.md) · [`diagnostics.md`](diagnostics.md) · [`joint_anomaly.md`](joint_anomaly.md) — 모델 설계 근거 실험 기록
- [`old/README.md`](../old/README.md) — 2026-08-27 이전의 행 단위 모델
- [`README.md`](../README.md) — 개발 환경 구성
