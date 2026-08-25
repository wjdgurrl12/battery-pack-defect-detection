# 파이프라인 인사이트 — 무엇이 어떤 주기로 어떻게 흐르는가

> 팀원용 요약. 코드를 열지 않고도 "지금 이 시스템이 어떻게 돌아가는가" 를 알 수 있게 정리했다.
> 근거가 되는 파일과 상수 이름을 같이 적어 뒀으니, 숫자를 바꿀 일이 생기면 그 자리를 고치면 된다.
>
> 기준: 2026-08-25, 브랜치 `feature/frontend`

---

## 1. 한 장으로 보는 전체 흐름

```
 db/data/*.csv (원본 101개 파일, 480,949행)
        │  load_raw.py           ← 손대지 않고 그대로 적재 (구조 변환만)
        ▼
 Postgres  pack_measurement
        │  database.py           ← 배제 규칙 + 5초 정규화를 "읽을 때" 적용 → 125,488행
        ▼
 sensor_generator.py             ← 3초에 1건씩 Kafka 메시지로 변환·발행
        │
        ▼
 Kafka  topic: battery.pack.measurement   (schema_version 1.2.0)
        │
        ├──────────────▶ api (main.py)  group.id = api-measurement-consumer
        │                    │  detector.judge()  ← 모든 측정을 판정한다 (정상 포함)
        │                    ▼
        │               Kafka  topic: battery.pack.verdict  (schema_version 2.0.0)
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
| **Kafka 발행 (판정)** | 측정 1건당 판정 1건 → 실질 **3초에 1건** | `main.py` `VERDICT_TOPIC` |
| **Streamlit 화면 갱신** | **3초** | `app.py` `REFRESH_EVERY = "3s"` |
| 컨슈머 폴링 루프 | 1초 타임아웃으로 회전 | `consumer.py` `self._consumer.poll(1.0)` |
| 토픽 메타데이터 갱신 | 5초 (기본 5분은 너무 길다) | `consumer.py` `topic.metadata.refresh.interval.ms` |
| 이상 주입 도구 | 3초 간격 | `inject_anomalies.py` `--interval` 기본 3.0 |
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
- **전량 재생 시간**: 125,488건 × 3초 ≈ **104시간(4.4일)**.
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
| `src/…/detector.py` | 판정 | **모델이 들어올 자리.** 지금은 자리표 규칙 |
| `main.py` (api) | 구독 → 판정 → 발행 | 파이프라인의 심장. HTTP 는 들여다보는 창일 뿐 |
| `app.py` (streamlit) | 화면 | 측정으로 차트를, 판정으로 색을 칠한다. 판단은 하지 않는다 |
| `inject_anomalies.py` | 개발용 도구 | 진짜 측정의 셀 하나만 띄워 **측정 토픽에** 넣는다 (판정은 api 에게 맡긴다) |

---

## 4. 두 개의 토픽

### `battery.pack.measurement` — 측정 (schema_version **1.2.0**)

- **키**: `serial_number` 를 UTF-8 문자열로 (`"1000"`). 팩 하나가 한 파티션에 모여 순서가 보장된다.
- **헤더**: `mode` (chg/dchg). JSON 파싱 없이 kafka-ui 와 컨슈머가 충/방전을 구분한다.
- **구조**: `pack` / `soc` / `limits` / `cell` / `temperature` 로 묶이고,
  배열은 **16 × 11**(셀 전압), **16 × 2**(모듈 온도) 로 접혀 있다.
- **메타 필드**: `event_id`(UUID, 중복 제거용) · `seq`(구간 안에서 0부터, 유실 감지용) · `produced_at`

### `battery.pack.verdict` — 판정 (schema_version **2.0.0**)

- **키**: 측정과 같은 규약(`serial_number`). **헤더**: `state`
- **정상도 전부 발행한다.** 화면이 판정 카드와 타일을 칠하려면 정상 판정이 필요하기 때문이다.
- 나가는 필드: `state` · `module` · `cell` · `detail` · `model` · `verdict_id` · `detected_at`
- **나가지 않는 것: `score` / `threshold` / `module_scores`** (2026-08-25 결정, 필드 삭제라 major → 2.0.0)
  실제 모델이 점수를 돌려주지 않으므로 점수 관련 필드를 전부 걷어냈다.
  화면은 `state` 와 `module` 만 보고 칠한다 — 타일 16개 중 지목된 하나만 상태색, 나머지는 중립색.

> `verdict_id` 와 `detected_at` 은 `detector.judge()` 가 만들지 않고 **발행 시점에 api 가 붙인다.**
> judge 안에서 만들면 같은 입력에 다른 출력이 나와 테스트가 불가능해지기 때문이다.

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

## 7. 판정 로직 — 지금은 자리표(placeholder)

`detector.py` / 모델 `placeholder-deviation` v`0.0.0`

```
176개 셀 전압 → 팩 평균에서 가장 많이 벗어난 셀 하나를 찾는다
    이탈 ≥ 16.8 mV  →  anomaly (이상)
    이탈 ≥ 12.0 mV  →  warning (주의)
    그 외           →  normal  (정상, module/cell 은 None)
```

**모델을 붙일 때 갈아 끼우는 함수는 `predict()` 하나뿐이다.**

```python
predict(row, history) -> (
    {"state": "normal|warning|anomaly", "module": 1~16 | None, "cell": 1~11 | None},
    {"name": ..., "version": ...},
)
```

- `state` 는 `"normal" / "warning" / "anomaly"` 셋 중 하나여야 한다.
  `judge()` 가 검사해서 아니면 즉시 예외를 던진다 — 모델 교체 시 가장 흔한 사고가 라벨 불일치라서다.
- `history` 는 같은 (팩, 구간)의 **직전 64건**(`HISTORY_SIZE`)이다. 지금 규칙은 안 쓰지만
  시계열 모델용으로 인터페이스를 미리 열어 뒀다 (자기 자신은 `[:-1]` 로 뺀다).
- **점수를 돌려줄 필요가 없다.** 판정 메시지 구성·발행·화면은 그대로 돌아간다.

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
480,949행  pack_measurement (원시 CSV 101개 파일)
  -1,523행  sentinel(169) / 불완전 행(5) / 1043_dchg(1,349)
479,426행
     ÷ 5    5초 정규화 (1초 파일 72개만 해당, 5초 파일 30개는 그대로)
128,005행
  -2,517행  1043_chg (시간축 파손: 1,349행의 고유 타임스탬프가 35개뿐)
125,488건  ← Kafka 로 나가는 메시지 (충전 80,313 + 방전 45,175)
```

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

# 3. 이상 판정이 화면에 뜨는지 확인 (서로 다른 모듈 8곳을 순서대로 띄운다)
docker compose exec dev python inject_anomalies.py
docker compose exec dev python inject_anomalies.py --serial 1000 --count 8

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
3. **`inject_anomalies.py` 는 판정 토픽에 직접 쓰지 않는다.** 손으로 쓴 판정을 밀어 넣으면
   화면은 칠해지지만 정작 확인하려던 것 — api 가 이 측정을 이상으로 보는가 — 은 확인되지 않는다.
4. **이상 주입 시 이탈 폭에 주의.** 셀 하나만 올리면 팩 평균도 그쪽으로 1/176 만큼 끌려가므로
   실제 이탈은 올린 값의 175/176 이다. `anomaly` 경계가 16.8mV 라 25mV 부터 잡았다.
5. **`PYTHONPATH=/workspace/src`** 가 없으면 세 컨테이너 모두
   `import battery_pack_defect_detection` 이 실패한다. src 레이아웃인데 이미지에는
   의존성만 굽기(`--no-install-project`) 때문이다.
6. **의존성만 바뀌면 이미지를 다시 굽지 않아도 된다.** 각 컨테이너가 뜰 때 `uv sync --locked` 로
   맞춘다. 받는 쪽은 `git pull` 후 `docker compose up -d`. 이미지 재빌드는 `Dockerfile` 이 바뀔 때만.
7. **`db/init/*.sql` 은 Postgres 최초 기동 시 1회만 실행된다.** 이미 볼륨이 있으면
   `docker compose down -v` 로 지워야 다시 적용된다.

---

## 12. 숫자를 바꾸고 싶다면

| 바꾸고 싶은 것 | 고칠 자리 |
|---|---|
| 발행 속도 | `sensor_generator.SEND_INTERVAL_SECONDS` 또는 실행 시 `--interval` |
| 화면 갱신 주기 | `app.REFRESH_EVERY` (발행 주기와 맞추는 것을 권장) |
| 판정 임계값 | `detector._DEVIATION_ANOMALY_MV` / `_DEVIATION_WARNING_MV` |
| **모델 교체** | `detector.predict()` **하나만.** 나머지는 그대로 돌아간다 |
| 차트 창 선택지 | `app.WINDOW_CHOICES` (값은 건수, × 5초가 실제 시간) |
| 버퍼 보관량 | `consumer.PER_SECTION`(측정) / `consumer.ALERT_LIMIT`(알림) |
| 제외 구간 | `database.EXCLUDE_CHG` / `EXCLUDE_DCHG` — 재적재 불필요 |
| 색·타이포 | `app.PALETTE` / `app.TONES` |

---

## 13. 앞으로

- **`detector.predict()` 에 학습된 모델 연결** — 바깥 계약(state / module / cell)은 이미 고정되어 있다.
- `label` 필드가 아직 `None` 이다. 정답 라벨 확보 / 규칙 파생은 다음 단계의 주제.
- 보류 중인 **결측 의심 구간 34개** — 다시 빼기로 하면 `database.py` 의 두 집합에 되돌려 넣으면 된다
  (`docs/kafka-message-spec.md` 8.5절에 목록과 근거가 남아 있다).

---

### 참고 문서

- [`docs/kafka-message-spec.md`](kafka-message-spec.md) — 메시지 필드 전체 명세, 값 규약, 변경 이력
- [`kafkadata.json`](../kafkadata.json) — 측정 메시지 JSON Schema
- [`verdictdata.json`](../verdictdata.json) — 판정 메시지 JSON Schema
- [`README.md`](../README.md) — 개발 환경 구성
