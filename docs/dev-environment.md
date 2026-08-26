# 개발환경 — 무엇이 어디서 도는가

> 클론해서 컨테이너를 띄운 뒤 "지금 내 손에 뭐가 있는가" 를 한 장으로 보는 문서다.
> 의존성 추가·이미지 갱신 절차는 [`CONTRIBUTING.md`](../CONTRIBUTING.md),
> 파이프라인의 동작 원리는 [`docs/pipeline-overview.md`](pipeline-overview.md) 에 있다.
>
> 기준: 2026-08-26, 브랜치 `feature/frontend`

---

## 1. 한 장으로 보는 구성

```
 호스트                          docker compose
 ─────────                       ──────────────────────────────────────────────
 ./  ──(bind mount)──▶  /workspace   ← 여섯 컨테이너가 같은 폴더를 본다
                        │
                        ├─ dev         개발 작업용. sleep infinity 로 떠 있다
                        │              (VS Code Dev Containers 가 여기 붙는다)
                        │
                        ├─ postgres    원본 측정 480,949행
                        │      ▲ load_raw.py (최초 1회)
                        │      │
                        │   db/data/*.csv  102개 파일
                        │      │
                        │      ▼ database.py  배제 + 통전 필터 + 5초 정규화
                        │   sensor_generator.py ──┐
                        │                         │
                        ├─ broker      Kafka      ▼
                        │              battery.pack.measurement
                        │                    │
                        │                    ├──▶ api (main.py)
                        │                    │      detector.judge() ← 이상탐지 모델
                        │                    │      battery.pack.verdict ──┐
                        │                    │                             │
                        │                    └──▶ streamlit (app.py) ◀─────┘
                        │
                        └─ kafka-ui    토픽·메시지를 눈으로 확인
```

가상환경은 `/opt/venv` 다. `/workspace` 밖에 둬서 bind mount 에 덮이지 않는다.

---

## 2. 시작하기

```bash
docker compose up -d          # 여섯 컨테이너 기동
docker compose exec dev pytest tests/test_smoke.py   # 환경 확인
```

VS Code 라면 폴더를 열고 **Reopen in Container**. `dev` 에 붙고 나머지도 함께 뜬다.

최초 1회, DB 가 비어 있으면 원본을 적재한다.

```bash
docker compose exec dev python load_raw.py
```

---

## 3. 서비스와 포트

| 서비스 | 호스트에서 | 컨테이너끼리 | 하는 일 |
|---|---|---|---|
| `dev` | – | – | 스크립트 실행·테스트. `sleep infinity` |
| `api` | http://localhost:3000 | `api:3000` | FastAPI. 측정 구독 → 모델 판정 → 판정 발행 |
| `streamlit` | http://localhost:8501 | `streamlit:8501` | 대시보드 |
| `kafka-ui` | http://localhost:8080 | – | 토픽·메시지 확인 |
| `broker` | `localhost:9092` | **`broker:19092`** | Kafka (KRaft, 파티션 3) |
| `postgres` | `localhost:5432` | `postgres:5432` | `app` / `app` / `appdb` |

> **브로커 주소가 두 개인 이유.** 컨테이너 안에서는 `broker:19092`(INTERNAL),
> 호스트에서는 `localhost:9092`(EXTERNAL) 다. 컨테이너 안에서 `localhost:9092` 를
> 쓰면 자기 자신을 찾다가 실패한다.

---

## 4. 환경변수

`docker-compose.yml` 의 `x-app-env` 한 곳에서 `dev`·`api` 에 들어간다.
(`streamlit` 은 `PYTHONPATH` 와 `KAFKA_BROKER` 만 받는다 — DB 도 모델도 안 쓴다)

| 변수 | 값 | 왜 필요한가 |
|---|---|---|
| `PYTHONPATH` | `/workspace/src` | src 레이아웃인데 이미지에 프로젝트를 설치하지 않으므로(`--no-install-project`), 이게 없으면 `import battery_pack_defect_detection` 이 세 컨테이너 모두에서 실패한다 |
| `DATABASE_URL` | `postgresql+psycopg://app:app@postgres:5432/appdb` | 드라이버를 명시해야 psycopg3 를 찾는다 |
| `KAFKA_BROKER` | `broker:19092` | 위 INTERNAL 주소 |

### 모델 관련 (`BD_*`)

api 가 기동할 때 `detector.load()` 가 읽는다. 원본 설명은
[`src/.env.example`](../src/.env.example) 에 있다.

| 변수 | 값 | 비고 |
|---|---|---|
| `BD_ARTIFACT_BUNDLE` | `/workspace/models/battery_model_20260825_165511_b_option.bundle` | 코드에도 같은 기본값이 있지만 compose 에서 못 박는다 — 번들이 하나 더 생겼을 때 '가장 최근 것' 이 조용히 바뀌면 안 된다 |
| `BD_SRC_DIR` | `/workspace/src` | 모델 코드(`step*.py`)를 여기서 import 한다 |
| `BD_SOURCE_HZ` | **`0.2`** | **가장 틀리기 쉬운 값.** 아래 7절 참고 |
| `BD_SCORE_KEY` | `rule` | 임계 11.47. `score` 로 바꾸면 임계(33.72)도 번들에서 짝이 맞춰 따라온다 |
| `BD_VERIFY_HASH` | `1` | 기동 시 번들의 코드 해시와 `src/step*.py` 대조. 어긋나면 기동 거부 |

---

## 5. 코드가 어디 있는가

| 경로 | 무엇 |
|---|---|
| `main.py` | api. Kafka 구독 → 판정 → 발행. HTTP 는 들여다보는 창일 뿐 |
| `app.py` | streamlit 대시보드 |
| `database.py` | Postgres → dict 스트림. 배제 규칙·통전 필터·5초 정규화를 **읽는 시점에** 적용 |
| `sensor_generator.py` | dict → Kafka 메시지. 재생 도구 |
| `load_raw.py` | CSV → Postgres (최초 1회) |
| `inject_anomalies.py` | 이상 측정 주입 도구 (개발용) |
| `src/battery_pack_defect_detection/` | 공용 패키지 — `consumer.py`(Kafka), `detector.py`(모델 접착부) |
| `battery_detector.py` | **모델팀 인수인계본.** 전처리 게이트 + 검출기. Kafka·FastAPI 를 모른다 |
| `src/step*.py` 외 16개 | 모델 학습 코드. 번들에 복제하지 않고 **import 한다** |
| `models/*.bundle` | 모델 아티팩트 1개 (3.2 MB) |
| `db/data/*.csv` | 원본 102개 파일 |
| `tests/test_smoke.py` | **환경** 확인 (파이썬·Postgres·Kafka 왕복) |
| `tests/test_detector.py` | **모델** 확인 27건. 인프라 없이 돈다 |

---

## 6. 자주 쓰는 명령

모두 `dev` 컨테이너 안에서 돈다.

### 데이터 흘리기

```bash
# 팩 하나를 빠르게 (개발용). 기본 간격은 3초라 그대로 두면 오래 걸린다
docker compose exec dev python sensor_generator.py --serial 1000 --mode chg --limit 500 --interval 0.05

# 전량 재생 (38,058건 / 3초 간격이면 약 32시간)
docker compose exec dev python sensor_generator.py

# 이상치를 섞어 재생. 구간마다 120행에 한 번, 무작위 셀 하나를 6행 연속 -60mV 로 띄운다
# (띄운 행은 label: "defect" 로 나간다. --anomaly-burst / --anomaly-mv / --seed 로 조절)
docker compose exec dev python sensor_generator.py --serial 1000 --mode chg --anomaly-every 120
```

### 이상 주입 (일회성)

재생 중에 계속 섞으려면 위의 `--anomaly-every` 를 쓰고, 원하는 순간에
원하는 팩/셀 하나만 찔러 보려면 이 도구를 쓴다.

```bash
# 같은 셀을 여러 행에 걸쳐 띄운다. api 판정을 되읽어 결과를 알려 준다
docker compose exec dev python inject_anomalies.py --serial 1000
docker compose exec dev python inject_anomalies.py --serial 1000 --module 3 --cell 7 --mv -80
```

**측정을 먼저 충분히 흘린 뒤에** 주입한다. 모델은 충전 세션 시작 후
60 판정행(300초)이 지나야 온도까지 보고, 임계 초과가 2행 이어져야 알람을 낸다.

### 상태 보기

```bash
curl localhost:3000/health          # 컨슈머 살아 있는지 + 모델 로드됐는지
curl localhost:3000/stats           # 수신·판정·발행 수, 모델 설정, 팩별 상태
curl localhost:3000/sections        # 지금까지 본 (팩, 구간)
curl localhost:3000/verdicts/recent # 최근 이상/주의 판정
curl -X POST localhost:3000/packs/1000/reset   # 한 팩의 모델 상태 초기화
```

`/stats` 에서 먼저 볼 것:

| 항목 | 정상값 | 어긋나면 |
|---|---|---|
| `model.stride` | **`1`** | `BD_SOURCE_HZ` 가 틀렸다 (7절) |
| `model.loaded` | `true` | 번들·해시 문제. api 로그를 본다 |
| `received` | `judged` 합 + `skipped` | 판정 예외 — `consumer_errors` 를 본다 |
| `published` | `judged` 합과 같음 | 브로커 쪽 문제 |
| `packs[].warmup_left` | `0` | 아직 온도 판정 보류 중 |

### 테스트

```bash
docker compose exec dev pytest tests/test_smoke.py     # 환경 (Postgres·Kafka 필요)
docker compose exec dev pytest tests/test_detector.py  # 모델 (인프라 불필요)
```

---

## 7. 함정 — 겪은 것들

### 1) `BD_SOURCE_HZ` 는 `0.2` 다. `1.0` 이 아니다

원본 CSV 가 1초/행인 것을 보고 `1.0` 을 넣기 쉽다. 그런데 토픽에 흐르는 것은
`database.py` 가 5초 구간마다 첫 행만 남긴 **5초/행**이다.

`1.0` 을 넣으면 모델이 한 번 더 솎아 25초/행이 된다. **예외는 나지 않고 점수
분포도 거의 그대로다.** 대신 시간 창이 전부 5배 늘어난다 — 지속 조건 10→50초,
warmup 300→1500초. 감도와 오탐률만 조용히 달라진다.

→ 기동 후 **`/stats` 의 `model.stride` 가 `1`** 인지 확인한다.

### 2) 파일을 고치면 api 의 메모리가 날아간다

api 는 `uvicorn --reload` 로 뜬다. `/workspace` 의 `.py` 를 **아무거나** 고치면
재시작하고, 그때 다음이 전부 사라진다.

- `MeasurementBuffer` / `VerdictBuffer` (수신 이력)
- 모델의 팩별 상태 (링버퍼·온도 오프셋·지속 카운터) → warmup 이 다시 300초

그런데 **컨슈머 그룹 오프셋은 커밋된 채로 남는다.** 그래서 재시작 후에는 이미
읽은 메시지를 다시 읽지 않아 화면이 비어 보인다. 코드를 고쳤다면 generator 를
다시 돌리는 것이 가장 빠르다.

### 3) Kafka 를 지워도 앱 메모리는 안 지워진다

streamlit 은 `@st.cache_resource` 로 컨슈머와 버퍼를 프로세스 수명 내내 붙들고
있다. 토픽을 비워도 화면의 팩 목록에는 옛 데이터가 그대로 남는다.

**완전 초기화 절차** (순서가 중요하다):

```bash
# 1) 앱을 먼저 내린다 — 컨슈머가 그룹에서 빠져야 그룹을 지울 수 있다
docker compose stop api streamlit

# 2) 토픽 삭제
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic battery.pack.measurement
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic battery.pack.verdict

# 3) 컨슈머 그룹 삭제 — 한 번에 안 되면(GroupNotEmptyException) 잠시 뒤 재시도
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --delete --group api-measurement-consumer

# 4) 앱 재시작 — 메모리 버퍼와 모델 상태가 여기서 비워진다
docker compose up -d api streamlit
```

토픽은 다음 발행 때 자동으로 다시 생긴다(`KAFKA_NUM_PARTITIONS=3`).
Postgres 원본은 건드리지 않으므로 재적재는 필요 없다.

확인:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
curl localhost:3000/stats        # received 0 / sections [] 이면 깨끗하다
```

### 4) Git Bash 는 컨테이너 안의 절대경로를 바꿔 버린다

Windows 의 Git Bash 에서 `docker compose exec` 에 `/opt/...` 를 넘기면
`C:/Program Files/Git/opt/...` 로 변환되어 실패한다.

```bash
MSYS_NO_PATHCONV=1 docker compose exec broker /opt/kafka/bin/kafka-topics.sh ...
```

PowerShell 에서는 그냥 된다.

### 5) `--workers 1` 을 유지한다

모델이 팩마다 상태를 들고 있어서, 워커가 여럿이면 같은 팩의 행이 여러 프로세스로
흩어져 상태가 조각난다. 확장은 워커 수가 아니라 **Kafka 파티션 수**로 한다.

### 6) `db/init/*.sql` 은 최초 기동 시 1회만 실행된다

이미 볼륨이 있으면 `docker compose down -v` 로 지워야 다시 적용된다.
**`-v` 는 Postgres 데이터를 통째로 지운다** — 지우면 `load_raw.py` 를 다시 돌려야 한다.

---

## 8. 이미지에 들어 있는 것

`ghcr.io/astral-sh/uv:python3.13-bookworm-slim` 기반. 의존성만 굽고 앱 코드는 마운트한다.

| | 버전 | 비고 |
|---|---|---|
| Python | 3.13.11 | |
| numpy | 2.5.2 | |
| pandas | 3.0.5 | |
| **scikit-learn** | **1.9.0** | **정확히 고정.** 번들 안에 PCA/IsolationForest 객체가 pickle 로 들어 있어 버전이 다르면 로드가 깨지거나 조용히 다르게 동작한다 |
| fastapi | 0.141.1 | |
| streamlit | 1.61.1 | |
| confluent-kafka | 2.15.0 | api·streamlit 이 쓰는 Kafka 클라이언트 |

버전 확인:

```bash
docker compose exec dev python -c "import sklearn, numpy, pandas; print(sklearn.__version__, numpy.__version__, pandas.__version__)"
```

---

## 참고 문서

- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — 의존성 추가, 이미지 갱신, 컨테이너가 안 뜰 때
- [`docs/pipeline-overview.md`](pipeline-overview.md) — 파이프라인 동작 원리, 판정 로직, 주기
- [`docs/kafka-message-spec.md`](kafka-message-spec.md) — 측정 메시지 필드 전체 명세
- [`src/README.md`](../src/README.md) — 모델팀 인수인계 문서 (함정·운영 지표·적용 범위)
- [`src/.env.example`](../src/.env.example) — `BD_*` 환경변수 원본 설명
