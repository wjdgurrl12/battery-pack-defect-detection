"""FastAPI. Kafka 측정을 구독해 모델로 판정하고, 결과를 판정 토픽으로 발행한다.

    Kafka(battery.pack.measurement)
        │  컨슈머 스레드 (group.id = api-measurement-consumer)
        ▼
    detector.judge()          <- 학습 중인 모델이 들어올 자리 (predict)
        │  모든 측정을 판정한다. 정상도 발행한다 - 화면이 이 결과로 색을 칠한다
        ▼
    Kafka(battery.pack.verdict)
        │
        ▼
    streamlit (알림 + 타일)

판정 권한은 여기 한 곳뿐이다. streamlit 은 스스로 판단하지 않고 받은 것만
표시한다(2026-08-24 결정, detector.py docstring 참고).

모델은 이상 점수를 내지 않는다. 판정 메시지에 실리는 것은 state 와 문제
모듈·셀 지목뿐이다(2026-08-25 결정).

HTTP 엔드포인트는 파이프라인을 들여다보는 창이다. 데이터의 본 통로는 Kafka 이고,
HTTP 는 상태 확인과 디버깅에 쓴다.
"""

import json
import os
import threading
import uuid
from collections import Counter, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException

from battery_pack_defect_detection import consumer as kc
from battery_pack_defect_detection import detector

# --------------------------------------------------------------------------
# 설정
# --------------------------------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# 구독하는 토픽은 kc.TOPIC(battery.pack.measurement), 발행하는 토픽이 이것이다.
VERDICT_TOPIC = "battery.pack.verdict"

# 측정 컨슈머의 그룹. streamlit 과 다른 그룹이어야 양쪽 다 전 건을 받는다.
GROUP_ID = "api-measurement-consumer"

# judge 에 넘길 최근 이력 길이. 지금 자리표 규칙은 안 쓰지만,
# 시계열 모델이 들어오면 "직전 N건" 이 판단 재료가 된다.
HISTORY_SIZE = 64

# HTTP 로 보여줄 최근 이상/주의 판정 보관 수
RECENT_LIMIT = 100


# --------------------------------------------------------------------------
# 파이프라인 상태
#
# lifespan 에서 만들어 채우고, 엔드포인트가 읽는다. FastAPI 앱 하나에
# 파이프라인도 하나라서 모듈 전역으로 둔다.
# --------------------------------------------------------------------------

class Pipeline:
    """컨슈머·프로듀서와 그 통계를 한 덩어리로 묶는다."""

    def __init__(self) -> None:
        self.buffer: kc.MeasurementBuffer | None = None      # 받은 측정
        self.consumer: kc.ConsumerThread | None = None
        self.producer: Producer | None = None
        # 발행 통계. 컨슈머 스레드와 HTTP 스레드가 같이 만지므로 락으로 지킨다.
        self.lock = threading.Lock()
        self.judged = Counter()          # state 별 판정 수
        self.published = 0               # 브로커가 기록을 확인한 수 (콜백 기준)
        self.publish_errors = 0
        self.latest: dict[tuple[int, str], dict] = {}   # 구간별 마지막 판정
        self.recent: deque[dict] = deque(maxlen=RECENT_LIMIT)  # 최근 이상/주의


pipe = Pipeline()


# --------------------------------------------------------------------------
# 판정 -> 발행
# --------------------------------------------------------------------------

def _on_delivery(err, msg) -> None:
    """판정 메시지의 발행 결과 콜백.

    produce 는 큐잉일 뿐이고(sensor_generator 3단계에서 배운 그대로),
    실제로 브로커에 닿았는지는 여기로만 알 수 있다.
    """
    with pipe.lock:
        if err is None:
            pipe.published += 1
        else:
            pipe.publish_errors += 1


def handle_measurement(row: dict) -> None:
    """측정 한 건이 도착할 때마다 컨슈머 스레드가 부르는 함수. 파이프라인의 심장이다.

    1) 같은 구간의 최근 이력을 꺼내고   (시계열 모델 대비)
    2) 판정한 뒤                        (detector.judge - 항상 결과가 나온다)
    3) 발행 메시지로 완성해             (verdict_id / detected_at 은 여기서 붙인다)
    4) 판정 토픽으로 낸다               (키 = serial_number, 측정 토픽과 같은 규약)
    """
    # [:-1] 인 이유: 버퍼에는 방금 이 행까지 들어가 있다. history 는
    # '앞선' 측정들이어야 하므로 자기 자신을 뺀다.
    history = pipe.buffer.rows(row["serial_number"], row["mode"],
                               limit=HISTORY_SIZE + 1)[:-1]

    verdict = detector.judge(row, history)

    # judge 가 만들지 않는 두 값은 발행하는 쪽의 상태라 여기서 붙인다.
    # (judge 안에서 만들면 같은 입력에 다른 출력이 나와 테스트가 불가능해진다)
    message = {
        # 2.0.0: score / threshold / module_scores 삭제. 필드 삭제는 major 다
        # (docs/kafka-message-spec.md 의 버전 규칙).
        "schema_version": "2.0.0",
        "verdict_id": str(uuid.uuid4()),
        "detected_at": datetime.now(timezone.utc)
                       .isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        **verdict,
    }

    pipe.producer.produce(
        VERDICT_TOPIC,
        # 측정 토픽과 같은 키 규약. 팩 하나의 판정이 한 파티션에 모여
        # 발생 순서가 보장된다.
        key=str(message["serial_number"]).encode(),
        value=json.dumps(message, ensure_ascii=False).encode(),
        # state 를 헤더로도 실어 준다. kafka-ui 필터와 컨슈머의 빠른 분기용.
        # 측정 토픽이 mode 를 헤더에 싣는 것과 같은 패턴이다.
        headers=[("state", message["state"].encode())],
        on_delivery=_on_delivery,
    )
    # 밀린 발행 콜백을 처리할 기회. 안 부르면 published 가 영영 0 이다.
    pipe.producer.poll(0)

    # HTTP 로 들여다볼 수 있게 요약을 남긴다.
    with pipe.lock:
        pipe.judged[message["state"]] += 1
        pipe.latest[(message["serial_number"], message["mode"])] = message
        if message["state"] != "normal":
            pipe.recent.append(message)


# --------------------------------------------------------------------------
# 수명 주기
# --------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI 는 요청이 와야 도는 서버라, 계속 돌아야 하는 컨슈머는
    시작할 때 백그라운드 스레드로 띄워 두고 종료할 때 정리한다.
    """
    # 프로듀서 설정은 sensor_generator.make_producer 와 같은 이유로 같다:
    # acks=all(기록 확인까지 성공으로 안 침), idempotence(재전송 중복 방지).
    pipe.producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "enable.idempotence": True,
        "client.id": "api-verdict-producer",
    })

    # 측정 구독 시작. on_row 로 판정-발행이 이어진다.
    pipe.buffer, pipe.consumer = kc.start(
        KAFKA_BROKER, GROUP_ID, on_row=handle_measurement)

    yield   # ---- 여기서 서버가 요청을 받는다 ----

    # 종료: 컨슈머를 세우고, 큐에 남은 판정을 마저 내보낸다.
    # flush 없이 죽으면 큐에 있던 판정이 전송 시도조차 없이 사라진다
    # (sensor_generator 5단계의 try/finally 와 같은 원리).
    pipe.consumer.stop()
    pipe.producer.flush(10)


app = FastAPI(title="battery-pack-defect-detection api", lifespan=lifespan)


# --------------------------------------------------------------------------
# 엔드포인트 - 파이프라인을 들여다보는 창
# --------------------------------------------------------------------------

@app.get("/health")
def health():
    """컨테이너 healthcheck 가 부른다. 파이프라인 스레드 생존까지 확인한다."""
    alive = pipe.consumer is not None and pipe.consumer.is_alive()
    return {"status": "ok" if alive else "degraded", "consumer_alive": alive}


@app.get("/stats")
def stats():
    """수신·판정·발행 현황 한눈에. 어디서 새는지 여기서 보인다.

    received == judged 합 == published 면 파이프라인이 건강한 것이다.
    published 가 뒤처지면 브로커 쪽 문제, judged 가 뒤처지면 판정 예외
    (consumer.errors 로 드러난다) 를 의심한다.
    """
    # 발행 확인 콜백은 poll 안에서만 돈다(3단계 교훈). 마지막 발행 뒤에는
    # poll 할 계기가 없어 published 가 뒤처지므로, 들여다보는 순간 밀린
    # 콜백을 먼저 처리한다.
    if pipe.producer is not None:
        pipe.producer.poll(0)
    with pipe.lock:
        judged = dict(pipe.judged)
        published = pipe.published
        errors = pipe.publish_errors
    return {
        "consumer": pipe.buffer.stats() if pipe.buffer else None,
        "consumer_errors": pipe.consumer.errors if pipe.consumer else None,
        "judged": judged,
        "published": published,
        "publish_errors": errors,
        "model": {"name": detector.MODEL_NAME, "version": detector.MODEL_VERSION},
    }


@app.get("/sections")
def sections():
    """지금까지 받은 (팩, 구간) 목록과 건수."""
    return pipe.buffer.sections() if pipe.buffer else []


@app.get("/verdicts/latest/{serial_number}/{mode}")
def latest_verdict(serial_number: int, mode: str):
    """한 구간의 가장 최근 판정. 화면 디버깅용."""
    verdict = pipe.latest.get((serial_number, mode))
    if verdict is None:
        raise HTTPException(404, f"{serial_number} {mode} 의 판정이 아직 없습니다")
    return verdict


@app.get("/verdicts/recent")
def recent_verdicts(limit: int = 20):
    """최근 이상/주의 판정. 정상은 여기 쌓지 않는다 - 알림 대상만 본다."""
    with pipe.lock:
        items = list(pipe.recent)
    return items[-limit:][::-1]   # 최신이 앞에 오게
