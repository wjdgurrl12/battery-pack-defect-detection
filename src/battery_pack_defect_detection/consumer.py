"""Kafka 측정 메시지 컨슈머. api 와 streamlit 이 함께 쓴다.

    sensor_generator --> Kafka --> ┬-> api        (group.id=api-measurement-consumer)
                                  └-> streamlit  (group.id=streamlit-dashboard)

두 소비자는 **컨슈머 그룹이 다르다.** Kafka 는 그룹 단위로 오프셋을 관리하므로
그룹이 다르면 각자 모든 메시지를 받는다(fan-out). 같은 그룹으로 묶으면 파티션을
나눠 갖게 되어 api 는 1000번 팩만, streamlit 은 1001번 팩만 보는 식이 된다.

메시지 형식은 docs/kafka-message-spec.md 를 따른다. 이 모듈은 받은 메시지를
DB 조회 결과와 같은 평평한 dict 로 되돌려서(flatten) 화면과 API 가 기존 코드를
그대로 쓸 수 있게 한다.
"""

import json
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

TOPIC = "battery.pack.measurement"

# 구간(팩+충방전)마다 최근 몇 건을 들고 있을지. 한 구간이 최대 2,517건이라
# 3,000이면 구간 하나를 통째로 담을 수 있다.
PER_SECTION = 3000

# 중복 판정에 쓸 event_id 를 얼마나 기억할지. 무한히 쌓으면 메모리가 샌다.
SEEN_LIMIT = 20000

MODULE_COUNT = 16


def flatten(message: dict) -> dict:
    """Kafka 메시지를 DB 조회 결과와 같은 모양의 평평한 dict 로 바꾼다.

    메시지는 pack/soc/limits/cell/temperature 로 묶여 있고 배열은 16 x 11 로
    접혀 있다. 화면과 판정 코드는 평평한 176개 배열을 기대하므로 여기서 펼친다.
    database.load_measurements 의 컬럼명과 일부러 똑같이 맞췄다.
    """
    pack, soc = message["pack"], message["soc"]
    limits, cell, temp = message["limits"], message["cell"], message["temperature"]

    return {
        # 식별 / 파이프라인 장치
        "measured_at": datetime.fromisoformat(message["measured_at"]),
        "serial_number": message["serial_number"],
        "mode": message["mode"],
        "seq": message["seq"],
        "event_id": message["event_id"],
        "produced_at": message["produced_at"],
        # 팩 상태
        "voltage": pack["voltage"], "current": pack["current"], "power": pack["power"],
        # 충전 상태
        "rsoc_min": soc["rsoc_min"], "rsoc_max": soc["rsoc_max"], "rsoc_avg": soc["rsoc_avg"],
        "usoc_min": soc["usoc_min"], "usoc_max": soc["usoc_max"], "usoc_avg": soc["usoc_avg"],
        # BMS 한계
        "chg_p_max": limits["chg_p_max"], "dchg_p_max": limits["dchg_p_max"],
        "chg_i_max": limits["chg_i_max"], "dchg_i_max": limits["dchg_i_max"],
        # 셀 전압: 16 x 11 -> 176
        "v_min": cell["v_min"], "v_max": cell["v_max"], "dv": cell["dv"],
        "cell_voltages": [v for module in cell["voltages"] for v in module],
        # 온도: 16 x 2 -> 32
        "t_min": temp["t_min"], "t_max": temp["t_max"], "t_avg": temp["t_avg"],
        "module_temps": [v for module in temp["values"] for v in module],
        "label": message.get("label"),
    }


class MeasurementBuffer:
    """받은 측정을 (팩, 구간)별로 들고 있는 스레드 안전 버퍼.

    컨슈머 스레드가 add 로 쓰고 웹 요청 스레드가 rows/sections 로 읽으므로
    락이 필요하다. 읽을 때는 복사본을 돌려줘서, 호출자가 데이터를 훑는 동안
    컨슈머가 뒤에서 리스트를 바꿔도 깨지지 않게 한다.
    """

    def __init__(self, per_section: int = PER_SECTION):
        self._per_section = per_section
        self._rows: dict[tuple[int, str], deque] = defaultdict(
            lambda: deque(maxlen=per_section))
        self._seen: deque[str] = deque(maxlen=SEEN_LIMIT)
        self._seen_set: set[str] = set()
        self._last_seq: dict[tuple[int, str], int] = {}
        self._lock = threading.Lock()
        self.received = 0      # 처리한 메시지 수
        self.duplicates = 0    # event_id 가 겹쳐 버린 수
        self.gaps = 0          # seq 가 건너뛴 횟수(유실 추정)
        self.last_at: datetime | None = None

    def add(self, row: dict) -> bool:
        """한 건을 넣는다. 중복이면 False.

        Kafka 는 at-least-once 라 같은 메시지가 두 번 올 수 있다. 명세가
        event_id 를 준 이유가 이것이고, 여기가 그 값을 실제로 쓰는 자리다.
        """
        key = (row["serial_number"], row["mode"])
        with self._lock:
            event_id = row["event_id"]
            if event_id in self._seen_set:
                self.duplicates += 1
                return False

            # deque 가 넘치면 가장 오래된 id 도 함께 잊는다
            if len(self._seen) == self._seen.maxlen:
                self._seen_set.discard(self._seen[0])
            self._seen.append(event_id)
            self._seen_set.add(event_id)

            # seq 는 (팩, 구간) 안에서 0부터 1씩 오른다. 건너뛰면 유실이다.
            previous = self._last_seq.get(key)
            if previous is not None and row["seq"] > previous + 1:
                self.gaps += 1
            self._last_seq[key] = row["seq"]

            self._rows[key].append(row)
            self.received += 1
            self.last_at = datetime.now(timezone.utc)
            return True

    def sections(self) -> list[dict]:
        """지금까지 본 (팩, 구간) 목록. 화면의 팩 목록이 이걸 쓴다."""
        with self._lock:
            return [{"serial_number": serial, "mode": mode,
                     "steps": len(rows), "last_seq": self._last_seq.get((serial, mode))}
                    for (serial, mode), rows in sorted(self._rows.items(),
                                                       key=lambda kv: (kv[0][1], kv[0][0]))]

    def rows(self, serial_number: int, mode: str, limit: int | None = None) -> list[dict]:
        """한 구간의 측정을 오래된 것부터 돌려준다. limit 은 뒤에서 세어 자른다."""
        with self._lock:
            rows = list(self._rows.get((serial_number, mode), ()))
        return rows if limit is None else rows[-limit:]

    def latest(self, serial_number: int, mode: str) -> dict | None:
        """가장 최근 한 건."""
        rows = self.rows(serial_number, mode, limit=1)
        return rows[0] if rows else None

    def stats(self) -> dict:
        """수신 현황. 화면 하단과 API 의 상태 확인에 쓴다."""
        with self._lock:
            return {"received": self.received, "duplicates": self.duplicates,
                    "gaps": self.gaps, "sections": len(self._rows),
                    "last_at": self.last_at.isoformat() if self.last_at else None}


class ConsumerThread(threading.Thread):
    """Kafka 를 계속 읽어 버퍼를 채우는 백그라운드 스레드.

    데몬 스레드로 돌리므로 프로세스가 끝나면 함께 사라진다. 그래도 stop()
    으로 컨슈머를 닫아 주는 편이 낫다 - 그룹에서 깔끔히 빠져나가면 다음
    기동 때 리밸런스를 기다리지 않는다.
    """

    def __init__(self, broker: str, group_id: str, buffer: MeasurementBuffer,
                 topic: str = TOPIC, from_beginning: bool = True,
                 on_row=None):
        super().__init__(name=f"kafka-{group_id}", daemon=True)
        self._buffer = buffer
        self._topic = topic
        # 메시지 한 건이 버퍼에 들어간 '직후' 불리는 콜백. api 가 여기서
        # 판정과 발행을 한다. streamlit 처럼 버퍼만 필요하면 안 넘기면 된다.
        # 중복(event_id 겹침)으로 버려진 메시지에는 불리지 않는다.
        self._on_row = on_row
        self._stop = threading.Event()
        self._consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": group_id,
            # 기본은 earliest 다. latest 로 두면 화면을 켠 뒤 새 메시지가
            # 올 때까지 빈 화면이고, 구독 전에 발행된 것은 영영 못 본다.
            # 토픽이 아직 없을 때 구독하면 파티션 배정 자체가 늦는데, 그
            # 사이에 온 메시지까지 놓치게 된다.
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            # 토픽이 나중에 생겨도 빨리 알아채게 한다(기본 5분은 너무 길다)
            "topic.metadata.refresh.interval.ms": 5000,
            "enable.auto.commit": True,
            "client.id": group_id,
        })
        self.errors = 0

    def run(self) -> None:
        self._consumer.subscribe([self._topic])
        while not self._stop.is_set():
            message = self._consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                # 파티션 끝에 닿은 것은 오류가 아니다
                if message.error().code() != KafkaError._PARTITION_EOF:
                    self.errors += 1
                continue
            try:
                row = flatten(json.loads(message.value()))
                # add 가 False 면 중복이라 버린 것 - 콜백도 부르지 않는다
                if self._buffer.add(row) and self._on_row is not None:
                    self._on_row(row)
            except Exception:
                # 형식이 어긋난 메시지나 콜백 내부 오류 하나가
                # 소비 루프를 세우면 안 된다. 세고 다음으로 넘어간다.
                self.errors += 1
        self._consumer.close()

    def stop(self) -> None:
        self._stop.set()


def start(broker: str, group_id: str, from_beginning: bool = True,
          on_row=None) -> tuple[MeasurementBuffer, ConsumerThread]:
    """버퍼와 컨슈머 스레드를 만들어 돌린다.

        buffer, thread = start(os.environ["KAFKA_BROKER"], "api-measurement-consumer")

    api 는 lifespan 에서, streamlit 은 st.cache_resource 안에서 한 번만 부른다.
    """
    buffer = MeasurementBuffer()
    thread = ConsumerThread(broker, group_id, buffer,
                            from_beginning=from_beginning, on_row=on_row)
    thread.start()
    return buffer, thread


# --------------------------------------------------------------------------
# 판정 토픽 (battery.pack.verdict) 구독
#
# streamlit 이 쓴다. api 가 낸 판정을 받아 타일 색과 알림을 그린다.
# 측정과 달리 판정 메시지는 이미 평평해서 flatten 이 필요 없다.
# --------------------------------------------------------------------------

VERDICT_TOPIC = "battery.pack.verdict"

# 알림 목록에 들고 있을 이상/주의 판정 수
ALERT_LIMIT = 200


class VerdictBuffer:
    """받은 판정을 들고 있는 스레드 안전 버퍼.

    화면이 쓰는 것은 세 가지다:
      - latest_for()     구간별 마지막 판정 -> 타일 색, 판정 카드
      - recent_alerts()  최근 이상/주의     -> 알림 목록
      - stats()          수신 현황          -> 파이프라인 상태 표시
    """

    def __init__(self) -> None:
        self._latest: dict[tuple[int, str], dict] = {}
        self._alerts: deque[dict] = deque(maxlen=ALERT_LIMIT)
        self._seen: deque[str] = deque(maxlen=SEEN_LIMIT)
        self._seen_set: set[str] = set()
        self._counts: dict[str, int] = defaultdict(int)   # state 별 수신 수
        self._lock = threading.Lock()
        self.received = 0
        self.duplicates = 0
        self.last_at: datetime | None = None

    def add(self, verdict: dict) -> bool:
        """판정 한 건을 넣는다. verdict_id 가 겹치면 중복으로 버린다."""
        with self._lock:
            vid = verdict["verdict_id"]
            if vid in self._seen_set:
                self.duplicates += 1
                return False
            if len(self._seen) == self._seen.maxlen:
                self._seen_set.discard(self._seen[0])
            self._seen.append(vid)
            self._seen_set.add(vid)

            key = (verdict["serial_number"], verdict["mode"])
            self._latest[key] = verdict
            self._counts[verdict["state"]] += 1
            # 정상은 알림 목록에 쌓지 않는다 - 알림은 봐야 할 것만 남긴다
            if verdict["state"] != "normal":
                self._alerts.append(verdict)
            self.received += 1
            self.last_at = datetime.now(timezone.utc)
            return True

    def latest_for(self, serial_number: int, mode: str) -> dict | None:
        """한 구간의 가장 최근 판정. 없으면 None (api 가 아직 안 돌았다)."""
        with self._lock:
            return self._latest.get((serial_number, mode))

    def recent_alerts(self, limit: int = 20) -> list[dict]:
        """최근 이상/주의 판정. 최신이 앞이다."""
        with self._lock:
            return list(self._alerts)[-limit:][::-1]

    def stats(self) -> dict:
        with self._lock:
            return {"received": self.received, "duplicates": self.duplicates,
                    "by_state": dict(self._counts),
                    "last_at": self.last_at.isoformat() if self.last_at else None}


class VerdictThread(threading.Thread):
    """판정 토픽을 계속 읽어 VerdictBuffer 를 채우는 백그라운드 스레드."""

    def __init__(self, broker: str, group_id: str, buffer: VerdictBuffer):
        super().__init__(name=f"kafka-{group_id}", daemon=True)
        self._buffer = buffer
        self._stop = threading.Event()
        self._consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": group_id,
            # 켰을 때 이미 나온 판정도 보여야 하므로 earliest (측정과 같은 이유)
            "auto.offset.reset": "earliest",
            "topic.metadata.refresh.interval.ms": 5000,
            "enable.auto.commit": True,
            "client.id": group_id,
        })
        self.errors = 0

    def run(self) -> None:
        self._consumer.subscribe([VERDICT_TOPIC])
        while not self._stop.is_set():
            message = self._consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    self.errors += 1
                continue
            try:
                self._buffer.add(json.loads(message.value()))
            except Exception:
                # 깨진 판정 하나가 알림 전체를 세우면 안 된다
                self.errors += 1
        self._consumer.close()

    def stop(self) -> None:
        self._stop.set()


def start_verdicts(broker: str, group_id: str) -> tuple[VerdictBuffer, VerdictThread]:
    """판정 구독을 시작한다. streamlit 이 st.cache_resource 안에서 한 번 부른다."""
    buffer = VerdictBuffer()
    thread = VerdictThread(broker, group_id, buffer)
    thread.start()
    return buffer, thread
