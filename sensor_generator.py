"""sensor generator.
Postgres 에서 읽은 측정 이력을 Kafka 메시지로 바꿔 발행한다.

    Postgres  -->  database.py  -->  이 파일  -->  Kafka  -->  api

메시지 형식은 docs/kafka-message-spec.md 와 kafkadata.json 을 따른다.

원본은 전부 정상 팩이라 그대로 재생하면 이상 판정이 한 번도 안 나온다.
--anomaly-every 를 주면 구간마다 주기적으로 셀 하나를 골라 전압을 띄운
'이상치 버스트' 를 섞어 보낸다(3단계 참고). api 의 검출·화면의 알림까지
끝에서 끝으로 확인하려면 이 옵션으로 재생한다.

    python sensor_generator.py                       # 정상 데이터만
    python sensor_generator.py --anomaly-every 120   # 120행마다 이상치 버스트
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

import database

# 팩 하나의 구성. 원본 CSV 의 컬럼 이름(M01CV01 ~ M16CV11)에서 온 숫자다.
MODULE_COUNT = 16       # 모듈 16개
CELLS_PER_MODULE = 11   # 모듈당 셀 11개  -> 16 * 11 = 176
TEMPS_PER_MODULE = 2    # 모듈당 온도 센서 2개 -> 16 * 2 = 32


def to_modules(values: list[float], per_module: int) -> list[list[float]]:
    """일렬로 늘어선 값을 모듈 단위로 접는다.

    DB 의 cell_voltages 는 176개가 한 줄로 들어 있어서 어디까지가 몇 번
    모듈인지 알 수 없다. 명세가 요구하는 16 x 11 모양으로 바꿔 준다.

        cell_voltages  176개 -> to_modules(v, 11) -> 16줄 x 11칸
        module_temps    32개 -> to_modules(t,  2) -> 16줄 x  2칸

    접은 뒤에는 result[m][c] 가 M{m+1}CV{c+1} 이다. 인덱스는 0부터 세므로
    3번 모듈 7번 셀은 result[2][6] 이 된다.

    길이가 안 맞으면 바로 멈춘다. 175개나 177개가 들어오면 조용히 잘린
    데이터를 만드는 것보다 여기서 터지는 편이 낫다.
    """
    expected = MODULE_COUNT * per_module
    if len(values) != expected:
        raise ValueError(
            f"값이 {expected}개여야 하는데 {len(values)}개가 왔다 "
            f"(모듈 {MODULE_COUNT} x {per_module})"
        )

    return [
        values[m * per_module : (m + 1) * per_module]
        for m in range(MODULE_COUNT)
    ]


# ---------------------------------------------------------------------------
# 2단계: DB 행 -> Kafka 메시지
# ---------------------------------------------------------------------------

# 메시지 명세 버전. 필드 구성이 바뀌면 kafkadata.json / 명세서와 함께 올린다.
# (v1.2.0: 전 행이 0 인 soh 를 팀 결정으로 메시지에서 뺐다)
SCHEMA_VERSION = "1.2.0"

# 발행 주기(초). 팀 결정: 개발 중에는 3초에 1건. 데이터의 측정 간격(5초)과는
# 별개다 - 그건 '데이터가 몇 초짜리인가' 고 이건 '얼마나 빨리 재생하는가' 다.
# 6단계의 run() 이 쓴다.
SEND_INTERVAL_SECONDS = 3

# 원본 측정 시각은 KST 로 기록됐다(명세 6-4). DB 의 TIMESTAMPTZ 는 UTC 로
# 나오므로 표시만 KST 로 되돌린다. 같은 순간이고 표기만 다르다.
KST = timezone(timedelta(hours=9))


def build_message(row: dict, seq: int, label: str | None = None) -> dict:
    """DB 행 하나를 명세(kafkadata.json v1.2.0) 형태의 메시지로 바꾼다.

    row 는 database.iter_measurements() 가 주는 dict 그대로다.

    seq 를 밖에서 받는 이유: 순번은 '지금 몇 번째인가' 라는 발행 루프의
    상태다. 행 하나만 보는 이 함수는 알 수 없고, 함수 안에서 세기 시작하면
    상태가 생겨서 같은 입력에 다른 출력이 나온다(테스트 불가). 세는 일은
    루프(run)가 하고, 이 함수는 받은 값을 자리에 넣기만 한다.

    label 도 같은 이유로 밖에서 받는다. 행만 봐서는 이상치를 심었는지 알 수
    없다 - 심은 쪽(발행 루프 / inject_anomalies)만 안다. 원본 데이터에는
    정답 라벨이 없으므로 안 심은 행은 None 그대로 나간다(명세 5.6).
    """
    return {
        # --- DB 에 없어서 여기서 만드는 값 ---
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),   # 메시지마다 새 UUID. 컨슈머의 중복 제거용
        "produced_at": datetime.now(timezone.utc)
                               .isoformat(timespec="milliseconds")
                               .replace("+00:00", "Z"),
        "seq": seq,

        # --- DB 행에서 옮기는 값 ---
        "serial_number": row["serial_number"],
        "measured_at": row["measured_at"].astimezone(KST).isoformat(),
        "mode": row["mode"],

        "pack": {
            "voltage": row["voltage"],
            "current": row["current"],          # 음수 = 충전
            "power": row["power"],
            # soh 는 싣지 않는다. 원본 전 행이 0 인 죽은 컬럼 (v1.2.0)
        },
        "soc": {
            "rsoc_min": row["rsoc_min"],
            "rsoc_max": row["rsoc_max"],
            "rsoc_avg": row["rsoc_avg"],
            "usoc_min": row["usoc_min"],
            "usoc_max": row["usoc_max"],
            "usoc_avg": row["usoc_avg"],
        },
        "limits": {
            "chg_p_max": row["chg_p_max"],
            "dchg_p_max": row["dchg_p_max"],
            "chg_i_max": row["chg_i_max"],
            "dchg_i_max": row["dchg_i_max"],
        },
        "cell": {
            "v_min": row["v_min"],
            "v_max": row["v_max"],
            "dv": row["dv"],                    # 단위만 mV
            "voltages": to_modules(row["cell_voltages"], CELLS_PER_MODULE),
        },
        "temperature": {
            "t_min": row["t_min"],
            "t_max": row["t_max"],
            "t_avg": row["t_avg"],
            "values": to_modules(row["module_temps"], TEMPS_PER_MODULE),
        },

        # 원본에는 정답 라벨이 없어 보통 None 이다. 이상치를 심은 행만
        # 심은 쪽이 "defect" 를 넣어 준다 - 그 행의 정답은 우리가 아니까.
        "label": label,
    }


# ---------------------------------------------------------------------------
# 3단계: 이상치 주입
# ---------------------------------------------------------------------------
# 원본이 전부 정상 팩이라, 검출 파이프라인을 시험하려면 이상치를 만들어야 한다.
# 여기서 만드는 이상은 모델이 학습·검증에 쓴 시나리오와 같은 '셀 전압 저하'
# (src/fault_injection.py 의 capacity 유형)다.
#
# 설계가 모델의 판정 방식에 묶여 있다:
#   - **같은 셀을 연속 행으로 띄운다(버스트).** 모델은 임계 초과가 2 판정행
#     이어져야 알람을 낸다(persist). 행마다 다른 셀을 무작위로 건드리면 어느
#     셀도 두 번 연속 튀지 않아서, 아무리 크게 띄워도 알람이 영영 안 뜬다.
#   - **구간 시작 직후는 피한다.** 충전 세션 시작 후 60 판정행(300초)까지는
#     warmup 이라 온도 판정이 미뤄지고 상태도 덜 쌓였다. 버스트는 구간의
#     anomaly_every 번째 행부터 시작한다(0번째가 아니라).
#
# 기본값 -60mV / 6건은 inject_anomalies.py 와 같은 근거다: 모델팀 기준
# -20mV 는 검출률 0.61 이라 몇 건짜리 버스트에서는 낮고, -60mV 면 바로 뜬다.
# 지속 조건이 2 판정행이라 3건 미만 버스트는 알람을 못 넘긴다.

DEFAULT_ANOMALY_BURST = 6     # 버스트 하나의 행 수
DEFAULT_ANOMALY_MV = -60.0    # 이탈 폭(mV). 음수 = 셀 전압 저하(용량불량 방향)


def make_anomalous(row: dict, module: int, cell: int, offset_mv: float) -> dict:
    """행 하나의 셀 한 개를 띄운 사본을 돌려준다. 원본은 건드리지 않는다.

    module / cell 은 사람이 읽는 번호(M03 CV07)로 받아 0부터의 인덱스로 바꾼다.
    v_min / v_max / dv 도 같이 고친다 - dv = (v_max - v_min) * 1000 이라는
    파생 관계가 한 행 안에서 깨지면 메시지가 스스로 모순된다.

    inject_anomalies.py(일회성 주입 도구)도 이 함수를 쓴다. 두 곳이 각자
    만들면 파생 컬럼 갱신 규칙이 어긋났을 때 한쪽만 고치게 된다.
    """
    cells = list(row["cell_voltages"])
    cells[(module - 1) * CELLS_PER_MODULE + (cell - 1)] += offset_mv / 1000

    return {**row,
            "cell_voltages": cells,
            "v_min": min(cells),
            "v_max": max(cells),
            "dv": (max(cells) - min(cells)) * 1000}


class AnomalyPlan:
    """발행 루프에 끼어들어 '이 행을 띄울 것인가' 를 정하는 상태 기계.

    구간(serial, mode) 안에서 every 번째 행마다 셀 하나를 무작위로 골라
    burst 행 연속으로 띄운다. 셀을 매번 새로 고르는 이유: 화면에서 여러
    모듈 타일이 번갈아 켜지는 것을 봐야 지목(module/cell)이 실제로
    동작하는지 확인할 수 있다. seed 를 주면 그 무작위가 재현된다.

    루프가 아니라 별도 클래스인 이유: run() 은 이미 구간 전환·seq·flush 를
    챙기고 있어서, 주입의 상태(남은 버스트·표적 셀)까지 섞으면 어느 변수가
    누구 것인지 읽을 수 없게 된다. 상태를 여기 가두면 run() 쪽 변화는
    touch() 호출 한 줄이다.
    """

    def __init__(self, every: int, burst: int = DEFAULT_ANOMALY_BURST,
                 offset_mv: float = DEFAULT_ANOMALY_MV,
                 seed: int | None = None) -> None:
        if every < 1:
            raise ValueError(f"anomaly_every 는 1 이상이어야 한다: {every}")
        self.every = every
        self.burst = burst
        self.offset_mv = offset_mv
        self.rng = random.Random(seed)
        self.remaining = 0            # 지금 버스트에서 몇 행 더 띄울 것인가
        self.module = self.cell = 0   # 지금 버스트의 표적 (사람이 읽는 번호)

    def reset(self) -> None:
        """구간이 바뀔 때 부른다. 진행 중이던 버스트를 버린다.

        구간을 넘겨 이어 봐야 소용없다 - 모델도 팩(구간)마다 상태를 따로
        들고 있어서, 다음 구간의 첫 행은 지속 카운트 0 에서 다시 센다.
        """
        self.remaining = 0

    def touch(self, row: dict, seq: int) -> tuple[dict, bool]:
        """이 행을 띄울 차례면 띄운 사본을, 아니면 원본 그대로를 돌려준다.

        (행, 띄웠는가) 를 함께 돌려준다. 호출부가 label 을 채우고 로그를
        찍으려면 '바뀌었는가' 를 알아야 하는데, dict 비교로 알아내게 하는
        것보다 여기서 말해 주는 편이 낫다.
        """
        # seq > 0: 구간 첫 행(warmup 구간)에서 시작하지 않는다.
        # remaining == 0: 버스트가 겹치면 표적이 중간에 바뀌어 지속이 끊긴다.
        if self.remaining == 0 and seq > 0 and seq % self.every == 0:
            self.module = self.rng.randint(1, MODULE_COUNT)
            self.cell = self.rng.randint(1, CELLS_PER_MODULE)
            self.remaining = self.burst
            print(f"  ** 이상치 주입 seq {seq}~{seq + self.burst - 1}: "
                  f"M{self.module:02d} CV{self.cell:02d} {self.offset_mv:+.0f}mV")

        if self.remaining == 0:
            return row, False
        self.remaining -= 1
        return make_anomalous(row, self.module, self.cell, self.offset_mv), True


# ---------------------------------------------------------------------------
# 4단계: Kafka 연결
# ---------------------------------------------------------------------------

# 명세(kafkadata.json x-kafka.topic)에 박아 둔 토픽 이름
TOPIC = "battery.pack.measurement"


def on_delivery(err, msg) -> None:
    """메시지 하나의 발행 결과를 받는 콜백.

    produce() 는 메시지를 로컬 큐에 넣고 곧장 돌아온다. 브로커에 실제로
    닿았는지는 이 콜백으로만 알 수 있고, 콜백은 poll() 이나 flush() 를
    부르는 동안에 실행된다. poll 을 안 부르면 실패해도 아무 일도 없던
    것처럼 보인다 - 그게 이 콜백이 필수인 이유다.

    실패해도 루프를 세우지 않고 기록만 한다. 재생 도구라 한 건 유실이
    치명적이지 않아서다. 세우는 쪽이 맞다고 판단되면 여기서 raise 로 바꾼다.
    """
    if err is not None:
        key = msg.key().decode() if msg.key() else "?"
        print(f"발행 실패 key={key}: {err}", file=sys.stderr)


def make_producer(broker: str | None = None) -> Producer:
    """Kafka 프로듀서를 만든다.

    가장 중요한 사실: 여기서 연결이 일어나지 않는다. Producer() 는 설정만
    들고 즉시 돌아오고, 브로커 접속은 첫 발행 때 백그라운드 스레드가 한다.
    그래서 주소가 틀려도 이 함수는 '성공' 한다 - 에러는 한참 뒤 on_delivery
    로 온다. "예외가 안 났으니 연결됐다" 고 믿으면 안 되는 이유다.

    설정 세 개의 의미:
    - acks=all: 브로커가 기록을 마쳤다고 답할 때까지 성공으로 치지 않는다.
      지금은 브로커 1대지만, 복제본이 늘어도 이 설정은 그대로 안전하다.
    - enable.idempotence: 네트워크 오류로 재전송될 때 같은 메시지가 두 번
      기록되는 것을 브로커가 막는다(파티션별 순번 검사). 순서 보존도 함께
      보장된다. 메시지의 event_id 는 그 위 - 앱을 재실행한 경우 같은 - 의
      중복을 위한 것이라 역할이 다르다.
    - client.id: 브로커 로그와 kafka-ui 에 찍히는 이름. 문제가 생겼을 때
      "누가 보낸 것인가" 를 바로 찾게 해 준다.
    """
    return Producer({
        "bootstrap.servers": broker or os.environ.get("KAFKA_BROKER", "localhost:9092"),
        "acks": "all",
        "enable.idempotence": True,
        "client.id": "sensor-generator",
    })


# ---------------------------------------------------------------------------
# 5단계: 발행
# ---------------------------------------------------------------------------

def publish(producer: Producer, message: dict) -> None:
    '''메시지 하나를 토픽에 넣는다.

    Kafka 는 dict 도 JSON 도 모른다. 바이트만 나른다. 그래서 키와 값을
    우리가 직접 bytes 로 바꾸는 것이 이 함수의 일이다.

    키: serial_number 를 "1000" 같은 UTF-8 문자열로 보낸다. 정수를 그대로
    4바이트로 보낼 수도 있지만, 문자열이면 kafka-ui 에서 눈으로 읽히고
    다른 언어로 짠 컨슈머와도 해석이 어긋날 일이 없다. 명세 2절의 규약이다.

    값: dict -> JSON 문자열 -> UTF-8 bytes. ensure_ascii=False 는 비 ASCII
    문자가 유니코드 이스케이프 표기로 부풀어 나가지 않게 한다(지금 값엔
    없지만 습관이다).

    poll(0): produce 는 큐에 넣기만 하므로, 밀린 도착/실패 콜백을 실행할
    기회를 클라이언트에 줘야 한다. 0 은 '기다리지 말고 밀린 것만 처리하라'.
    이걸 안 부르면 on_delivery 가 영영 실행되지 않아서, 실패해도 아무도
    모른 채 큐만 쌓인다.

    헤더: mode 를 꼬리표로 붙인다. 값 안에도 있지만, 헤더에 두면 kafka-ui
    목록과 컨슈머가 JSON 을 파싱하지 않고도 충전/방전을 구분할 수 있다.
    키가 아니라 헤더인 이유: 파티션 배정에 관여하는 것은 키뿐이다. mode 를
    키에 넣으면 팩 구분이 사라지고, 키에 이어붙이면(1000_chg) 같은 팩의
    충전과 방전이 다른 파티션으로 갈라진다(실측 6팩 중 4팩). 헤더는
    라우팅을 건드리지 않으면서 같은 정보를 노출한다.

    producer 를 인자로 받는 것도 의도다. 전역으로 두면 진짜 브로커 없이는
    테스트할 수 없지만, 인자면 produce/poll 만 흉내 내는 가짜를 넣어 볼 수
    있다(tests/test_practice.py 의 _FakeProducer 참고).
    '''
    producer.produce(
        TOPIC,
        key=str(message["serial_number"]).encode(),
        value=json.dumps(message, ensure_ascii=False).encode(),
        headers=[("mode", message["mode"].encode())],
        on_delivery=on_delivery,
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# 6단계: 발행 루프
# ---------------------------------------------------------------------------

def run(serial_number: int | None = None,
        mode: str | None = None,
        interval: float = SEND_INTERVAL_SECONDS,
        limit: int | None = None,
        anomaly: AnomalyPlan | None = None) -> int:
    '''DB 를 훑어 Kafka 로 흘려보낸다. 발행한 건수를 돌려준다.

    anomaly 를 주면 그 계획대로 일부 행을 띄워 보낸다(3단계). 띄운 행은
    label 이 "defect" 로 나가므로, 컨슈머 쪽에서 판정과 정답을 맞춰 볼 수 있다.

    행 순서는 database 가 정한 그대로다 - 팩 오름차순, 팩마다 chg -> dchg.
    팀이 정한 재생 순서가 이미 SQL 의 ORDER BY 에서 나오므로 여기서 다시
    정렬하지 않는다.

    seq 를 구간마다 0 으로 되돌리는 이유: 명세는 seq 를 '(serial_number,
    mode) 안에서 0 부터' 라고 정의한다. 컨슈머가 유실을 감지하는 근거라
    구간이 바뀌면 다시 세야 한다. 2단계에서 build_message 가 seq 를 밖에서
    받게 만든 것이 여기서 값을 한다 - 세는 일은 루프의 몫이다.

    try/finally 가 이 함수의 핵심이다. produce 는 큐에 넣을 뿐이고 큐를
    비우는 것은 flush 뿐이므로, Ctrl+C 로 빠져나가도 flush 를 거치게 해야
    큐에 남은 메시지가 사라지지 않는다.
    '''
    producer = make_producer()
    sent = 0
    seq = 0
    section = None          # 지금 어느 (serial, mode) 구간인가

    try:
        for row in database.iter_measurements(serial_number, mode):
            here = (row["serial_number"], row["mode"])
            if here != section:
                if section is not None:
                    print(f"  {section[0]} {section[1]:5s} {seq}건 완료")
                section, seq = here, 0
                if anomaly is not None:
                    anomaly.reset()

            touched = False
            if anomaly is not None:
                row, touched = anomaly.touch(row, seq)

            publish(producer, build_message(row, seq, "defect" if touched else None))
            sent += 1
            seq += 1

            if limit is not None and sent >= limit:
                break

            # 마지막 한 건 뒤에는 자지 않는다. 자 봐야 끝나기만 늦어진다.
            time.sleep(interval)

    except KeyboardInterrupt:
        print()
        print("중단 요청 - 큐를 비우는 중")
    finally:
        # flush 는 큐가 빌 때까지 기다리며 그 동안 on_delivery 를 실행한다.
        # 여기서 비로소 발행 실패가 드러난다.
        remaining = producer.flush(30)
        if remaining:
            print(f"경고: {remaining}건이 전송되지 못했다", file=sys.stderr)

    if section is not None:
        print(f"  {section[0]} {section[1]:5s} {seq}건 완료")
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serial", type=int, help="팩 하나만 재생한다")
    parser.add_argument("--mode", choices=["chg", "dchg"], help="충전/방전 하나만")
    parser.add_argument("--interval", type=float, default=SEND_INTERVAL_SECONDS,
                        help=f"발행 간격(초). 기본 {SEND_INTERVAL_SECONDS}")
    parser.add_argument("--limit", type=int, help="이 건수만 보내고 멈춘다")

    group = parser.add_argument_group(
        "이상치 주입",
        "구간마다 every 번째 행부터 셀 하나를 골라 burst 행 연속으로 띄운다. "
        "안 주면 정상 데이터만 나간다.")
    group.add_argument("--anomaly-every", type=int, metavar="N",
                       help="구간 안에서 N행마다 이상치 버스트를 시작한다")
    group.add_argument("--anomaly-burst", type=int, default=DEFAULT_ANOMALY_BURST,
                       help=f"버스트 하나의 행 수. 기본 {DEFAULT_ANOMALY_BURST} "
                            "(모델의 지속 조건이 2행이라 3 이상이어야 알람이 뜬다)")
    group.add_argument("--anomaly-mv", type=float, default=DEFAULT_ANOMALY_MV,
                       help=f"이탈 폭(mV). 음수가 셀 전압이 낮아지는 쪽이다. "
                            f"기본 {DEFAULT_ANOMALY_MV:.0f}")
    group.add_argument("--seed", type=int,
                       help="표적 셀을 고르는 난수 시드. 주면 같은 자리에 다시 뜬다")
    args = parser.parse_args()

    plan = None
    if args.anomaly_every is not None:
        if args.anomaly_every < 1:
            parser.error("--anomaly-every 는 1 이상이어야 한다")
        if args.anomaly_burst < 3:
            print("  (anomaly-burst 가 3보다 작으면 지속 조건(2행)을 못 넘겨 "
                  "알람이 안 뜬다)")
        plan = AnomalyPlan(args.anomaly_every, args.anomaly_burst,
                           args.anomaly_mv, args.seed)
        print(f"이상치 주입: {args.anomaly_every}행마다 "
              f"{args.anomaly_burst}건 연속 {args.anomaly_mv:+.0f}mV")

    print(f"토픽 {TOPIC} 으로 {args.interval}초에 1건씩 발행합니다. (Ctrl+C 로 중단)")
    started = time.time()
    sent = run(args.serial, args.mode, args.interval, args.limit, plan)
    print(f"총 {sent:,}건 / {time.time() - started:.1f}초")


if __name__ == "__main__":
    main()
