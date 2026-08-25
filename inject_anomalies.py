"""이상 판정이 나오도록 손댄 측정을 Kafka 로 보내는 개발용 도구.

    inject_anomalies.py --> battery.pack.measurement --> api(판정) --> battery.pack.verdict --> streamlit

**판정을 직접 만들어 보내지 않는다.** 판정 토픽에 손으로 쓴 메시지를 밀어 넣으면
화면은 칠해지지만 정작 확인하려던 것 - api 가 이 측정을 이상으로 보는가 - 은
확인되지 않는다. 그래서 측정 토픽에만 넣고 판정은 api 에게 맡긴다.
판정 권한이 api 한 곳뿐이라는 규칙(detector.py docstring)을 도구도 따른다.

만드는 방법: DB 의 진짜 측정 행을 가져와 셀 하나만 팩 평균에서 크게 띄운다.
나머지 176개 값과 온도·충전량은 원본 그대로라 차트가 자연스럽게 이어진다.

    docker compose exec dev python inject_anomalies.py
    docker compose exec dev python inject_anomalies.py --serial 1000 --count 8
"""

import argparse
import time
import uuid

import database
import sensor_generator as gen
from battery_pack_defect_detection import detector

# 셀 하나를 팩 평균에서 이만큼(mV) 띄운다. 어느 모듈이 짚히는지 눈에 보이게
# 8건이 서로 다른 모듈을 가리키도록 짜 뒀다. (모듈 1~16, 셀 1~11 - 사람 번호)
#
# 이탈 폭에 주의: 한 셀만 올리면 팩 평균도 그 셀 쪽으로 1/176 만큼 끌려가므로
# 실제 이탈은 올린 값의 175/176 이다. detector 의 '이상' 경계가 16.8mV 라
# 25mV 부터 잡았고, 그 아래로는 '주의' 로 떨어질 수 있다.
TARGETS = [
    (3, 7, 45.0),
    (7, 2, 28.0),
    (12, 11, 60.0),
    (16, 5, 33.0),
    (1, 1, 52.0),
    (9, 8, 26.0),
    (5, 4, 38.0),
    (14, 10, 70.0),
]


def make_anomalous(row: dict, module: int, cell: int, offset_mv: float) -> dict:
    """행 하나의 셀 한 개를 띄운 사본을 돌려준다. 원본은 건드리지 않는다.

    module / cell 은 사람이 읽는 번호(M03 CV07)로 받아 0부터의 인덱스로 바꾼다.
    v_min / v_max / dv 도 같이 고친다 - dv = (v_max - v_min) * 1000 이라는
    파생 관계가 한 행 안에서 깨지면 메시지가 스스로 모순된다.
    """
    cells = list(row["cell_voltages"])
    cells[(module - 1) * gen.CELLS_PER_MODULE + (cell - 1)] += offset_mv / 1000

    return {**row,
            "cell_voltages": cells,
            "v_min": min(cells),
            "v_max": max(cells),
            "dv": (max(cells) - min(cells)) * 1000}


def next_seq(serial_number: int, mode: str) -> int:
    """api 가 이 구간에서 마지막으로 본 seq 의 다음 값.

    seq 를 아무렇게나 매기면 컨슈머가 유실로 센다(consumer.MeasurementBuffer
    가 seq 건너뜀을 gaps 로 집계한다). 이어 붙여야 화면의 '유실' 숫자가
    거짓으로 오르지 않는다. api 를 못 부르면 0 부터 시작한다.
    """
    import json
    import urllib.error
    import urllib.request

    for host in ("http://api:3000", "http://localhost:3000"):
        try:
            with urllib.request.urlopen(f"{host}/sections", timeout=2) as response:
                sections = json.load(response)
        except (urllib.error.URLError, OSError, TimeoutError):
            continue
        for section in sections:
            if (section["serial_number"], section["mode"]) == (serial_number, mode):
                last = section.get("last_seq")
                return 0 if last is None else last + 1
        return 0        # api 는 살아 있는데 이 구간을 아직 못 봤다
    print("  (api 에 못 물어봐서 seq 를 0 부터 시작한다 - 유실 표시가 오를 수 있다)")
    return 0


def run(serial_number: int, mode: str, count: int, interval: float) -> int:
    rows = []
    for row in database.iter_measurements(serial_number, mode):
        rows.append(row)
        if len(rows) >= count:
            break

    if not rows:
        print(f"{serial_number} {mode} 의 측정이 DB 에 없다")
        return 0
    if len(rows) < count:
        print(f"DB 에 {len(rows)}건뿐이라 그만큼만 보낸다")

    producer = gen.make_producer()
    seq = next_seq(serial_number, mode)
    sent = 0

    try:
        for row, (module, cell, offset_mv) in zip(rows, TARGETS):
            touched = make_anomalous(row, module, cell, offset_mv)

            # 보내기 전에 detector 로 미리 돌려 본다. 실제로 이상이 아니면
            # 화면에 아무 일도 안 일어나는데, 그 사실을 나중에 알면 늦다.
            preview = detector.judge({**touched, "event_id": str(uuid.uuid4()),
                                      "seq": seq})
            gen.publish(producer, gen.build_message(touched, seq))
            sent += 1

            mark = "OK " if preview["state"] == "anomaly" else "!! "
            print(f"  {mark}seq {seq:>5}  M{module:02d} CV{cell:02d} "
                  f"+{offset_mv:.0f}mV  ->  {preview['state']} "
                  f"({preview['detail']})")
            seq += 1

            if sent < len(rows):
                time.sleep(interval)
    finally:
        remaining = producer.flush(30)
        if remaining:
            print(f"경고: {remaining}건이 전송되지 못했다")

    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serial", type=int, default=1000, help="팩 번호. 기본 1000")
    parser.add_argument("--mode", choices=["chg", "dchg"], default="chg")
    parser.add_argument("--count", type=int, default=len(TARGETS),
                        help=f"보낼 건수. 기본 {len(TARGETS)} (TARGETS 개수가 상한)")
    # 화면이 3초마다 다시 그려지므로(app.REFRESH_EVERY) 기본을 3초로 뒀다.
    # 더 짧게 주면 앞 건이 화면에 뜨기 전에 다음 건에 덮인다 - 판정 카드와
    # 타일은 '구간별 마지막 판정' 하나만 보여주기 때문이다.
    parser.add_argument("--interval", type=float, default=3.0,
                        help="발행 간격(초). 기본 3 - 화면 갱신 주기와 같다")
    args = parser.parse_args()

    count = min(args.count, len(TARGETS))
    print(f"{gen.TOPIC} 으로 PACK {args.serial} {args.mode} 이상 측정 "
          f"{count}건을 {args.interval}초 간격으로 보냅니다.")
    sent = run(args.serial, args.mode, count, args.interval)
    print(f"총 {sent}건 발행. 화면의 '최근 알림' 과 모듈 타일을 확인하세요.")


if __name__ == "__main__":
    main()
