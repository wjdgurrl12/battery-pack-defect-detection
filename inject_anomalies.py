"""이상 판정이 나오도록 손댄 측정을 Kafka 로 보내는 개발용 도구.

    inject_anomalies.py --> battery.pack.measurement --> api(판정) --> battery.pack.verdict --> streamlit

**판정을 직접 만들어 보내지 않는다.** 판정 토픽에 손으로 쓴 메시지를 밀어 넣으면
화면은 칠해지지만 정작 확인하려던 것 - api 가 이 측정을 이상으로 보는가 - 은
확인되지 않는다. 그래서 측정 토픽에만 넣고 판정은 api 에게 맡긴다.
판정 권한이 api 한 곳뿐이라는 규칙(detector.py docstring)을 도구도 따른다.

만드는 방법: api 가 아직 안 본 다음 측정 행들을 DB 에서 가져와, **같은 셀 하나**를
여러 행에 걸쳐 계속 띄운다. 나머지 175개 값과 온도·충전량은 원본 그대로라
차트가 자연스럽게 이어지고, 모델이 보는 조건도 '셀 하나만 이상' 이 된다.

같은 셀을 이어서 띄우는 것이 중요하다. 모델은 임계 초과가 2 판정행 이어져야
알람을 내므로(persist), 행마다 다른 곳을 건드리면 아무리 크게 띄워도 안 뜬다.

    docker compose exec dev python inject_anomalies.py --serial 1000
    docker compose exec dev python inject_anomalies.py --serial 1000 --module 3 --cell 7 --mv -80
"""

import argparse
import itertools
import time

import database
import sensor_generator as gen

# 기본 주입 대상 (모듈, 셀, 이탈 mV). 사람이 읽는 번호다(M08 CV01).
#
# **한 셀을 여러 행에 걸쳐 계속 띄운다.** 예전에는 8건이 서로 다른 모듈을 하나씩
# 가리켰는데, 그건 한 행만 보고 판단하던 자리표 규칙에 맞춘 것이었다.
# 지금 모델은 **임계 초과가 2 판정행 이어져야** 알람을 낸다(persist). 매 행마다
# 다른 모듈을 건드리면 어느 곳도 두 번 연속 튀지 않아서, 아무리 크게 띄워도
# 영원히 알람이 안 뜬다 - 예외도 안 나고 그냥 조용히 정상으로 지나간다.
#
# 부호가 음수인 것도 이유가 있다. 모델이 학습·검증에 쓴 시나리오가 셀 전압이
# 낮아지는 쪽(용량불량)이다. 양수로도 뜨지만 검출률이 확인된 쪽을 기본값으로 둔다.
#
# 폭 60mV 는 실측으로 고른 값이다. 모델팀 기준 -20mV/25초의 검출률이 0.61 이라
# 몇 행만 넣는 이 도구에서는 낮고, -60mV 면 주입 직후 바로 뜬다.
DEFAULT_MODULE, DEFAULT_CELL, DEFAULT_MV = 8, 1, -60.0


def api_get(path: str):
    """api 에 물어본다. 못 부르거나 없으면 None.

    컨테이너 안에서는 api:3000, 호스트에서 직접 돌릴 때는 localhost:3000 이다.
    """
    import json
    import urllib.error
    import urllib.request

    for host in ("http://api:3000", "http://localhost:3000"):
        try:
            with urllib.request.urlopen(f"{host}{path}", timeout=2) as response:
                return json.load(response)
        except urllib.error.HTTPError:
            return None          # api 는 살아 있는데 그런 게 없다 (404)
        except (urllib.error.URLError, OSError, TimeoutError):
            continue             # 이 주소로는 안 된다. 다음 주소로
    return None


def _section_seq(serial_number: int, mode: str) -> int | None:
    """api 가 이 구간에서 마지막으로 본 seq. 아직 못 봤으면 None."""
    sections = api_get("/sections")
    if sections is None:
        return None
    for section in sections:
        if (section["serial_number"], section["mode"]) == (serial_number, mode):
            return section.get("last_seq")
    return None


def wait_for_api(serial_number: int, mode: str, quiet_checks: int = 3) -> None:
    """api 가 밀린 측정을 다 소화할 때까지 기다린다.

    **이걸 안 하면 조용히 어긋난다.** generator 가 500건을 10초에 쏟아 넣어도
    api 는 뒤에서 천천히 소화한다. 그 사이에 seq 와 측정 시각을 물어보면
    '아직 아무것도 못 봤다'는 답이 오고, 그 답으로 seq 0 부터 시작하는 행을
    만들어 보내게 된다 - 결과는 유실 카운트가 오르고, 이미 지난 시각이라
    모델이 전부 중복으로 버린다. 발행은 성공하고 화면은 그대로다.

    last_seq 가 연속으로 안 변하면 다 따라잡은 것으로 본다.
    """
    previous, still = None, 0
    for _ in range(240):                      # 최대 120초
        current = _section_seq(serial_number, mode)
        if current is not None and current == previous:
            still += 1
            if still >= quiet_checks:
                return
        else:
            still = 0
        previous = current
        time.sleep(0.5)
    print("  !! api 가 120초 안에 다 따라잡지 못했다. 그대로 진행하지만, "
          "주입한 행이 api 보다 뒤처지면 조용히 버려진다 - 아래 판정을 확인할 것")


def next_seq(serial_number: int, mode: str) -> int:
    """api 가 이 구간에서 마지막으로 본 seq 의 다음 값.

    seq 를 아무렇게나 매기면 컨슈머가 유실로 센다(consumer.MeasurementBuffer
    가 seq 건너뜀을 gaps 로 집계한다). 이어 붙여야 화면의 '유실' 숫자가
    거짓으로 오르지 않는다. api 를 못 부르면 0 부터 시작한다.
    """
    last = _section_seq(serial_number, mode)
    if last is None:
        print("  (api 가 이 구간을 아직 못 봐서 seq 를 0 부터 시작한다)")
        return 0
    return last + 1


def run(serial_number: int, mode: str, count: int, interval: float,
        module: int, cell: int, offset_mv: float) -> int:
    # 밀린 측정을 api 가 다 소화한 뒤에 물어봐야 seq 가 맞는다.
    wait_for_api(serial_number, mode)
    start = next_seq(serial_number, mode)

    # **구간의 앞쪽이 아니라, api 가 멈춘 자리의 바로 다음 행을 쓴다.**
    #
    # 앞쪽 행을 쓰면 두 가지가 한꺼번에 어긋난다.
    #   1) 측정 시각이 이미 지난 값이라 모델이 중복으로 보고 버린다
    #   2) 전류가 충전 시작 구간의 값이라 지금 흐르는 전류와 수십 A 차이가 난다.
    #      모델은 전류가 5 A 넘게 튀면 과도구간으로 보고 뒤 5행을 버리는데,
    #      매 행이 튀므로 보낸 것이 전부 버려진다.
    # 둘 다 예외가 나지 않는다 - 발행은 성공하고 화면만 그대로다.
    #
    # 다음 행을 그대로 쓰면 시각·전류·SOC·온도가 전부 자연스럽게 이어지고,
    # 우리가 바꾼 것은 셀 하나뿐이라는 조건이 실제로 지켜진다.
    rows = list(itertools.islice(
        database.iter_measurements(serial_number, mode), start, start + count))

    if not rows:
        print(f"{serial_number} {mode} 의 seq {start} 이후 측정이 DB 에 없다.\n"
              "  구간을 다 재생한 것이다 - 다른 팩을 쓰거나 "
              f"POST /packs/{serial_number}/reset 뒤 처음부터 다시 흘린다")
        return 0
    if len(rows) < count:
        print(f"구간에 {len(rows)}건만 남아 그만큼만 보낸다")

    producer = gen.make_producer()
    seq = start
    sent = 0

    last_seq = None
    try:
        for row in rows:
            # 셀 하나 띄우기는 sensor_generator.make_anomalous 와 공유한다.
            # 심은 행이므로 정답 라벨 "defect" 를 함께 싣는다(명세 5.6).
            touched = gen.make_anomalous(row, module, cell, offset_mv)

            gen.publish(producer, gen.build_message(touched, seq, "defect"))
            sent += 1
            last_seq = seq
            print(f"  보냄 seq {seq:>5}  M{module:02d} CV{cell:02d} {offset_mv:+.0f}mV"
                  f"  {touched['measured_at']:%H:%M:%S}")
            seq += 1

            if sent < len(rows):
                time.sleep(interval)
    finally:
        remaining = producer.flush(30)
        if remaining:
            print(f"경고: {remaining}건이 전송되지 못했다")

    # 판정은 api 가 한다. 여기서 미리 돌려 보지 않는 이유:
    # 모델은 팩마다 상태를 들고 있고(링버퍼 61행·온도 오프셋·지속 카운터),
    # 그 상태는 api 프로세스 안에만 있다. 이 도구가 자기 검출기를 새로 만들어
    # 몇 행만 넣으면 warmup 도 안 끝난 빈 상태라, api 와 다른 답이 나온다.
    # 틀린 미리보기는 없느니만 못하므로 **api 에게 물어본다.**
    if last_seq is not None:
        report(serial_number, mode, start, last_seq)

    return sent


def report(serial_number: int, mode: str, first_seq: int, last_seq: int) -> None:
    """방금 보낸 행을 api 가 어떻게 봤는지 되읽어 알린다.

    **판정의 seq 가 우리가 보낸 범위 안인지 반드시 확인한다.** 그냥 '가장 최근
    판정' 을 읽으면, 우리 행이 통째로 버려졌을 때 api 가 앞서 판정해 둔 남의
    행을 우리 결과로 착각해 보고한다 - 실제로 그렇게 '정상' 이라고 잘못 알렸다.
    """
    for _ in range(20):                       # 최대 10초
        verdict = api_get(f"/verdicts/latest/{serial_number}/{mode}")
        if verdict is None or verdict["seq"] < first_seq:
            time.sleep(0.5)
            continue

        if verdict["seq"] > last_seq:
            # 우리가 보낸 것보다 api 가 앞서 있다. seq 를 물어본 뒤 api 가 더
            # 나아간 것이라, 우리 행은 '이미 지난 시각' 이 되어 버려졌다.
            print(f"\n  !! 주입한 행(seq {first_seq}~{last_seq})이 판정되지 않았다. "
                  f"api 는 이미 seq {verdict['seq']} 까지 가 있다.\n"
                  "      측정 발행이 끝난 뒤 api 가 다 따라잡고 나서 다시 실행하면 된다")
            return

        mark = "OK " if verdict["state"] == "anomaly" else "!! "
        print(f"\n  {mark}api 판정: {verdict['state']} · {verdict['detail']}"
              f"  (seq {verdict['seq']})")
        if verdict["state"] == "anomaly":
            print(f"      지목 {app_target(verdict)} · 유형 {verdict['fault_type']}")
        elif verdict["warmup"]:
            # 이 경우가 가장 헷갈린다. 주입은 제대로 됐는데 모델이 아직
            # 온도 판정을 미루는 중이라 전압만으로 본 결과다.
            stats = api_get("/stats") or {}
            packs = {p["pack_id"]: p for p in stats.get("model", {}).get("packs", [])}
            left = packs.get(serial_number, {}).get("warmup_left", "?")
            print(f"      아직 warmup 이다(남은 판정행 {left}). 충전 세션 시작 후 "
                  "60행(300초)이 지나야 온도까지 본다 - 측정을 더 흘린 뒤 다시 주입한다")
        else:
            print("      이상으로 안 보였다. --mv 로 이탈 폭을 키우거나 "
                  "--count 를 늘린다(지속 조건 2행)")
        return
    print("\n  !! api 판정을 10초 안에 못 받았다. /health 로 컨슈머가 살아 있는지 본다")


def app_target(verdict: dict) -> str:
    """판정이 짚은 곳. module 과 cell 은 따로 비어 있을 수 있다."""
    module, cell = verdict["module"], verdict["cell"]
    if module is None:
        return "없음"
    return f"M{module:02d}" if cell is None else f"M{module:02d} CV{cell:02d}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serial", type=int, default=1000, help="팩 번호. 기본 1000")
    parser.add_argument("--mode", choices=["chg", "dchg"], default="chg")
    # 지속 조건이 2 판정행이라 최소 3건은 보내야 알람이 뜬다. 여유를 둬 6 이다.
    parser.add_argument("--count", type=int, default=6,
                        help="보낼 건수. 기본 6 (모델의 지속 조건이 2행이라 3건 이상)")
    parser.add_argument("--module", type=int, default=DEFAULT_MODULE,
                        help=f"띄울 모듈 1~16. 기본 {DEFAULT_MODULE}")
    parser.add_argument("--cell", type=int, default=DEFAULT_CELL,
                        help=f"띄울 셀 1~11. 기본 {DEFAULT_CELL}")
    parser.add_argument("--mv", type=float, default=DEFAULT_MV,
                        help=f"이탈 폭(mV). 음수가 셀 전압이 낮아지는 쪽이다. "
                             f"기본 {DEFAULT_MV:.0f}")
    # 화면이 3초마다 다시 그려지므로(app.REFRESH_EVERY) 기본을 3초로 뒀다.
    # 더 짧게 주면 앞 건이 화면에 뜨기 전에 다음 건에 덮인다 - 판정 카드와
    # 타일은 '구간별 마지막 판정' 하나만 보여주기 때문이다.
    parser.add_argument("--interval", type=float, default=3.0,
                        help="발행 간격(초). 기본 3 - 화면 갱신 주기와 같다")
    args = parser.parse_args()

    if not 1 <= args.module <= 16 or not 1 <= args.cell <= 11:
        parser.error("module 은 1~16, cell 은 1~11 이다")
    if args.count < 3:
        print("  (count 가 3보다 작으면 지속 조건(2행)을 못 넘겨 알람이 안 뜬다)")

    print(f"{gen.TOPIC} 으로 PACK {args.serial} {args.mode} "
          f"M{args.module:02d} CV{args.cell:02d} {args.mv:+.0f}mV 를 "
          f"{args.count}건 연속으로, {args.interval}초 간격으로 보냅니다.")
    sent = run(args.serial, args.mode, args.count, args.interval,
               args.module, args.cell, args.mv)
    print(f"총 {sent}건 발행. 화면의 '최근 알림' 과 모듈 타일을 확인하세요.")


if __name__ == "__main__":
    main()
