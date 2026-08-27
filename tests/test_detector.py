"""이상탐지 모델이 api 에 제대로 붙었는지 확인한다.

인프라(Kafka·DB)가 필요 없다. 학습된 모델(models/battery_anomaly.pkl)과
`db/data/*.csv` 만 있으면 돈다.

**왜 이 테스트가 있는가.** 이 모델은 잘못 붙여도 예외가 나지 않는다. 셀 배열
순서가 뒤집히거나, 비통전 행이 섞여 들어오거나, 세션 두 개가 이어 붙어도
점수는 그럴듯한 값이 계속 나온다. 감도와 오탐률만 조용히 달라진다. 그래서
'터지지 않는다' 로는 확인이 안 되고, **정답을 아는 데이터에서 그 정답을 내는지**
를 봐야 한다.

2026-08-27: 모델이 오토인코더(battery_anomaly.py)로 바뀌면서 기준점도 바뀌었다.
예전에는 정상 팩 1002 의 중앙값 점수 2.529 였는데, 새 모델은 행마다 점수를 내지
않아 그 수가 존재하지 않는다. 대신 **데모 팩 9개의 판정을 정답표와 대조**한다.
무엇을 심었는지 아는 데이터라(database.DEMO_PACKS) 훨씬 강한 확인이고, 전처리
· 곡선 · AE · 임계 · 지목 파싱까지 전 경로가 한 번에 걸린다.
"""

import itertools
import uuid

import pandas as pd
import pytest

from battery_pack_defect_detection import detector

# 셀 176개 / 온도 32개의 열 이름. consumer.flatten 이 만드는 평평한 배열과
# 순서가 같아야 한다 - voltages[m][c] = M{m+1:02d}CV{c+1:02d} (kafkadata.json).
CELL_COLS = [f"M{m:02d}CV{c:02d}" for m in range(1, 17) for c in range(1, 12)]
TEMP_COLS = [f"M{m:02d}T{s:02d}" for m in range(1, 17) for s in range(1, 3)]

# 정상 팩. 모델 학습에 쓰인 50팩 중 하나다.
NORMAL_PACK = 1002
# 결함을 주입할 팩. NORMAL_PACK 과 나눠 써야 누적 버퍼가 섞이지 않는다.
INJECT_PACK = 1003


@pytest.fixture(scope="module")
def model():
    """모델을 한 번만 읽는다."""
    detector.load()
    return detector


def measurements(pack: str | int, mode: str = "chg", inject: tuple | None = None,
                 energized_only: bool = True, shift_days: int = 0):
    """CSV 를 읽어 consumer.flatten 이 주는 모양의 측정 행으로 흘려보낸다.

    **database.py 와 같은 순서로 거른다.** 그 차이를 빼먹으면 이 테스트가 운영과
    다른 것을 재게 된다.

        1) 통전 구간만   |current| > CURRENT_ON_AMPS   (energized_only)
        2) 5초 구간마다 첫 행만                        (RESAMPLE_SECONDS)

    센티넬 배제는 여기서 하지 않는다. 실측으로 통전 행 중에 센티넬 행이 한 건도
    없어서(1002/1003 확인) 결과가 같고, 거르는 규칙을 테스트가 또 복제하면
    운영과 어긋났을 때 오히려 못 잡는다.

    `shift_days` 는 같은 구간을 며칠 뒤로 밀어 두 번째 충전 세션을 만든다.
    """
    import database

    df = pd.read_csv(f"db/data/{pack}_{mode}.csv").dropna(subset=["Date", "Time"])
    ts = pd.to_datetime(df["Date"] + " " + df["Time"]) + pd.Timedelta(days=shift_days)

    if energized_only:
        on = df["Current"].abs() > database.CURRENT_ON_AMPS
        df, ts = df[on], ts[on]

    # 5초 구간마다 첫 행만 남긴다 (평균이 아니다 - database.py 의 DISTINCT ON 과 같다)
    epoch = (ts - pd.Timestamp("1970-01-01")).dt.total_seconds()
    keep = ~(epoch.astype("int64") // int(database.RESAMPLE_SECONDS)).duplicated()
    df, ts = df[keep].reset_index(drop=True), ts[keep].reset_index(drop=True)

    cells = df[CELL_COLS].to_numpy(float)
    if inject is not None:
        col, millivolts, start, end = inject
        cells[start:end, CELL_COLS.index(col)] += millivolts / 1000.0
    temps = df[TEMP_COLS].to_numpy(float)

    serial = int(df["SerialNumber"].iloc[0])
    for i in range(len(df)):
        yield {
            "event_id": str(uuid.uuid4()),
            "serial_number": serial,
            "mode": mode,
            "measured_at": ts.iloc[i].to_pydatetime(),
            "seq": i,
            "cell_voltages": cells[i].tolist(),
            "module_temps": temps[i].tolist(),
            "rsoc_avg": float(df["RSOCavg"].iloc[i]),
            "current": float(df["Current"].iloc[i]),
        }


def last_verdict(model, pack, **kwargs):
    """한 팩을 끝까지 흘리고 마지막 판정을 돌려준다. 판정이 없으면 None.

    **먹이기 전에 그 팩의 누적 버퍼를 반드시 비운다.** 같은 팩을 두 번 흘리면
    두 번째는 전부 '역순 도착' 으로 버려져 판정이 0건이 된다(설계된 동작이다 -
    detector 의 '판정하지 않는 행' 참고). serial 은 CSV 에서 읽는다. 파일 이름이
    DEMO01 이어도 serial 은 9001 이라 이름으로는 알 수 없다.
    """
    rows = measurements(pack, **kwargs)
    first = next(rows)
    model.reset_pack(first["serial_number"])

    final = None
    for row in itertools.chain([first], rows):
        verdict = model.judge(row)
        if verdict is not None:
            final = verdict
    return final


def test_resample_period_matches_database():
    """모델에 알려 주는 주기와 database.py 가 실제로 보내는 주기가 같은가.

    이 둘이 어긋나는 것이 가장 조용한 사고다. 한쪽만 고치면 시간 창이 통째로
    배수만큼 틀어지는데 예외는 나지 않는다.
    """
    import database

    assert detector.MEASUREMENT_SECONDS == database.RESAMPLE_SECONDS, (
        f"detector 는 {detector.MEASUREMENT_SECONDS}초/행을 가정하는데 "
        f"database 는 {database.RESAMPLE_SECONDS}초마다 보낸다")


def test_energized_threshold_is_shared_with_database():
    """학습이 쓰는 통전 기준과 발행이 쓰는 통전 기준이 같은 값인가.

    **새 모델에는 자체 통전 게이트가 없다.** 예전 모델(StreamGate)은 비통전
    행이 섞여 들어와도 스스로 걸러냈지만, 지금은 database.py 가 거른 것을
    그대로 믿는다. 그래서 두 곳이 같은 상수를 봐야 한다 - pack_loader 가
    숫자를 복제하지 않고 database 에서 import 하는 이유가 이것이다.
    """
    import database
    import pack_loader

    source = pack_loader.from_csv.__doc__ or ""
    assert "database.py" in source, "from_csv 가 어느 규칙을 재현하는지 적혀 있어야 한다"
    # 상수를 복제했는지 확인한다. 복제했다면 한쪽만 바뀌어도 여기서 안 걸린다.
    assert pack_loader.database.CURRENT_ON_AMPS is database.CURRENT_ON_AMPS


def test_model_loads_with_three_stream_thresholds(model):
    """모델이 읽히고 세 스트림의 임계가 모두 잡혀 있는가.

    임계가 0 이거나 터무니없이 크면(예전에 셀 스트림이 3e9 로 잡혀 죽었다)
    그 스트림은 아무것도 검출하지 않는다. **예외는 나지 않는다.**
    """
    from battery_anomaly import STREAMS

    info = model.info()
    assert info["loaded"]
    assert set(info["threshold"]) == set(STREAMS)
    for name, value in info["threshold"].items():
        assert 0 < value < 1e3, (
            f"{name} 임계가 {value} 다. 0 이면 전부 걸리고, 지나치게 크면 "
            "그 스트림은 죽은 것이다 - battery_anomaly.MAD_FLOOR_MV 주석 참고")
    assert info["version"] != "0.0.0"


@pytest.mark.parametrize("pack_id", [f"DEMO{n:02d}" for n in range(1, 10)])
def test_demo_pack_final_verdict_matches_answer_key(model, pack_id):
    """데모 팩을 끝까지 흘렸을 때 정답표대로 판정하는가. **이 파일의 기준점이다.**

    데모 팩은 실제 팩의 편차 패턴 위에 고장을 심어 만든 것이라(make_demo.py)
    무엇이 정답인지 안다. 전처리·곡선·AE·임계·지목 파싱 어디가 틀어져도
    여기서 걸린다.

    DEMO09 만 고장을 심었는데 정답이 '정상' 이다. 2 mV 는 검출 한계 아래라
    안 걸리는 것이 맞다 - 임계가 헐거워지면 여기가 먼저 깨진다.
    """
    import database

    answer = next(m for m in database.DEMO_PACKS.values() if m["pack_id"] == pack_id)
    expected = answer["expect"].split(" (")[0]

    verdict = last_verdict(model, pack_id)
    assert verdict is not None, f"{pack_id} 에서 판정이 한 건도 안 나왔다"

    got = verdict["fault_type"] or "정상"
    assert got == expected, (
        f"{pack_id}: '{got}' 로 판정했는데 정답은 '{expected}' 다 "
        f"(주입: {answer['fault'] or '없음'} @ {answer['location'] or '-'})")

    # 세션을 끝까지 봤으므로 확정 판정이어야 한다.
    assert not verdict["warmup"], "끝까지 흘렸는데 아직 판정 확정 전이다"
    assert verdict["state"] == ("normal" if got == "정상" else "anomaly")


@pytest.mark.parametrize("pack_id, module, cell", [
    ("DEMO03", 7, None),     # 용접불량 - 모듈까지만 짚는다
    ("DEMO04", 12, None),
    ("DEMO05", 5, 6),        # 센싱와이어 - 인접 쌍의 앞쪽 셀
    ("DEMO06", 9, 3),        # 용량불량 - 셀까지
    ("DEMO07", 1, None),     # 온도 센서 - 셀이 없다
    ("DEMO08", 14, None),
])
def test_demo_pack_points_at_the_injected_part(model, pack_id, module, cell):
    """판정이 맞아도 지목이 틀리면 정비 대상이 엉뚱해진다.

    기대값은 make_demo.py 가 심은 자리(database.DEMO_PACKS 의 location)다.
    """
    verdict = last_verdict(model, pack_id)
    assert (verdict["module"], verdict["cell"]) == (module, cell), (
        f"{pack_id}: M{verdict['module']}CV{verdict['cell']} 를 짚었는데 "
        f"심은 곳은 M{module}CV{cell} 다")


def test_normal_pack_has_no_alarm(model):
    """학습에 쓴 정상 팩에서는 이상 판정이 나오지 않아야 한다."""
    model.reset_pack(NORMAL_PACK)
    states = {"normal": 0, "warning": 0, "anomaly": 0}
    for row in measurements(NORMAL_PACK):
        verdict = model.judge(row)
        if verdict is not None:
            states[verdict["state"]] += 1

    assert states["normal"] > 5, f"판정이 너무 적다: {states}"
    assert states["anomaly"] == 0, f"정상 팩에 이상 판정이 나왔다: {states}"


def test_discharge_is_not_judged(model):
    """방전 구간은 한 건도 판정하지 않아야 한다.

    모델은 충전 곡선으로만 학습됐다. 막지 않으면 방전에서도 조용히 판정이 나온다.
    """
    judged = [model.judge(row) for row in measurements(NORMAL_PACK, "dchg")]
    assert all(v is None for v in judged)


def test_verdicts_come_at_the_repredict_cadence(model):
    """판정이 REPREDICT_EVERY_ROWS 행마다 한 번씩 나오는가.

    새 모델은 행마다 판정하지 않는다(팩 단위 모델이라 그럴 수 없다). 판정
    간격이 이 주기와 어긋나면 누적/재판정 조건이 틀어진 것이다.
    """
    model.reset_pack(NORMAL_PACK)
    at = [i for i, row in enumerate(measurements(NORMAL_PACK))
          if model.judge(row) is not None]

    assert at, "판정이 한 건도 없다"
    assert at[0] + 1 >= detector.MIN_ROWS, (
        f"{at[0] + 1}행 만에 첫 판정이 나왔다. MIN_ROWS({detector.MIN_ROWS}) "
        "전에는 근거가 모자라 판정하면 안 된다")
    gaps = {b - a for a, b in zip(at, at[1:])}
    assert gaps == {detector.REPREDICT_EVERY_ROWS}, (
        f"판정 간격이 {sorted(gaps)} 다. {detector.REPREDICT_EVERY_ROWS}행마다 "
        "한 번이어야 한다")


def test_train_and_serve_build_the_same_pack(model):
    """학습 경로(CSV)와 추론 경로(측정 행)가 같은 PackData 를 만드는가.

    **이 프로젝트에서 가장 조용히 틀어질 수 있는 곳이다.** 학습은 db/data 의
    CSV 를 읽고 추론은 Kafka 행을 모으는데, 둘이 다른 전처리를 거치면 모델은
    학습 때 못 본 모양을 받게 된다. 예외는 나지 않고 점수만 달라진다.
    """
    import numpy as np

    import pack_loader

    rows = list(measurements(NORMAL_PACK))
    from_stream = pack_loader.from_rows(rows, pack_id=str(NORMAL_PACK))
    from_disk = pack_loader.from_csv(f"db/data/{NORMAL_PACK}_chg.csv")

    assert len(from_stream.soc) == len(from_disk.soc), (
        f"행 수가 다르다 - 스트림 {len(from_stream.soc)} / CSV {len(from_disk.soc)}")
    for name in ("soc", "v_pack", "mod_dev", "cell_res", "temp"):
        assert np.allclose(getattr(from_stream, name), getattr(from_disk, name)), (
            f"{name} 이 두 경로에서 다르다")


def test_session_gap_starts_a_new_pack(model):
    """충전 세션 사이의 공백을 보고 누적 버퍼를 비우는가.

    발행 쪽이 비통전 행을 빼면서 정지 구간이 아예 오지 않으므로, measured_at
    의 공백으로 세션 경계를 판단한다. 안 비우면 두 세션의 곡선이 이어 붙어
    SOC 축이 두 번 왕복하고, 판정이 조용히 틀어진다. **예외는 나지 않는다.**
    """
    model.reset_pack(NORMAL_PACK)

    def replay(shift_days: int) -> list:
        return [i for i, row in enumerate(measurements(NORMAL_PACK, shift_days=shift_days))
                if model.judge(row) is not None]

    first = replay(0)
    # 같은 팩이 사흘 뒤 다시 충전한다. 공백이 SESSION_GAP_SECONDS 를 넘으므로
    # 새 세션으로 보고 처음부터 다시 쌓아야 한다.
    second = replay(3)

    assert second == first, (
        f"두 번째 세션의 판정 시점이 {second[:3]}... 인데 첫 세션은 {first[:3]}... 다. "
        "세션 공백을 못 알아채고 앞 세션에 이어 붙였다")


def test_reset_lets_a_rewound_replay_through(model):
    """reset 뒤에 같은 구간을 다시 먹이면 처음처럼 판정되어야 한다.

    누적 버퍼는 마지막으로 본 측정 시각을 들고 역순 행을 걸러낸다. reset 이
    그것까지 비우지 않으면, 되감은 재생이 통째로 '역순' 으로 버려지고
    **예외는 나지 않는다** - 판정만 조용히 0건이 된다.
    """
    def judged_rows() -> int:
        model.reset_pack(NORMAL_PACK)
        return sum(model.judge(row) is not None for row in measurements(NORMAL_PACK))

    first = judged_rows()
    assert first > 5
    assert judged_rows() == first, "되감은 재생이 역순으로 버려졌다"


def test_injected_cell_fault_is_detected_and_located(model):
    """셀 하나에 -60 mV 를 주입하면 이상 판정과 함께 그 셀을 짚어야 한다.

    데모 팩과 별개로, 정상 팩에 그 자리에서 주입해도 잡히는지 본다.
    """
    model.reset_pack(INJECT_PACK)
    anomalies = []
    for row in measurements(INJECT_PACK, inject=("M08CV01", -60.0, 400, 800)):
        verdict = model.judge(row)
        if verdict is not None and verdict["state"] == "anomaly":
            anomalies.append(verdict)

    assert anomalies, "-60 mV 를 넣었는데 이상 판정이 없다"

    first = anomalies[0]
    assert first["module"] == 8, f"M08 을 짚어야 하는데 {first['module']}"
    assert first["cell"] == 1, f"CV01 을 짚어야 하는데 {first['cell']}"
    assert first["fault_type"], "이상인데 불량 유형이 비어 있다"
    assert not first["warmup"], "anomaly 는 판정이 확정된 뒤에만 나와야 한다"


def test_verdict_matches_published_schema(model):
    """판정 메시지가 verdictdata.json 을 만족하는가.

    api 가 붙이는 세 필드(schema_version / verdict_id / detected_at)까지 얹어
    실제로 발행되는 모양 그대로 검사한다.
    """
    jsonschema = pytest.importorskip("jsonschema")
    import json
    from datetime import datetime, timezone

    schema = json.load(open("verdictdata.json", encoding="utf-8"))

    model.reset_pack(NORMAL_PACK)
    checked = 0
    for row in measurements(NORMAL_PACK):
        verdict = model.judge(row)
        if verdict is None:
            continue
        message = {
            "schema_version": schema["properties"]["schema_version"]["const"],
            "verdict_id": str(uuid.uuid4()),
            "detected_at": datetime.now(timezone.utc)
                           .isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            **verdict,
        }
        jsonschema.validate(message, schema)
        checked += 1

    assert checked > 5, f"검사한 판정이 {checked}건뿐이다"


def test_state_labels_are_the_contract(model):
    """모델이 낼 수 있는 state 는 화면이 아는 세 값뿐이어야 한다."""
    assert detector.STATES == ("normal", "warning", "anomaly")


@pytest.mark.parametrize("label, expected", [
    ("M07", (7, None)),                  # 용접불량 - 모듈까지만
    ("M09CV03", (9, 3)),                 # 용량불량 - 셀까지
    ("M05CV06-CV07", (5, 6)),            # 센싱와이어 - 인접 쌍이면 앞쪽 셀
    ("M01T02", (1, None)),               # 온도 센서 - 셀이 없다
    ("M14 센서쌍", (14, None)),           # 모듈 안 두 센서의 차
    ("", (None, None)),
    ("M99CV01", (None, None)),           # 범위를 벗어나면 지목하지 않는다
    ("M08CV99", (8, None)),              # 모듈만 살린다
])
def test_location_parsing(label, expected):
    """모델의 지목 라벨을 화면이 쓰는 번호로 옮기는 부분.

    라벨 형식은 battery_anomaly.locate_cell / locate_temp 와 predict 의
    component 가 정한다. 모델을 새로 받을 때 여기부터 깨진다.
    """
    assert detector._parse_location(label) == expected


# --------------------------------------------------------------------------
# 화면이 판정을 읽는 부분
#
# 모델이 짚는 단위가 유형마다 다르다는 것을 화면이 모르면 조용히 죽는다.
# 실제로 도넛 캡션이 module 하나만 확인하고 두 값을 함께 찍다가, 셀 없는
# 알람이 처음 뜨는 순간 화면 전체가 TypeError 로 멈췄다.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("fault_type, expected", [
    ("셀 단위 이상", "셀이상"),
    ("용접불량", "용접"),
    ("센서불량", "센서"),
    ("셀 단위 이상, 용접불량", "셀이상+용접"),   # 스트림이 둘 걸리면 둘 다 적는다
    ("", ""),                                  # 정상이면 유형이 없다
    ("새 유형", "새 유형"),                     # 모르는 유형은 원문 그대로 (안 깨진다)
])
def test_fault_short(fault_type, expected):
    """불량 유형을 목록 배지용 짧은 이름으로 줄이는 부분.

    팩 단위 모델은 세 스트림을 따로 채점해 걸린 것을 전부 돌려주므로 유형이
    둘 이상 나올 수 있다. 첫 번째만 보여주면 나머지를 놓친다.
    """
    import app

    assert app.fault_short(fault_type) == expected


_seq = itertools.count()


def _verdict(serial, state, fault_type, module, cell, warmup):
    """판정 메시지 한 건. detector.judge 가 내는 모양 그대로.

    verdict_id 는 부를 때마다 달라야 한다. VerdictBuffer 가 그 값으로 중복을
    거르므로, 같은 id 로 두 번 넣으면 두 번째가 조용히 버려진다.
    """
    unique = next(_seq)
    return {"verdict_id": f"v{serial}-{unique}", "event_id": f"e{serial}-{unique}",
            "serial_number": serial, "mode": "chg", "seq": 90,
            "measured_at": "2026-08-24T08:00:00+09:00",
            "state": state, "module": module, "cell": cell,
            "fault_type": fault_type, "warmup": warmup,
            "detail": fault_type or "이상 없음",
            "model": {"name": "battery-anomaly-ae", "version": "7.505"}}


def test_provisional_verdicts_stay_out_of_the_history():
    """미확정 판정(warmup)은 알림·지목 이력에 남지 않아야 한다.

    팩 단위 모델의 판정은 SOC 칸이 덜 찬 동안 실제로 뒤집힌다 - DEMO08 은
    세션 초반에 '용접불량 M02' 였다가 확정 시점에 '센서불량 M14' 가 된다.
    그것을 이력에 남기면 최종 결과가 정상인 팩에도 '이상 1건' 이 영구히 붙어
    검사 결과판이 거짓말을 한다.
    """
    from battery_pack_defect_detection.consumer import VerdictBuffer

    buffer = VerdictBuffer()
    buffer.add(_verdict(9008, "warning", "용접불량", 2, None, warmup=True))
    buffer.add(_verdict(9008, "anomaly", "센서불량", 14, None, warmup=False))

    assert [a["state"] for a in buffer.recent_alerts()] == ["anomaly"]
    assert buffer.flagged_modules(9008, "chg") == {14: "anomaly"}
    # results 는 '지금 판정' 이라 미확정도 그대로 보여준다 - 이력과 쓰임이 다르다
    assert [v["serial_number"] for v in buffer.results()] == [9008]


def test_screen_survives_every_verdict_shape():
    """화면이 판정의 모든 모양을 받아도 죽지 않아야 한다.

    실제로 도넛 캡션이 module 하나만 확인하고 두 값을 함께 찍다가, 셀 없는
    알람이 처음 뜨는 순간 화면 전체가 TypeError 로 멈춘 적이 있다. 유형별로
    짚는 단위가 다른 것이 원인이라, 모양을 전부 한 번씩 태워 본다.
    """
    import app
    from battery_pack_defect_detection.consumer import VerdictBuffer

    shapes = [
        _verdict(9001, "normal", "", None, None, False),          # 정상
        _verdict(9003, "anomaly", "용접불량", 7, None, False),      # 모듈까지
        _verdict(9005, "anomaly", "셀 단위 이상", 5, 6, False),      # 셀까지
        _verdict(9007, "warning", "센서불량", 1, None, True),       # 미확정
    ]
    buffer = VerdictBuffer()
    for shape in shapes:
        buffer.add(shape)

    window = pd.DataFrame({"measured_at": pd.date_range("2026-08-24", periods=10, freq="5s")})
    for shape in shapes:
        app.render_verdict(shape, window)
    app.render_verdict(None, window)              # 판정 대기 중
    app.render_results(buffer, 9003)
    app.render_results(VerdictBuffer(), None)     # 아직 아무 팩도 판정 전


@pytest.mark.parametrize("module, cell, expected", [
    (8, 1, "M08 CV01"),      # 용량불량 / 센싱와이어불량 - 셀까지 짚는다
    (5, None, "M05"),        # 용접불량 - 모듈까지만
    (16, None, "M16"),       # 센서불량 - 온도 센서라 셀이 없다
    (None, None, "지목 없음"),  # 정상 / 주의 - 짚은 곳이 없다
])
def test_target_label(module, cell, expected):
    """판정의 지목을 화면 표기로 옮기는 부분. module 과 cell 은 따로 빈다."""
    import app

    assert app.target_label(module, cell) == expected
