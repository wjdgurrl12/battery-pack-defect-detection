"""이상탐지 모델이 api 에 제대로 붙었는지 확인한다.

인프라(Kafka·DB)가 필요 없다. 모델 번들과 `db/data/*.csv` 만 있으면 돈다.

**왜 이 테스트가 있는가.** 이 모델은 잘못 붙여도 예외가 나지 않는다. 입력 주기를
틀리게 넣거나, 방전 구간을 같이 먹이거나, 셀 배열 순서가 뒤집혀도 점수는 그럴듯한
값이 계속 나온다. 감도와 오탐률만 조용히 달라진다. 그래서 '터지지 않는다' 로는
확인이 안 되고, **학습 때와 같은 수를 내는지**를 봐야 한다.

기준값 2.529 는 모델팀이 `src/step9_realtime.replay` 로 낸 정상 팩 1002 의 중앙값
점수다(src/README.md 의 스모크 테스트 [3]). 전처리·피처·정규화 어디가 틀어져도
이 값이 흔들리므로, 한 줄로 전 경로를 확인하는 셈이다.
"""

import uuid

import pandas as pd
import pytest

from battery_pack_defect_detection import detector

# 셀 176개 / 온도 32개의 열 이름. consumer.flatten 이 만드는 평평한 배열과
# 순서가 같아야 한다 - voltages[m][c] = M{m+1:02d}CV{c+1:02d} (kafkadata.json).
CELL_COLS = [f"M{m:02d}CV{c:02d}" for m in range(1, 17) for c in range(1, 12)]
TEMP_COLS = [f"M{m:02d}T{s:02d}" for m in range(1, 17) for s in range(1, 3)]

# 정상 팩. 모델 학습에 쓰인 30팩 중 holdout 쪽이다.
NORMAL_PACK = 1002
# 결함을 주입할 팩. NORMAL_PACK 과 나눠 써야 모델 상태가 섞이지 않는다.
INJECT_PACK = 1003

# 모델팀 기준값(src/README.md). 전처리가 맞으면 여기서 다시 나온다.
REFERENCE_MEDIAN_SCORE = 2.529


@pytest.fixture(scope="module")
def model():
    """모델을 한 번만 읽는다. 번들 로드가 3초쯤 걸린다."""
    detector.load()
    return detector


def measurements(pack: int, mode: str, inject: tuple | None = None,
                 energized_only: bool = True, shift_days: int = 0):
    """CSV 를 읽어 consumer.flatten 이 주는 모양의 측정 행으로 흘려보낸다.

    **database.py 와 같은 순서로 거른다.** 그 차이를 빼먹으면 이 테스트가 운영과
    다른 것을 재게 된다.

        1) 통전 구간만   |current| > CURRENT_ON_AMPS   (energized_only)
        2) 5초 구간마다 첫 행만                        (RESAMPLE_SECONDS)

    `energized_only=False` 는 발행 쪽 필터가 없을 때를 재현한다. 모델의
    StreamGate 가 같은 판단을 하므로 결과가 같아야 한다.
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

    for i in range(len(df)):
        yield {
            "event_id": str(uuid.uuid4()),
            "serial_number": pack,
            "mode": mode,
            "measured_at": ts.iloc[i].to_pydatetime(),
            "seq": i,
            "cell_voltages": cells[i].tolist(),
            "module_temps": temps[i].tolist(),
            "rsoc_avg": float(df["RSOCavg"].iloc[i]),
            "current": float(df["Current"].iloc[i]),
        }


def test_resample_period_matches_database():
    """모델에 알려 주는 주기와 database.py 가 실제로 보내는 주기가 같은가.

    이 둘이 어긋나는 것이 가장 조용한 사고다. 한쪽만 고치면 시간 창이 통째로
    배수만큼 틀어지는데 예외는 나지 않는다.
    """
    import database

    assert detector.MEASUREMENT_SECONDS == database.RESAMPLE_SECONDS, (
        f"detector 는 {detector.MEASUREMENT_SECONDS}초/행을 가정하는데 "
        f"database 는 {database.RESAMPLE_SECONDS}초마다 보낸다")


def test_energized_threshold_matches_model(model):
    """발행 쪽 통전 기준과 모델의 통전 게이트가 같은 값인가.

    database.py 가 더 느슨하면 모델이 어차피 버리고(무해), 더 빡빡하면 모델이
    보고 싶어 하는 행이 발행되지 않는다. 어느 쪽이든 두 곳이 같은 값을 보는
    편이 낫다.
    """
    import database

    assert database.CURRENT_ON_AMPS == model._pool.st.current_on


def test_model_loads_and_stride_is_one(model):
    """번들이 읽히고, 토픽 주기와 학습 격자가 맞아떨어지는가.

    stride 가 1 이 아니면 모델이 측정을 한 번 더 솎고 있다는 뜻이다.
    BD_SOURCE_HZ 를 잘못 넣은 경우가 거의 전부다.
    """
    info = model.info()
    assert info["loaded"]
    assert info["stride"] == 1, (
        f"stride 가 {info['stride']} 다. 토픽은 5초/행인데 모델이 더 솎고 있다 - "
        "BD_SOURCE_HZ 를 확인할 것(0.2 여야 한다)")
    assert info["threshold"] > 0
    assert info["version"] != "0.0.0"


def test_normal_pack_reproduces_reference_score(model):
    """정상 팩의 중앙값 점수가 학습 때와 같은가. 전 경로를 한 줄로 확인한다.

    점수는 판정 메시지에 실리지 않으므로(2026-08-25 결정) 검출기에서 직접 꺼낸다.
    테스트라서 들여다보는 것이고, 바깥 계약은 그대로다.

    이 값이 어긋나면 전처리·피처·정규화·모델 중 어딘가가 학습 때와 달라진 것이다.
    가장 흔한 원인은 셀/온도 배열 순서와 입력 주기 두 가지다.
    """
    import numpy as np

    scores = []
    model.reset_pack(NORMAL_PACK)
    for row in measurements(NORMAL_PACK, "chg"):
        result = model._pool.feed(
            pack_id=row["serial_number"], ts=row["measured_at"].timestamp(),
            cells=row["cell_voltages"], temps=row["module_temps"],
            soc=row["rsoc_avg"], current=row["current"],
            key=row["measured_at"].timestamp())
        if result is not None:
            scores.append(result.score)

    assert scores, "정상 팩인데 판정된 행이 하나도 없다"
    assert np.median(scores) == pytest.approx(REFERENCE_MEDIAN_SCORE, abs=0.001), (
        f"중앙값 {np.median(scores):.3f} != 기준 {REFERENCE_MEDIAN_SCORE}. "
        "셀/온도 배열 순서나 입력 주기를 확인할 것")


def test_normal_pack_has_no_alarm(model):
    """정상 팩에서는 이상 판정이 나오지 않아야 한다.

    학습에 쓰인 팩이라 오탐이 나오면 붙이는 과정이 틀린 것이다
    (실측 오탐률은 0.21건/시간이라 한 세션에서 0건이 정상이다).
    """
    model.reset_pack(NORMAL_PACK)
    states = {"normal": 0, "warning": 0, "anomaly": 0}
    for row in measurements(NORMAL_PACK, "chg"):
        verdict = model.judge(row)
        if verdict is not None:
            states[verdict["state"]] += 1

    assert states["normal"] > 500, f"판정 행이 너무 적다: {states}"
    assert states["anomaly"] == 0, f"정상 팩에 이상 판정이 나왔다: {states}"


def test_discharge_is_not_judged(model):
    """방전 구간은 한 건도 판정하지 않아야 한다.

    모델은 충전 구간으로만 학습됐는데, 통전 게이트는 |I| 만 보고 부호는 보지
    않는다. 막지 않으면 방전에서도 조용히 판정이 나온다.
    """
    judged = [model.judge(row) for row in measurements(NORMAL_PACK, "dchg")]
    assert all(v is None for v in judged)


def test_decimation_is_not_applied_twice(model):
    """5초/행 입력이면 받은 행 대부분이 판정되어야 한다.

    판정 행이 1/5 로 떨어지면 모델이 한 번 더 솎고 있다는 뜻이다.
    (판정되지 않는 행은 충전 전후의 비통전·과도구간뿐이라야 한다)
    """
    model.reset_pack(NORMAL_PACK)
    received = judged = 0
    for row in measurements(NORMAL_PACK, "chg"):
        received += 1
        judged += model.judge(row) is not None

    assert judged / received > 0.6, (
        f"{received}행 중 {judged}행만 판정됐다. 두 번 솎고 있는지 stride 를 볼 것")


def test_energized_filter_does_not_change_judgments(model):
    """발행 쪽에서 비통전 행을 걸러도 판정 결과가 같아야 한다.

    걸러진 행은 어차피 모델이 판정하지 않던 것이다. 판정 수나 warmup 이
    달라지면 필터가 판정에 쓰이던 행까지 가져간 것이다.
    """
    def replay(energized_only: bool):
        model.reset_pack(NORMAL_PACK)
        model._pool.drop(NORMAL_PACK)
        states, warmups, received = {}, 0, 0
        for row in measurements(NORMAL_PACK, "chg", energized_only=energized_only):
            received += 1
            verdict = model.judge(row)
            if verdict is not None:
                states[verdict["state"]] = states.get(verdict["state"], 0) + 1
                warmups += verdict["warmup"]
        return received, states, warmups

    raw_received, raw_states, raw_warmups = replay(False)
    filtered_received, filtered_states, filtered_warmups = replay(True)

    assert filtered_states == raw_states, "필터가 판정 결과를 바꿨다"
    assert filtered_warmups == raw_warmups, "필터가 warmup 구간을 바꿨다"
    assert filtered_received < raw_received, "필터가 아무것도 걸러내지 않았다"


def test_session_gap_restarts_warmup(model):
    """충전 세션 사이의 공백을 보고 모델 상태를 다시 시작하는가.

    발행 쪽이 비통전 행을 빼면서, 모델이 정지 행을 세어 세션 종료를 알아채던
    길이 막혔다(StreamGate.idle_reset_rows). 대신 measured_at 의 공백으로 같은
    판단을 한다 - 안 하면 두 세션이 이어 붙어 V2 기울기가 공백을 가로질러
    계산되고 warmup 이 다시 돌지 않는다. **예외는 나지 않는다.**
    """
    model.reset_pack(NORMAL_PACK)
    model._pool.drop(NORMAL_PACK)

    def replay(shift_days: int) -> int:
        """한 세션을 흘리고 warmup 으로 표시된 판정 행 수를 돌려준다."""
        return sum(
            (verdict := model.judge(row)) is not None and verdict["warmup"]
            for row in measurements(NORMAL_PACK, "chg", shift_days=shift_days))

    first = replay(0)
    # 같은 팩이 사흘 뒤 다시 충전한다. 공백이 SESSION_GAP_SECONDS 를 넘으므로
    # 새 세션으로 보고 warmup 부터 다시 시작해야 한다.
    second = replay(3)

    assert first == model._pool.art.cfg.warmup_sec, (
        f"첫 세션의 warmup 행이 {first} 다. {model._pool.art.cfg.warmup_sec} 여야 한다")
    assert second == first, (
        f"두 번째 세션의 warmup 행이 {second} 다. 세션 공백을 못 알아채고 "
        "앞 세션에 이어 붙였다")


def test_reset_lets_a_rewound_replay_through(model):
    """reset 뒤에 같은 구간을 다시 먹이면 처음처럼 판정되어야 한다.

    전처리는 마지막으로 본 측정 시각을 들고 중복·역순을 걸러낸다. reset 이
    그것까지 비우지 않으면, 되감은 재생이 통째로 '중복' 으로 버려지고
    **예외는 나지 않는다** - 판정만 조용히 0건이 된다.
    """
    def judged_rows() -> int:
        model.reset_pack(NORMAL_PACK)
        return sum(model.judge(row) is not None
                   for row in measurements(NORMAL_PACK, "chg"))

    first = judged_rows()
    assert first > 500
    assert judged_rows() == first, "되감은 재생이 중복으로 버려졌다"


def test_injected_cell_fault_is_detected_and_located(model):
    """셀 하나에 -60 mV 를 주입하면 이상 판정과 함께 그 셀을 짚어야 한다.

    이것이 화면까지 이어지는 계약의 핵심이다 - state 만 맞고 지목이 틀리면
    정비 대상이 엉뚱해진다.
    """
    model.reset_pack(INJECT_PACK)
    anomalies = []
    for row in measurements(INJECT_PACK, "chg",
                            inject=("M08CV01", -60.0, 400, 800)):
        verdict = model.judge(row)
        if verdict is not None and verdict["state"] == "anomaly":
            anomalies.append(verdict)

    assert anomalies, "-60 mV 를 넣었는데 이상 판정이 없다"

    first = anomalies[0]
    assert first["module"] == 8, f"M08 을 짚어야 하는데 {first['module']}"
    assert first["cell"] == 1, f"CV01 을 짚어야 하는데 {first['cell']}"
    assert first["fault_type"], "이상인데 불량 유형이 비어 있다"
    assert not first["warmup"], "주입 구간은 warmup 이 끝난 뒤여야 한다"


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
    for row in measurements(NORMAL_PACK, "chg"):
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
        if checked >= 20:      # 앞쪽 20건이면 warmup 구간까지 포함된다
            break

    assert checked == 20


def test_state_labels_are_the_contract(model):
    """모델이 낼 수 있는 state 는 화면이 아는 세 값뿐이어야 한다."""
    assert detector.STATES == ("normal", "warning", "anomaly")


@pytest.mark.parametrize("label, expected", [
    ("용량불량(M08CV01, conf 0.82)", (8, 1)),
    ("용접불량(M05, conf 0.71)", (5, None)),
    ("센서불량(M16T02, conf 0.90)", (16, None)),
    ("V9:M08CV01", (8, 1)),
    ("V8:M08CV01-CV02", (8, 1)),         # 인접 쌍이면 앞쪽 셀을 짚는다
    ("V5:M16", (16, None)),
    ("정상(-, conf 0.00)", (None, None)),
    ("", (None, None)),
    ("M99CV01", (None, None)),           # 범위를 벗어나면 지목하지 않는다
    ("M08CV99", (8, None)),              # 모듈만 살린다
])
def test_location_parsing(label, expected):
    """모델의 라벨을 화면이 쓰는 번호로 옮기는 부분.

    라벨 형식은 step5_normalize.column_labels 와 step8_classify.Diagnosis 가
    정한다. 모델을 새로 받을 때 여기부터 깨진다.
    """
    assert detector._parse_location(label) == expected


# --------------------------------------------------------------------------
# 화면이 판정을 읽는 부분
#
# 모델이 짚는 단위가 유형마다 다르다는 것을 화면이 모르면 조용히 죽는다.
# 실제로 도넛 캡션이 module 하나만 확인하고 두 값을 함께 찍다가, 셀 없는
# 알람이 처음 뜨는 순간 화면 전체가 TypeError 로 멈췄다.
# --------------------------------------------------------------------------

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
