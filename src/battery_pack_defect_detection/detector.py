"""이상치 판정. api 가 Kafka 로 받은 측정을 이 모듈에 넘겨 판단한다.

    Kafka(battery.pack.measurement) --> api --> judge() --> Kafka(battery.pack.verdict)

2026-08-24 결정: **모든 측정을 판정해 결과를 전부 발행한다.** 정상 판정은 알림을
띄우지 않을 뿐, 화면이 판정 카드와 모듈 타일을 칠하는 데 쓴다. 판정 권한을 api
한 곳에 두고, 화면은 받은 것만 표시한다.

2026-08-25 결정: **모델은 이상 점수를 내지 않는다.** 판정 결과와 지목뿐이다.

    나가는 것:  state(정상/주의/이상) · module(문제 모듈) · cell(문제 셀)
    나가지 않는 것:  score · threshold · module_scores

    이전에는 모듈 16개의 이상 점수를 실어 보내고 화면이 그 숫자로 타일을
    칠했다. 실제 모델이 점수를 돌려주지 않으므로 점수 관련 필드를 전부
    걷어냈다. 화면은 state 와 module 만 보고 칠한다 - 지목된 모듈 하나에
    상태 색이 들어가고 나머지는 중립색이다.

**여기가 학습 중인 모델이 들어올 자리다.** predict() 하나만 갈아 끼우면 되고,
나머지(판정 메시지 구성, 발행, 화면)는 그대로 돌아간다.
"""

MODULE_COUNT = 16
CELLS_PER_MODULE = 11

# 판정에 쓰는 모델의 이름과 버전. 판정 메시지에 그대로 실어 보내므로,
# 나중에 "이 알림은 어느 모델이 낸 것인가" 를 되짚을 수 있다.
MODEL_NAME = "placeholder-deviation"
MODEL_VERSION = "0.0.0"

_MODEL = {"name": MODEL_NAME, "version": MODEL_VERSION}

# 모델이 낼 수 있는 판정 값. 화면이 이 값으로 색을 고른다.
STATES = ("normal", "warning", "anomaly")


# --------------------------------------------------------------------------
# 자리표 규칙 - 모델이 오면 이 구간이 통째로 사라진다
#
# _ 로 시작하는 것들은 predict() 안에서만 쓰는 임시 판단 재료다. **판정
# 메시지로는 절대 나가지 않는다** - 나가는 것은 state / module / cell 뿐이다.
# 모델이 자기 안에서 무엇을 보고 판단하든 바깥 계약은 바뀌지 않는다.
# --------------------------------------------------------------------------

# 셀 전압이 팩 평균에서 몇 mV 벗어나면 어느 판정인지.
# (관측된 이탈 범위가 2~37mV 라 이 언저리를 경계로 잡았다)
_DEVIATION_ANOMALY_MV = 16.8   # 이 위면 '이상'
_DEVIATION_WARNING_MV = 12.0   # 이 위면 '주의'


def _worst_cell(cell_voltages: list[float]) -> tuple[int, int, float]:
    """팩 평균에서 가장 많이 벗어난 셀. (모듈 인덱스, 셀 인덱스, 이탈 mV).

    cell_voltages 는 평평한 176개 배열이다(consumer.flatten 이 그렇게 준다).
    돌려주는 인덱스는 0부터다 - 사람이 읽는 M01/CV01 번호로 바꾸는 것은
    부르는 쪽의 몫이다.
    """
    if len(cell_voltages) != MODULE_COUNT * CELLS_PER_MODULE:
        raise ValueError(
            f"셀 전압이 {MODULE_COUNT * CELLS_PER_MODULE}개여야 하는데 "
            f"{len(cell_voltages)}개가 왔다")

    pack_mean = sum(cell_voltages) / len(cell_voltages)
    worst = max(range(len(cell_voltages)),
                key=lambda i: abs(cell_voltages[i] - pack_mean))
    deviation_mv = abs(cell_voltages[worst] - pack_mean) * 1000
    return worst // CELLS_PER_MODULE, worst % CELLS_PER_MODULE, deviation_mv


def _placeholder_predict(row: dict) -> tuple[dict, dict]:
    """자리표 판정. 가장 많이 이탈한 셀 하나로 팩 전체를 판정한다."""
    module, cell, deviation_mv = _worst_cell(row["cell_voltages"])

    if deviation_mv >= _DEVIATION_ANOMALY_MV:
        state = "anomaly"
    elif deviation_mv >= _DEVIATION_WARNING_MV:
        state = "warning"
    else:
        state = "normal"

    # 정상이면 지목하지 않는다 - 짚을 문제가 없다.
    if state == "normal":
        return {"state": state, "module": None, "cell": None}, _MODEL
    return {"state": state, "module": module + 1, "cell": cell + 1}, _MODEL


# --------------------------------------------------------------------------
# 모델 자리 - 여기부터가 바깥과의 계약이다
# --------------------------------------------------------------------------

def predict(row: dict, history: list[dict] | None = None) -> tuple[dict, dict]:
    """측정 한 건을 판정한다. (판정, 모델 정보) 를 돌려준다.

    **모델을 붙일 때 바꾸는 함수가 이것이다.**

    판정은 세 키를 갖는다:
        state   'normal' / 'warning' / 'anomaly'
        module  문제 모듈 번호(1~16). 정상이면 None
        cell    문제 셀 번호(1~11).   정상이면 None

    점수는 돌려주지 않는다. 모델이 안에서 확률이나 재구성 오차를 쓰더라도
    그것은 모델의 사정이고, 밖으로 나가는 것은 판정과 지목뿐이다.

    history 는 같은 (팩, 구간)의 앞선 측정들이다. 지금 규칙은 한 행만 보므로
    쓰지 않지만, 시계열 모델은 이것이 필요하다 - "어느 시점부터 전압이 멈췄다"
    같은 판단은 한 행만 보고는 불가능하기 때문이다. 인터페이스를 미리 열어 둔다.

    돌려주는 모델 정보는 판정 메시지에 그대로 실린다.
    """
    return _placeholder_predict(row)


def judge(row: dict, history: list[dict] | None = None) -> dict:
    """측정 한 건을 판정 메시지로 만든다. **항상 결과를 돌려준다.**

    정상도 돌려주는 이유는 모듈 파일 위 docstring 에 적었다 - 화면이 판정
    카드와 타일을 칠하려면 정상 판정도 필요하다.

    module / cell 은 정상일 때 None 이다. 화면은 지목이 없으면 타일 색을
    칠하지 않고 중립으로 둔다.

    verdict_id 와 detected_at 은 여기서 만들지 않는다. 그 둘은 '발행하는
    쪽의 상태' 라서, 넣으면 같은 입력에 다른 출력이 나오는 함수가 되어
    테스트가 불가능해진다. 발행 시점에 api 가 붙인다.
    """
    verdict, model = predict(row, history)
    state = verdict["state"]
    module, cell = verdict["module"], verdict["cell"]

    # 모델을 갈아 끼울 때 가장 흔한 사고가 라벨 불일치다('OK', 1, '이상' 등).
    # 여기서 막지 않으면 Kafka 헤더 인코딩과 화면의 STATE_KO 조회가
    # 엉뚱한 곳에서 터진다. 판정 권한이 한 곳이니 검사도 한 곳에서 한다.
    if state not in STATES:
        raise ValueError(f"모델이 낸 state 가 {STATES} 중 하나여야 하는데 {state!r} 이다")

    detail = (f"M{module:02d} CV{cell:02d} 이탈" if module is not None
              else "이상 없음")

    return {
        "event_id": row["event_id"],          # 판정 대상 측정. 추적용
        "serial_number": row["serial_number"],
        "mode": row["mode"],
        "measured_at": row["measured_at"].isoformat(),
        "seq": row["seq"],
        "state": state,
        "module": module,                     # 사람이 읽는 번호(M01~M16) 또는 None
        "cell": cell,                         # CV01~CV11 또는 None
        "detail": detail,
        "model": model,
    }
