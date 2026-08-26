"""이상치 판정. api 가 Kafka 로 받은 측정을 이 모듈에 넘겨 판단한다.

    Kafka(battery.pack.measurement) --> api --> judge() --> Kafka(battery.pack.verdict)

2026-08-24 결정: 모든 측정을 판정해 결과를 전부 발행한다.
2026-08-25 결정: 모델은 이상 점수를 내지 않는다. 판정 결과와 지목뿐이다.
2026-08-26 결정: **판정하지 않는 행이 생긴다.** 자리표를 실제 모델로 갈아 끼우면서
    위 첫 결정이 지킬 수 없게 됐다 - 아래 '판정하지 않는 행' 참고.

    나가는 것:  state(정상/주의/이상) · module(문제 모듈) · cell(문제 셀)
                fault_type(불량 유형) · warmup(온도 판정 보류 여부)
    나가지 않는 것:  score · threshold · module_scores

    점수를 계속 빼두는 이유는 2026-08-25 과 같다. 모델이 안에서 재구성 오차를
    쓰더라도 그것은 모델의 사정이고, 토픽으로 나가는 것은 판정과 지목뿐이다.

════════════════════════════════════════════════════════════════════════════
모델
════════════════════════════════════════════════════════════════════════════
`battery_detector.DetectorPool` 을 그대로 쓴다(모델팀 인수인계본, src/README.md).
이 모듈은 그 위에 두 가지 번역만 얹는다.

    1) 측정 메시지(flatten 된 dict) -> DetectorPool.feed 인자
    2) DetectorPool 의 Result       -> 판정 메시지 (state / module / cell)

모델 자체는 건드리지 않는다. battery_detector.py 와 src/step*.py 는 학습 시점
해시(manifest 의 source_sha8)로 검증되므로, 고치면 기동이 거부된다.

────────────────────────────────────────────────────────────────────────────
판정하지 않는 행 - judge() 가 None 을 돌려주는 경우
────────────────────────────────────────────────────────────────────────────
자리표는 한 행만 보고 판단해 늘 결과가 나왔지만, 실제 모델은 상태를 들고 있고
학습 때와 같은 전처리를 거친 행만 받는다. 아래 넷은 **판정 자체를 하지 않는다.**

    과도구간    전류 급변 직후 5행. 셀 전압이 내부저항 때문에 계단처럼 튄다
    중복        Kafka 는 at-least-once 다. 같은 행이 두 번 들어가면 링버퍼가 왜곡된다
    솎임        학습 격자가 5초/행보다 촘촘하게 들어올 때. 지금 파이프라인은
                이미 5초/행이라 여기서 빠지는 행은 없다(stride 1)
    비통전      |I| <= 1.0 A. 지금은 database.py 가 아예 보내지 않아 여기까지
                오지 않는다. 게이트는 그대로 두는 편이 낫다 - 발행 쪽 필터가
                빠져도 모델은 같은 판단을 한다

여기에 방전 구간(mode='dchg')이 하나 더 붙는다. 모델은 충전 구간으로만 학습됐다.
(EXCLUDE_DCHG 로 발행 자체를 막고 있지만, 같은 이유로 이 검사도 남겨 둔다)

이런 행에 '정상' 을 발행하면 안 된다. 판정을 안 한 것과 정상인 것은 다르고,
화면이 그 둘을 구분하지 못하면 오해를 부른다. **부르는 쪽은 None 을 받으면
아무것도 발행하지 않는다.** 화면은 마지막 판정을 계속 보여주면 된다 - 판정이
5초에 1건씩 갱신되므로 화면이 멈춘 것처럼 보이지는 않는다.

────────────────────────────────────────────────────────────────────────────
state 세 값이 모델의 무엇에 대응하는가
────────────────────────────────────────────────────────────────────────────
모델은 점수와 임계값, 그리고 지속 조건(임계 초과가 2 판정행 = 10초 이어져야
알람)을 갖는다. 화면의 3단계는 그 지속 조건의 중간 상태를 그대로 쓴다.

    normal    점수 <= 임계
    warning   점수 > 임계, 그러나 아직 지속 조건 미달 (초과 1행째)
    anomaly   지속 조건 충족 -> 알람

warning 에는 지목이 없다. 원인 분석(SPE 기여도 분해 + 유형 분류)은 알람이 뜬
시점에만 계산한다 - 평시 비용을 낮추려는 모델 쪽 설계라 여기서 바꿀 수 없다.

────────────────────────────────────────────────────────────────────────────
warmup
────────────────────────────────────────────────────────────────────────────
충전 세션 시작 후 60 판정행(=300초) 동안은 **온도 판정(T2/T3/T5)이 보류**다.
온도 오프셋을 그 구간에서 추정하기 때문이다. 그동안의 'normal' 은 '전압
기준으로는 정상' 이라는 뜻이지 온도까지 봤다는 뜻이 아니다. 판정 메시지에
그대로 실어 보내니 화면이 이 구간을 정직하게 표시해야 한다.
"""

import os
import re
import sys
import threading
from pathlib import Path

MODULE_COUNT = 16
CELLS_PER_MODULE = 11

# 모델이 낼 수 있는 판정 값. 화면이 이 값으로 색을 고른다.
STATES = ("normal", "warning", "anomaly")

# 모델이 학습된 구간. 방전은 별도 학습이 필요하다(src/README.md '적용 범위').
TRAINED_MODE = "chg"

# 측정 토픽이 실제로 실어 보내는 주기 [Hz]. **여기가 가장 틀리기 쉬운 값이다.**
#
# 모델은 5초/행 격자로 학습됐고, 입력이 그보다 촘촘하면 솎아내서 맞춘다
# (BD_SOURCE_HZ 로 알려 주면 stride 를 계산한다). 그런데 이 파이프라인은
# **이미 솎아낸 것을 보낸다** - database.py 의 RESAMPLE_SECONDS 가 5초 구간마다
# 첫 행만 남기므로(평균이 아니다. 모델의 STEP 1 과 같은 방식이다), 토픽에는
# 처음부터 5초/행이 흐른다. 그래서 0.2 Hz 이고 stride 는 1 이다.
#
# 원본 CSV(db/data/*.csv)가 1초/행인 것을 보고 1.0 을 넣으면 5행 중 1행만 써서
# 25초/행이 된다. **예외는 나지 않는다.** 점수 분포도 거의 그대로다. 대신
# 시간 창이 전부 5배로 늘어난다 - 지속 조건 10초 -> 50초, warmup 300초 ->
# 1500초, V2 기울기 창 300초 -> 1500초. 감도와 오탐률만 조용히 달라진다.
#
# database.py 의 RESAMPLE_SECONDS 를 바꾸면 이 값도 같이 바꿔야 한다.
# tests/test_detector.py 가 둘이 어긋나면 실패한다.
MEASUREMENT_SECONDS = 5.0
SOURCE_HZ = 1.0 / MEASUREMENT_SECONDS

# 측정이 이만큼 끊기면 충전 세션이 끝난 것으로 본다 [초].
#
# **database.py 가 비통전 행을 발행하지 않기 때문에 필요한 값이다.**
# 원래 모델은 정지 행이 연속으로 들어오는 것을 보고 세션 종료를 판단했다
# (StreamGate.idle_reset_rows = 60행 x 5초 = 300초). 그런데 정지 행이 아예 오지
# 않으므로 그 신호가 없어졌다. 대신 measured_at 의 공백으로 같은 판단을 한다.
#
# 안 하면: 충전이 끝나고(SOC 90) 두 시간 뒤 다음 세션이 시작돼도 모델은 그것을
# 같은 세션의 다음 행으로 본다. V2 기울기가 두 세션에 걸쳐 계산되고, T1 온도
# 오프셋은 지난 세션 것을 그대로 쓰고, warmup 이 다시 돌지 않는다.
# **예외는 나지 않는다.** 판정만 조용히 틀어진다.
#
# 값은 모델의 idle_reset_rows 와 맞춘다. load() 에서 실제 설정을 읽어 덮어쓰므로
# 여기 있는 것은 모델을 읽기 전에 쓰이는 기본값이다.
SESSION_GAP_SECONDS = 300.0

# 판정에 쓴 모델의 이름과 버전. 판정 메시지에 그대로 실어 보내므로,
# 나중에 "이 알림은 어느 모델이 낸 것인가" 를 되짚을 수 있다.
# load() 전에는 자리값이고, 로드에 성공하면 번들의 tag / created 로 바뀐다.
MODEL_NAME = "battery-anomaly"
MODEL_VERSION = "0.0.0"

_REPO_ROOT = Path(__file__).resolve().parents[2]

# DetectorPool 은 팩마다 상태를 들고 있어서 같은 팩의 연속 행이 순서대로
# 들어가야 한다. 컨슈머 스레드 하나가 순서대로 넣지만, /stats 를 부르는 HTTP
# 스레드가 같은 dict 를 훑으므로 락으로 막는다.
_lock = threading.Lock()
_pool = None

# 팩별 마지막 측정 시각. 세션 공백을 재는 데만 쓴다. _lock 이 지킨다.
_last_seen: dict[int, float] = {}


# --------------------------------------------------------------------------
# 로딩
# --------------------------------------------------------------------------

def _default_bundle() -> str:
    """models/ 에서 가장 최근 번들을 고른다."""
    found = sorted((_REPO_ROOT / "models").glob("battery_model_*.bundle"))
    if not found:
        raise RuntimeError(
            f"모델 번들이 없다. {_REPO_ROOT / 'models'} 에 battery_model_*.bundle 을 "
            "두거나 BD_ARTIFACT_BUNDLE 로 경로를 지정한다")
    # 파일명이 battery_model_{YYYYmmdd_HHMMSS}_{tag}.bundle 이라 사전순 = 시간순이다
    return str(found[-1])


def load() -> dict:
    """모델을 읽어 들인다. **api 가 기동할 때 한 번 부른다.**

    실패하면 예외를 낸다. 이 시스템은 모델과 코드가 어긋나도 예외 없이 조용히
    틀리므로(피처 순서·격자·상수가 전부 src/step*.py 쪽에 있다), 기동 시점에
    번들의 코드 해시와 실제 src/ 를 대조해 어긋나면 아예 뜨지 않게 한다.
    돌다가 틀린 알람을 내는 것보다 안 뜨는 편이 낫다.

    돌려주는 것은 모델 정보 요약이다(/stats 가 그대로 보여준다).
    """
    global _pool, MODEL_NAME, MODEL_VERSION

    # 모델팀 인수인계본이 읽는 환경변수(src/.env.example)를 그대로 쓴다.
    # 여기서 채우는 것은 이 저장소의 배치에 맞춘 기본값뿐이라, compose 나 .env 로
    # 덮어쓰면 그 문서에 적힌 대로 동작한다.
    os.environ.setdefault("BD_ARTIFACT_BUNDLE", _default_bundle())
    os.environ.setdefault("BD_SRC_DIR", str(_REPO_ROOT / "src"))
    # 측정 토픽의 주기. 위 SOURCE_HZ 주석에 왜 1.0 이 아닌지 적었다.
    os.environ.setdefault("BD_SOURCE_HZ", str(SOURCE_HZ))
    # rule 이 검출 감도가 가장 높다(임계 11.47). score 로 바꾸면 임계도 같이 바뀐다.
    os.environ.setdefault("BD_SCORE_KEY", "rule")

    # import 를 함수 안에서 하는 이유: battery_detector 는 저장소 루트에 있고
    # numpy / sklearn 을 끌어온다. 모듈 최상단에서 읽으면 이 모듈을 import 하기만
    # 해도(테스트 등) 모델 의존성이 전부 필요해진다.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from battery_detector import DetectorPool

    pool = DetectorPool()
    stats = pool.stats()

    # 세션 공백 기준을 모델 설정에서 가져온다. 모델이 정지 행으로 세션 종료를
    # 판단하던 그 값(idle_reset_rows)을 초로 바꾼 것이라, 번들이나 설정이 바뀌면
    # 여기도 따라온다. 상수를 두 곳에 적어 두면 한쪽만 고치게 된다.
    global SESSION_GAP_SECONDS
    SESSION_GAP_SECONDS = pool.st.idle_reset_rows * MEASUREMENT_SECONDS

    with _lock:
        _pool = pool
        _last_seen.clear()
    MODEL_NAME = f"battery-anomaly-{stats['tag']}"
    MODEL_VERSION = str(stats["model"])
    return info()


def is_loaded() -> bool:
    return _pool is not None


def info() -> dict:
    """모델 요약. /stats 가 보여준다."""
    if _pool is None:
        return {"name": MODEL_NAME, "version": MODEL_VERSION, "loaded": False}
    with _lock:
        s = _pool.stats()
    return {
        "name": MODEL_NAME,
        "version": MODEL_VERSION,
        "loaded": True,
        "source": s["source"],          # 어느 번들에서 읽었는가
        "score_key": s["score_key"],    # rule / score
        "threshold": s["threshold"],
        "stride": s["stride"],          # 원본 몇 행마다 1행을 판정에 쓰는가
        "warmup_rows": s["warmup_rows"],
        "persist_rows": s["persist_rows"],
        "packs": s["packs"],            # 팩별 수신·판정·알람 수, warmup 잔여
    }


def reset_pack(serial_number: int) -> bool:
    """한 팩의 상태를 비운다. 다시 warmup 부터 시작한다. 팩이 없으면 False.

    **상태가 두 겹이라 둘 다 비워야 한다.**

        reset_session()  검출기 - V2 링버퍼, T1 온도 오프셋, 지속 카운터
        gate.reset()     전처리 - 중복 판별 키(last_key), 솎기 카운터, 전류 이력

    검출기만 비우면 전처리가 마지막으로 본 측정 시각을 계속 들고 있어서, 그보다
    이른 행이 전부 '중복' 으로 버려진다. 되감은 재생을 다시 먹이는 것이 이 함수의
    주 용도인데 그때 한 행도 안 들어가고, 예외도 나지 않는다.
    """
    if _pool is None:
        return False
    with _lock:
        pack = _pool.packs.get(serial_number)
        if pack is None:
            return False
        pack.reset_session()
        pack.gate.reset()
        # 마지막 측정 시각도 잊는다. 안 그러면 되감은 재생의 첫 행이 '음수 공백'
        # 으로 읽혀 세션 경계 판단이 한 번 어긋난다.
        _last_seen.pop(serial_number, None)
    return True


# --------------------------------------------------------------------------
# 지목 파싱
#
# 모델은 사람이 읽는 라벨로 원인을 돌려준다. 화면이 쓰는 것은 숫자 번호라
# 여기서 한 번만 바꾼다.
#
#   diagnosis  '용량불량(M08CV01, conf 0.82)'  -> module 8, cell 1
#              '용접불량(M05, conf 0.71)'      -> module 5, cell None
#              '센서불량(M08T02, conf 0.90)'   -> module 8, cell None (온도 센서)
#   cause      'V9:M08CV01'      SPE 기여도 1위 열. 유형 분류가 못 짚었을 때의 대비책
#              'V8:M08CV01-CV02' 인접 셀 쌍이면 앞쪽 셀을 짚는다
# --------------------------------------------------------------------------

_LOC = re.compile(r"M(\d{2})(?:CV(\d{2})|T\d{2})?")


def _parse_location(label: str) -> tuple[int | None, int | None]:
    """'M08CV01' 꼴이 섞인 문자열 -> (모듈 1~16, 셀 1~11). 못 읽으면 (None, None)."""
    if not label:
        return None, None
    found = _LOC.search(label)
    if found is None:
        return None, None
    module = int(found.group(1))
    cell = int(found.group(2)) if found.group(2) else None
    # 범위를 벗어난 값은 짚지 않은 것으로 본다. 화면이 M00 이나 CV99 를
    # 그리려다 깨지는 것보다 '지목 없음' 이 낫다.
    if not 1 <= module <= MODULE_COUNT:
        return None, None
    if cell is not None and not 1 <= cell <= CELLS_PER_MODULE:
        cell = None
    return module, cell


def _target(result) -> tuple[int | None, int | None]:
    """알람 1건의 지목. 유형 분류가 짚은 곳을 먼저 보고, 없으면 기여도 1위 열."""
    # diagnosis 는 유형 분류(STEP 8)가 증거를 보고 고른 곳이라 cause 보다 정확하다.
    # cause 는 SPE 기여도 1위 열이라 'V8:M08CV01-CV02' 처럼 쌍으로 나올 수 있다.
    for label in (result.diagnosis, result.cause):
        module, cell = _parse_location(label)
        if module is not None:
            return module, cell
    return None, None


# --------------------------------------------------------------------------
# 모델 자리 - 여기부터가 바깥과의 계약이다
# --------------------------------------------------------------------------

def predict(row: dict, history: list[dict] | None = None) -> tuple[dict, dict] | None:
    """측정 한 건을 판정한다. (판정, 모델 정보) 를 돌려준다. 판정하지 않았으면 None.

    판정은 다섯 키를 갖는다:
        state       'normal' / 'warning' / 'anomaly'
        module      문제 모듈 번호(1~16). 지목이 없으면 None
        cell        문제 셀 번호(1~11).   지목이 없으면 None
        fault_type  불량 유형(용량불량 / 용접불량 / 센싱와이어불량 / 센서불량 /
                    미분류). 'anomaly' 가 아니면 빈 문자열
        warmup      True 면 온도 판정 보류 중

    **None 은 '이 행은 판정하지 않았다' 는 뜻이다** - 정상이 아니다. 어떤 행이
    그런지는 모듈 최상단 '판정하지 않는 행' 에 적었다.

    history 는 받지만 쓰지 않는다. 모델이 자기 상태(V2 링버퍼 61행, T1 오프셋,
    지속 카운터)를 직접 들고 있어서 앞선 측정을 다시 받을 필요가 없다.
    인터페이스는 자리표 시절 그대로 둔다 - 부르는 쪽을 고치지 않기 위해서다.
    """
    if _pool is None:
        raise RuntimeError("모델이 로드되지 않았다. api 기동 시 detector.load() 를 부른다")

    # 방전은 모델의 적용 범위 밖이다. 통전 게이트는 |I| 만 보고 부호는 보지
    # 않으므로, 방전 행도 그냥 넣으면 조용히 판정이 나온다 - 여기서 막아야 한다.
    if row["mode"] != TRAINED_MODE:
        return None

    pack_id = row["serial_number"]
    ts = row["measured_at"].timestamp()

    with _lock:
        # 세션 경계. database.py 가 비통전 행을 빼고 보내므로 모델이 정지 행을
        # 세어 세션 종료를 알아챌 수 없다. 측정 시각의 공백으로 대신 판단한다.
        # (SESSION_GAP_SECONDS 주석에 안 했을 때 무엇이 틀어지는지 적었다)
        previous = _last_seen.get(pack_id)
        if previous is not None and ts - previous > SESSION_GAP_SECONDS:
            pack = _pool.packs.get(pack_id)
            if pack is not None:
                pack.reset_session()   # 링버퍼·온도 오프셋·지속 카운터
                pack.gate.reset()      # 중복 판별 키·솎기 카운터·전류 이력
        # 역순으로 온 행은 기록하지 않는다. 뒤늦게 도착한 옛 행 하나가
        # 마지막 시각을 되돌리면 다음 행이 공백으로 잘못 읽힌다.
        if previous is None or ts > previous:
            _last_seen[pack_id] = ts

        result = _pool.feed(
            pack_id=pack_id,
            ts=ts,
            cells=row["cell_voltages"],      # 176개, M01CV01 ~ M16CV11 순
            temps=row["module_temps"],       # 32개,  M01T01 ~ M16T02 순
            soc=row["rsoc_avg"],             # 기준표 조회 키. RSOCavg 로 학습했다
            current=row["current"],          # 음수 = 충전
            # 중복·역순 판별 키. seq 는 (팩, 구간) 안에서 0부터 다시 세므로
            # 구간이 바뀌면 되감긴다. 측정 시각은 되감기지 않아 더 안전하다.
            key=ts,
        )
        threshold = _pool.threshold

    if result is None:
        return None

    if result.alarm:
        state = "anomaly"
        module, cell = _target(result)
        fault_type = result.fault_type
    elif result.score > threshold:
        # 임계는 넘었지만 지속 조건(2 판정행)에 아직 못 미친 상태.
        # 원인 분석을 하지 않는 시점이라 지목이 없다.
        state, module, cell, fault_type = "warning", None, None, ""
    else:
        state, module, cell, fault_type = "normal", None, None, ""

    return ({"state": state, "module": module, "cell": cell,
             "fault_type": fault_type, "warmup": result.warmup},
            {"name": MODEL_NAME, "version": MODEL_VERSION})


def judge(row: dict, history: list[dict] | None = None) -> dict | None:
    """측정 한 건을 판정 메시지로 만든다. 판정하지 않은 행이면 None.

    module / cell 은 지목이 없으면 None 이다. 화면은 지목이 없으면 타일 색을
    칠하지 않고 중립으로 둔다.

    verdict_id 와 detected_at 은 여기서 만들지 않는다. 그 둘은 '발행하는
    쪽의 상태' 라서, 넣으면 같은 입력에 다른 출력이 나오는 함수가 되어
    테스트가 불가능해진다. 발행 시점에 api 가 붙인다.
    """
    predicted = predict(row, history)
    if predicted is None:
        return None

    verdict, model = predicted
    state = verdict["state"]
    module, cell = verdict["module"], verdict["cell"]

    # 모델을 갈아 끼울 때 가장 흔한 사고가 라벨 불일치다('OK', 1, '이상' 등).
    # 여기서 막지 않으면 Kafka 헤더 인코딩과 화면의 STATE_KO 조회가
    # 엉뚱한 곳에서 터진다. 판정 권한이 한 곳이니 검사도 한 곳에서 한다.
    if state not in STATES:
        raise ValueError(f"모델이 낸 state 가 {STATES} 중 하나여야 하는데 {state!r} 이다")

    return {
        "event_id": row["event_id"],          # 판정 대상 측정. 추적용
        "serial_number": row["serial_number"],
        "mode": row["mode"],
        "measured_at": row["measured_at"].isoformat(),
        "seq": row["seq"],
        "state": state,
        "module": module,                     # 사람이 읽는 번호(M01~M16) 또는 None
        "cell": cell,                         # CV01~CV11 또는 None
        "fault_type": verdict["fault_type"],  # 이상일 때만. 아니면 빈 문자열
        "warmup": verdict["warmup"],          # True 면 온도 판정 보류 중
        "detail": _detail(state, module, cell,
                          verdict["fault_type"], verdict["warmup"]),
        "model": model,
    }


def _detail(state: str, module: int | None, cell: int | None,
            fault_type: str, warmup: bool) -> str:
    """사람이 읽는 한 줄 요약. 화면의 판정 카드가 이것을 그대로 쓴다."""
    if state == "anomaly":
        where = ""
        if module is not None:
            where = f"M{module:02d}" + (f" CV{cell:02d}" if cell is not None else "")
        return f"{fault_type} {where}".strip() or "이상"
    if state == "warning":
        # 지목이 없는 것이 정상이다 - 원인 분석은 알람 시점에만 돈다.
        return "임계 초과 - 지속 확인 중"
    # 정상이라도 warmup 중이면 온도는 아직 안 본 것이다. 숨기지 않는다.
    return "이상 없음(온도 판정 준비 중)" if warmup else "이상 없음"
