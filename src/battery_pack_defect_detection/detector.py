"""이상치 판정. api 가 Kafka 로 받은 측정을 이 모듈에 넘겨 판단한다.

    Kafka(battery.pack.measurement) --> api --> judge() --> Kafka(battery.pack.verdict)

2026-08-24 결정: 모든 측정을 판정해 결과를 전부 발행한다.
2026-08-25 결정: 모델은 이상 점수를 내지 않는다. 판정 결과와 지목뿐이다.
2026-08-26 결정: **판정하지 않는 행이 생긴다.** 자리표를 실제 모델로 갈아 끼우면서
    위 첫 결정이 지킬 수 없게 됐다.
2026-08-27 결정: **모델을 오토인코더(battery_anomaly.py)로 갈아탔다.**
    판정 단위가 행에서 팩(충전 세션)으로 바뀌었다 - 아래 '무엇이 달라졌나' 참고.

    나가는 것:  state(정상/주의/이상) · module(문제 모듈) · cell(문제 셀)
                fault_type(불량 유형) · warmup(판정 확정 전 여부)
    나가지 않는 것:  score · threshold · module_scores

    점수를 계속 빼두는 이유는 2026-08-25 과 같다. 모델이 안에서 재구성 오차를
    쓰더라도 그것은 모델의 사정이고, 토픽으로 나가는 것은 판정과 지목뿐이다.

════════════════════════════════════════════════════════════════════════════
무엇이 달라졌나 - 행 단위에서 팩 단위로
════════════════════════════════════════════════════════════════════════════
예전 모델(old/battery_detector.py 의 DetectorPool)은 행 하나를 받아 그 행을
판정했다.
새 모델은 **충전 세션 전체의 곡선을 SOC 16칸으로 접어서** 팩 하나의 합/불을
낸다. 행 하나만 보고는 아무 말도 할 수 없는 구조다.

그래서 이 모듈이 하는 일이 바뀌었다. 이제 팩별로 측정을 쌓아 두었다가,
일정 행마다 쌓인 것 전체로 다시 판정해 결과를 갱신한다(누적 재판정).

    5초마다 측정 도착 -> 팩 버퍼에 누적
                      -> REPREDICT_EVERY_ROWS 마다 PackData 재구성 -> predict
                      -> 판정 갱신 발행

**판정은 갱신되며 뒤집힐 수 있다.** SOC 칸이 덜 찬 동안에는 근거가 모자라서
그렇다. 그 구간을 정직하게 표시하려고 state 와 warmup 을 아래처럼 쓴다.

────────────────────────────────────────────────────────────────────────────
판정하지 않는 행 - judge() 가 None 을 돌려주는 경우
────────────────────────────────────────────────────────────────────────────
    방전        모델은 충전 구간으로만 학습됐다(mode != 'chg')
    재판정 전   누적만 하고 지나가는 행. REPREDICT_EVERY_ROWS 행마다 한 번만
                판정하므로 그 사이의 행은 결과를 내지 않는다
    근거 부족   쌓인 행이 MIN_ROWS 미만이거나 SOC 칸을 MIN_COVERAGE 만큼도
                못 채운 구간. 빈 칸을 보간해 채우면 그 칸 점수는 지어낸 값이다
    역순 도착   이미 본 시각보다 이른 행. 곡선 중간에 끼면 SOC 축이 되감긴다

이런 행에 '정상' 을 발행하면 안 된다. 판정을 안 한 것과 정상인 것은 다르고,
화면이 그 둘을 구분하지 못하면 오해를 부른다. **부르는 쪽은 None 을 받으면
아무것도 발행하지 않는다.** 화면은 마지막 판정을 계속 보여주면 된다.

예전과 달라진 점: 판정이 5초마다가 아니라 REPREDICT_EVERY_ROWS x 5초마다
갱신된다. 화면이 멈춘 것처럼 보이지 않을 만큼은 자주 온다.

────────────────────────────────────────────────────────────────────────────
state 세 값이 모델의 무엇에 대응하는가
────────────────────────────────────────────────────────────────────────────
새 모델에는 예전의 '지속 조건(2 판정행)' 이 없다. 대신 **SOC 칸이 얼마나
찼는가** 가 판정의 신뢰도를 가른다. 실측으로 세션 절반(칸 8/16)을 넘기면
판정이 더 뒤집히지 않았다.

    normal    이상 없음
    warning   이상이 잡혔으나 SOC 칸이 STABLE_COVERAGE 에 못 미쳐 미확정
    anomaly   SOC 칸이 충분히 찬 상태에서 이상

warning 에도 지목이 실린다(예전에는 비어 있었다). 새 모델은 판정과 지목을
같이 내므로 굳이 지울 이유가 없고, 화면이 미확정 구간에도 어느 모듈을
의심하는지 보여줄 수 있는 편이 낫다.

────────────────────────────────────────────────────────────────────────────
warmup
────────────────────────────────────────────────────────────────────────────
**뜻이 바뀌었다.** 예전에는 '온도 판정 보류' 였고, 지금은 '판정이 아직 확정
전' 이다(SOC 칸이 STABLE_COVERAGE 미만). 새 모델에는 온도 warmup 이 없다.
필드 이름을 그대로 둔 것은 메시지 스키마와 화면을 건드리지 않기 위해서다.
"""
from __future__ import annotations

import os
import re
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path

MODULE_COUNT = 16
CELLS_PER_MODULE = 11

# 모델이 낼 수 있는 판정 값. 화면이 이 값으로 색을 고른다.
STATES = ("normal", "warning", "anomaly")

# 모델이 학습된 구간. 방전은 별도 학습이 필요하다.
TRAINED_MODE = "chg"

# 측정 토픽이 실제로 실어 보내는 주기 [초/행].
#
# database.py 의 RESAMPLE_SECONDS 가 5초 구간마다 첫 행만 남기므로 토픽에는
# 5초/행이 흐른다. 아래의 '행' 단위 상수들이 전부 이 값을 곱해 초가 된다.
# database.py 의 RESAMPLE_SECONDS 를 바꾸면 이 값도 같이 바꿔야 한다.
# tests/test_detector.py 가 둘이 어긋나면 실패한다.
MEASUREMENT_SECONDS = 5.0
SOURCE_HZ = 1.0 / MEASUREMENT_SECONDS

# 측정이 이만큼 끊기면 충전 세션이 끝난 것으로 본다 [초].
#
# **database.py 가 비통전 행을 발행하지 않기 때문에 필요한 값이다.**
# 충전 완료 후의 정지 구간이 아예 오지 않으므로, measured_at 의 공백으로
# 세션 경계를 판단한다. 세션이 끝나면 누적 버퍼를 비운다 - 안 비우면 다음
# 세션의 곡선이 지난 세션 뒤에 이어 붙어 SOC 축이 두 번 왕복한다.
# **예외는 나지 않는다.** 판정만 조용히 틀어진다.
SESSION_GAP_SECONDS = 300.0

# 몇 행마다 다시 판정할 것인가. 30행 x 5초 = 2.5분.
#
# 매 행마다 다시 돌릴 수도 있지만 얻는 것이 없다. 판정은 SOC 칸 평균을 보는데
# 행 하나가 더해져 봐야 칸 평균은 거의 안 움직인다. 반대로 재판정 1회는 곡선
# 재구성 + AE 추론이라, 행마다 하면 발행 주기(5초) 예산을 갉아먹는다.
REPREDICT_EVERY_ROWS = 30

# 판정을 시작하는 최소 조건.
#
# MIN_ROWS 는 pack_loader 가 요구하는 값과 같아야 한다 - 그보다 적으면
# PackData 를 만드는 단계에서 예외가 난다.
#
# MIN_COVERAGE / STABLE_COVERAGE 는 SOC 16칸 중 실제로 측정이 들어온 칸의
# 비율이다. 모델은 빈 칸을 앞뒤 값으로 보간해서 채우는데, 세션 초반에는
# 높은 SOC 칸이 통째로 비어 있어 그 보간값이 곧 지어낸 점수가 된다.
#
# 실측 (데모 팩을 세션 앞부분만 잘라 판정):
#     칸  4/16 (25%)  DEMO05 가 없는 용접불량을 하나 더 붙였다
#     칸  8/16 (50%)  5팩 전부 정답. 이후 100% 까지 판정이 바뀌지 않았다
#
# 그래서 4칸까지는 아예 판정하지 않고, 8칸 미만은 미확정(warning)으로 낸다.
MIN_ROWS = 100
MIN_COVERAGE = 0.25
STABLE_COVERAGE = 0.5

# 판정에 쓴 모델의 이름과 버전. 판정 메시지에 그대로 실어 보내므로,
# 나중에 "이 알림은 어느 모델이 낸 것인가" 를 되짚을 수 있다.
MODEL_NAME = "battery-anomaly-ae"
MODEL_VERSION = "0.0.0"

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODEL = _REPO_ROOT / "models" / "battery_anomaly.pkl"

# 팩마다 누적 버퍼를 들고 있어서 같은 팩의 연속 행이 순서대로 들어가야 한다.
# 컨슈머 스레드 하나가 순서대로 넣지만, /stats 를 부르는 HTTP 스레드가 같은
# dict 를 훑으므로 락으로 막는다.
_lock = threading.Lock()
_model = None
_model_path: str | None = None


@dataclass
class _Session:
    """팩 하나의 진행 중인 충전 세션. judge() 가 여기에 측정을 쌓는다."""
    soc: list = field(default_factory=list)
    cells: list = field(default_factory=list)
    temps: list = field(default_factory=list)
    last_ts: float | None = None
    since_predict: int = 0          # 마지막 판정 이후 쌓인 행 수
    verdicts: int = 0               # 이 세션에서 판정한 횟수

    def add(self, ts: float, soc: float, cells, temps) -> None:
        self.soc.append(soc)
        self.cells.append(cells)
        self.temps.append(temps)
        self.last_ts = ts
        self.since_predict += 1

    def clear(self) -> None:
        self.soc.clear()
        self.cells.clear()
        self.temps.clear()
        self.since_predict = 0
        self.verdicts = 0


_sessions: dict[int, _Session] = {}


# --------------------------------------------------------------------------
# 로딩
# --------------------------------------------------------------------------

def load() -> dict:
    """모델을 읽어 들인다. **api 가 기동할 때 한 번 부른다.**

    실패하면 예외를 낸다. 모델이 없는 채로 떠서 판정 없이 도는 것보다
    아예 안 뜨는 편이 낫다.

    학습은 `python train_anomaly.py` 로 한다. 번들이 아니라 pkl 하나이고,
    안에는 sklearn 객체(MLPRegressor)와 임계값만 들어 있다.
    """
    global _model, _model_path, MODEL_VERSION

    path = Path(os.environ.get("BD_ANOMALY_MODEL", DEFAULT_MODEL))
    if not path.exists():
        raise RuntimeError(
            f"모델 파일이 없다: {path}\n"
            "  학습은 `python train_anomaly.py` 로 한다. "
            "다른 경로를 쓰려면 BD_ANOMALY_MODEL 로 지정한다")

    # import 를 함수 안에서 하는 이유: battery_anomaly 는 저장소 루트에 있고
    # numpy / sklearn 을 끌어온다. 모듈 최상단에서 읽으면 이 모듈을 import
    # 하기만 해도(테스트 등) 모델 의존성이 전부 필요해진다.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from battery_anomaly import BatteryAnomalyModel

    model = BatteryAnomalyModel.load(path)

    with _lock:
        _model = model
        _model_path = str(path)
        _sessions.clear()
    # 임계값이 모델의 신원이다. 같은 코드라도 보정이 다르면 다른 판정을 낸다.
    MODEL_VERSION = "-".join(f"{model.threshold[s]:.3f}"
                             for s in sorted(model.threshold))
    return info()


def is_loaded() -> bool:
    return _model is not None


def info() -> dict:
    """모델 요약. /stats 가 보여준다."""
    if _model is None:
        return {"name": MODEL_NAME, "version": MODEL_VERSION, "loaded": False}
    with _lock:
        flagged, total = _model.combined_fp
        packs = {pid: {"rows": len(s.soc),
                       "coverage": round(_coverage(s.soc), 3),
                       "verdicts": s.verdicts}
                 for pid, s in _sessions.items()}
        threshold = dict(_model.threshold)
    return {
        "name": MODEL_NAME,
        "version": MODEL_VERSION,
        "loaded": True,
        "source": _model_path,
        "threshold": threshold,            # 스트림별 임계
        "combined_fp": {"packs": flagged, "of": total},
        "repredict_every_rows": REPREDICT_EVERY_ROWS,
        "min_rows": MIN_ROWS,
        "packs": packs,                    # 팩별 누적 행 수·SOC 커버리지·판정 횟수
    }


def reset_pack(serial_number: int) -> bool:
    """한 팩의 누적 버퍼를 비운다. 다시 처음부터 쌓는다. 팩이 없으면 False.

    되감은 재생을 다시 먹일 때 쓴다. 안 비우면 되감긴 곡선이 앞의 곡선 뒤에
    이어 붙어 SOC 축이 두 번 왕복하고, 판정이 조용히 틀어진다. 마지막 측정
    시각도 함께 잊는다 - 안 그러면 되감은 재생의 첫 행이 '역순 도착' 으로
    읽혀 한 행도 안 들어간다.
    """
    with _lock:
        session = _sessions.get(serial_number)
        if session is None:
            return False
        session.clear()
        session.last_ts = None
    return True


def reset_all() -> int:
    """모든 팩의 누적 버퍼를 통째로 비운다. 비운 팩 수를 돌려준다.

    재생을 처음부터 다시 할 때 쓴다. **이것을 안 하면 되감은 재생이 한 행도
    안 들어간다.** 같은 데이터를 다시 흘리면 measured_at 이 마지막으로 본
    시각보다 이르므로 전부 '역순 도착' 으로 버려지고, 예외는 나지 않는다 -
    판정만 조용히 0건이 된다.

    reset_pack 을 팩마다 부르는 것과 달리 어떤 팩이 있었는지 몰라도 된다.
    """
    with _lock:
        count = len(_sessions)
        _sessions.clear()
    return count


# --------------------------------------------------------------------------
# SOC 커버리지
# --------------------------------------------------------------------------

def _coverage(soc) -> float:
    """SOC 16칸 중 측정이 실제로 들어온 칸의 비율.

    모델이 빈 칸을 보간해서 채우기 때문에 이 값을 봐야 한다. 행 수로는 알 수
    없다 - 낮은 SOC 에 1,000행이 몰려 있어도 채운 칸은 몇 개뿐일 수 있다.
    """
    if len(soc) == 0:
        return 0.0
    import numpy as np

    from battery_anomaly import N_BIN, SOC_EDGE

    idx = np.clip(np.digitize(np.asarray(soc, dtype=float), SOC_EDGE) - 1, 0, N_BIN - 1)
    return len(set(idx.tolist())) / N_BIN


# --------------------------------------------------------------------------
# 지목 파싱
#
# 모델은 사람이 읽는 라벨로 자리를 돌려준다. 화면이 쓰는 것은 숫자 번호라
# 여기서 한 번만 바꾼다.
#
#   'M07'              -> module 7,  cell None   (용접불량)
#   'M09CV03'          -> module 9,  cell 3      (용량불량)
#   'M05CV06-CV07'     -> module 5,  cell 6      (센싱와이어. 앞쪽 셀을 짚는다)
#   'M01T02'           -> module 1,  cell None   (온도 센서)
#   'M14 센서쌍'        -> module 14, cell None   (모듈 안 두 센서의 차)
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


def _target(detail: dict) -> tuple[int | None, int | None]:
    """걸린 스트림 중 가장 확신이 큰 곳을 짚는다.

    세 스트림이 동시에 걸릴 수 있는데 화면은 한 곳만 칠한다. 점수를 그대로
    비교하면 안 된다 - 스트림마다 단위가 달라서(셀은 로버스트 z, 나머지는
    재구성 RMSE) 큰 쪽이 곧 확신이 큰 쪽이 아니다. 임계 대비 몇 배인지로 본다.
    """
    hits = [d for d in detail.values() if d["hit"]]
    if not hits:
        return None, None
    best = max(hits, key=lambda d: d["score"] / max(d["threshold"], 1e-9))
    return _parse_location(best["component"])


# --------------------------------------------------------------------------
# 모델 자리 - 여기부터가 바깥과의 계약이다
# --------------------------------------------------------------------------

def predict(row: dict, history: list | None = None) -> tuple[dict, dict] | None:
    """측정 한 건을 받아 누적하고, 재판정 차례면 판정한다. 아니면 None.

    판정은 다섯 키를 갖는다:
        state       'normal' / 'warning' / 'anomaly'
        module      문제 모듈 번호(1~16). 지목이 없으면 None
        cell        문제 셀 번호(1~11).   지목이 없으면 None
        fault_type  불량 유형(셀 단위 이상 / 용접불량 / 센서불량). 정상이면 빈 문자열
        warmup      True 면 판정이 아직 확정 전(SOC 칸 부족)

    **None 은 '이 행으로는 판정하지 않았다' 는 뜻이다** - 정상이 아니다.
    어떤 행이 그런지는 모듈 최상단 '판정하지 않는 행' 에 적었다.

    history 는 받지만 쓰지 않는다. 이 모듈이 팩별 누적 버퍼를 직접 들고 있어서
    앞선 측정을 다시 받을 필요가 없다. 인터페이스는 부르는 쪽(main.py)을
    고치지 않으려고 그대로 둔다.
    """
    if _model is None:
        raise RuntimeError("모델이 로드되지 않았다. api 기동 시 detector.load() 를 부른다")

    # 방전은 모델의 적용 범위 밖이다. 충전 곡선으로만 학습됐다.
    if row["mode"] != TRAINED_MODE:
        return None

    pack_id = row["serial_number"]
    ts = row["measured_at"].timestamp()

    with _lock:
        session = _sessions.setdefault(pack_id, _Session())

        if session.last_ts is not None:
            gap = ts - session.last_ts
            if gap > SESSION_GAP_SECONDS:
                # 세션 경계. 지난 세션은 끝난 것이다.
                session.clear()
            elif gap < 0:
                # 역순으로 온 행은 버린다. 뒤늦게 도착한 옛 행 하나가 곡선
                # 중간에 끼어들면 SOC 축이 되감긴다. 되감은 재생은 reset_pack
                # 을 부른 뒤에 한다.
                return None

        session.add(ts, row["rsoc_avg"], row["cell_voltages"], row["module_temps"])

        # 아직 재판정 차례가 아니다. 첫 판정만 최소 조건을 채우는 즉시 낸다.
        if session.verdicts and session.since_predict < REPREDICT_EVERY_ROWS:
            return None

        # 근거가 모자란 구간. 빈 SOC 칸을 보간해 채운 점수는 지어낸 값이다.
        coverage = _coverage(session.soc)
        if len(session.soc) < MIN_ROWS or coverage < MIN_COVERAGE:
            return None

        import numpy as np

        import pack_loader

        pack = pack_loader.build(
            str(pack_id),
            np.asarray(session.soc, dtype=float),
            np.asarray(session.cells, dtype=float),
            np.asarray(session.temps, dtype=float))
        verdict = _model.predict(pack)
        session.since_predict = 0
        session.verdicts += 1

    module, cell = _target(verdict.detail)
    stable = coverage >= STABLE_COVERAGE

    if not verdict.fault_types:
        state = "normal"
    elif stable:
        state = "anomaly"
    else:
        # 이상 쪽이지만 SOC 칸이 덜 차서 뒤집힐 수 있다.
        state = "warning"

    return ({"state": state,
             "module": module,
             "cell": cell,
             "fault_type": ", ".join(verdict.fault_types),
             "warmup": not stable},
            {"name": MODEL_NAME, "version": MODEL_VERSION})


def judge(row: dict, history: list | None = None) -> dict | None:
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
        "warmup": verdict["warmup"],          # True 면 판정 확정 전
        "detail": _detail(state, module, cell,
                          verdict["fault_type"], verdict["warmup"]),
        "model": model,
    }


def _detail(state: str, module: int | None, cell: int | None,
            fault_type: str, warmup: bool) -> str:
    """사람이 읽는 한 줄 요약. 화면의 판정 카드가 이것을 그대로 쓴다."""
    where = ""
    if module is not None:
        where = f" M{module:02d}" + (f"CV{cell:02d}" if cell is not None else "")

    if state == "anomaly":
        return f"{fault_type or '이상'}{where}"
    if state == "warning":
        return f"{fault_type or '이상'}{where} (판정 확정 전)"
    return "이상 없음(판정 확정 전)" if warmup else "이상 없음"
