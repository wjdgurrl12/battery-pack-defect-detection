"""
배터리팩 이상탐지 모델 — 최종 설계

3개 스트림 혼합 구조:
  · 용접불량       → AE (모듈 편차 곡선)
  · 센서불량       → AE (온도 곡선)
  · 센싱와이어불량 → 로버스트 통계 (센싱와이어·용량 등 셀 단위 고장 통합)

판정 단위는 팩(충전 세션) 단위 합/불.
운영점은 오탐 3% (정상 30팩 중 1팩 허용).

근거 문서: diagnostics.md, joint_anomaly.md, ae_model.md
"""

from __future__ import annotations

import pickle
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
from sklearn.exceptions import ConvergenceWarning
from sklearn.neural_network import MLPRegressor

# ─────────────────────────────────────────────────────────────
# 상수 — 전처리 STEP 1~5 및 진단 실험에서 확정된 값
# ─────────────────────────────────────────────────────────────

N_MODULE = 16
N_CELL_PER_MODULE = 11
N_CELL = N_MODULE * N_CELL_PER_MODULE          # 176
N_TEMP = 32                                    # 모듈당 2개

SOC_LO, SOC_HI = 37.1, 88.8                    # 30팩 공통 구간
N_BIN = 16                                     # SOC 격자 칸 수
SOC_EDGE = np.linspace(SOC_LO, SOC_HI, N_BIN + 1)

MV = 1000.0                                    # V → mV

# 로버스트 z 의 분모 하한 [mV].
#
# 2026-08-27 수정 (인수인계본에서 바꾼 곳). 원본은 하한이 1e-9 이었는데,
# 이 프로젝트의 데이터에서는 그 값이 셀 스트림을 통째로 무력화했다.
#
# 무슨 일이 있었나: 셀 전압은 1 mV 단위로 기록된다. SOC 낮은 칸에서는 셀들이
# 촘촘히 모여 있어 잔차 176개 중 과반(1017팩 bin 0 에서 117개)이 정확히
# 0.000 mV 로 반올림된다. 그러면 MAD 가 정확히 0 이 되고, z = 2.0/1e-9 = 2e9
# 가 나온다. 정상 50팩 중 10팩에서 이 일이 일어나 임계가 3e9 로 잡혔고,
# 셀 스트림은 어떤 고장에도 반응하지 않는 죽은 스트림이 됐다.
#
# 하한을 기록 분해능으로 두는 근거: 1 mV 보다 작은 산포는 측정이 아니라
# 반올림이다. 그것으로 나누면 없는 신호를 만들어 낸다. diagnostics.md 가 같은
# 사실을 반대편에서 적어 뒀다 - "MAD 가 1 mV 의 배수로만 나온다".
#
# 1.4826 이 아니라 1.0 인 이유: diagnostics.md 가 관측한 최소 비영 sigma 는
# 1.4826(=1.4826 x 1 mV)이라 그쪽이 자연스러워 보이지만, 실측하면 진짜 고장과
# 정상 팩 사이의 여유가 좁아진다. 아래 '여유' 는 고장 팩 최저점 / 정상 팩 최대점.
#
#     하한       임계   정상최대   DEMO05   DEMO06   여유    데모 판정
#     없음       3e9      3e9      16.9     15.5    ---    아무것도 안 걸림
#     1.0        7.51    11.56     16.86    15.52   1.34   9/9 정답
#     1.4826     7.13    10.28     11.62    15.52   1.13   9/9 정답 (여유 좁음)
#     2.0        6.90     9.37      8.80    13.60   0.94   DEMO05 가 정상 아래로
MAD_FLOOR_MV = 1.0

# 오탐 운영점: 정상 N팩 중 몇 번째로 높은 점수를 임계로 쓸 것인가
# 1 = 오탐 3% (30팩 중 1팩 허용). 0이면 오탐 0%
FP_RANK = 1

STREAMS = ("cell", "module", "temp")
# 2026-08-27 결정: cell 스트림의 표시 이름을 '셀 단위 이상' 에서
# '센싱와이어불량' 으로 바꿨다. 화면·정답표(database.DEMO_PACKS)·문서가 함께
# 바뀌었고, verdictdata.json 의 fault_type enum 에는 원래 이 이름이 있었다.
#
# 주의: 이 스트림은 센싱와이어만 잡는 것이 아니다. 검출기가 하나라 용량불량
# 같은 셀 단위 고장도 전부 이 이름으로 나온다(용량불량을 심은 DEMO06 포함).
STREAM_LABEL = {
    "cell": "센싱와이어불량",
    "module": "용접불량",
    "temp": "센서불량",
}

# AE 구조 — 병목을 좁게 유지하는 것이 핵심 (ae_model.md 1-2절)
AE_ARCH = {"module": (6, 2, 6), "temp": (6, 2, 6)}
AE_KWARGS = dict(
    activation="tanh",
    alpha=1e-2,
    max_iter=800,
    early_stopping=True,
    n_iter_no_change=20,
    random_state=0,
)


# ─────────────────────────────────────────────────────────────
# 입력 데이터
# ─────────────────────────────────────────────────────────────

@dataclass
class PackData:
    """전처리 STEP 2 출력 (cache_chg/*.npz)"""
    pack_id: str
    soc: np.ndarray          # (T,)      %
    v_pack: np.ndarray       # (T,)      V
    mod_dev: np.ndarray      # (T, 16)   V
    cell_res: np.ndarray     # (T, 176)  V
    temp: np.ndarray         # (T, 32)   °C

    @classmethod
    def from_npz(cls, path: str | Path) -> "PackData":
        path = Path(path)
        d = np.load(path)
        return cls(
            pack_id=path.stem,
            soc=d["soc"].astype(float),
            v_pack=d["v_pack"].astype(float),
            mod_dev=d["mod_dev"].astype(float),
            cell_res=d["cell_res"].astype(float),
            temp=d["temp"].astype(float),
        )

    def cell_voltage(self) -> np.ndarray:
        """(T, 176) 셀 전압 복원. 항등식 cell = v_pack + mod_dev + cell_res"""
        return (
            self.v_pack[:, None]
            + np.repeat(self.mod_dev, N_CELL_PER_MODULE, axis=1)
            + self.cell_res
        )


# ─────────────────────────────────────────────────────────────
# 피처 생성
# ─────────────────────────────────────────────────────────────

def _bin_by_soc(x: np.ndarray, soc: np.ndarray) -> np.ndarray:
    """(T, K) 시계열을 SOC 16칸 평균으로 (K, 16) 변환.

    팩마다 행 수가 687~963으로 다르므로 시간축 대신 SOC축을 쓴다.
    충전 속도가 다른 팩끼리 비교하려면 이쪽이 물리적으로도 옳다.
    """
    idx = np.clip(np.digitize(soc, SOC_EDGE) - 1, 0, N_BIN - 1)
    out = np.full((x.shape[1], N_BIN), np.nan)
    for b in range(N_BIN):
        m = idx == b
        if m.any():
            out[:, b] = x[m].mean(axis=0)
    # 결측 칸은 앞뒤 값으로 보간 (공통 구간이라 정상적으로는 발생하지 않음)
    if np.isnan(out).any():
        for k in range(out.shape[0]):
            row = out[k]
            nan = np.isnan(row)
            if nan.all():
                out[k] = 0.0
            elif nan.any():
                row[nan] = np.interp(np.flatnonzero(nan), np.flatnonzero(~nan), row[~nan])
    return out


def build_curves(pack: PackData) -> dict[str, np.ndarray]:
    """세 스트림의 입력 곡선을 만든다.

    반환:
      cell   (176, 16)  모듈 기준 셀 잔차          — 통계 검출기용
      wire   (160, 16)  모듈 내 인접 셀 차          — 통계 검출기용
      module ( 16, 16)  모듈 편차                  — AE용
      temp   ( 48, 16)  센서쌍 차(16) + 센서 편차(32) — AE용
    """
    V = pack.cell_voltage()
    T, soc = pack.temp, pack.soc

    g = V.reshape(-1, N_MODULE, N_CELL_PER_MODULE)
    mod_median = np.median(g, axis=2)                       # (T, 16)

    cell_res = (g - mod_median[:, :, None]) * MV            # (T, 16, 11)
    mod_dev = (mod_median - np.median(mod_median, axis=1)[:, None]) * MV

    adjacent = (cell_res[:, :, :-1] - cell_res[:, :, 1:])   # (T, 16, 10)

    tg = T.reshape(-1, N_MODULE, 2)
    pair = np.abs(tg[:, :, 0] - tg[:, :, 1])                # (T, 16)
    # 주의: 초기 오프셋을 제거하면 상수 오프셋 고장이 지워진다 (ae_model.md 2-3절)
    dev = T - np.median(T, axis=1)[:, None]                 # (T, 32)

    return {
        "cell": _bin_by_soc(cell_res.reshape(len(V), -1), soc),
        "wire": _bin_by_soc(adjacent.reshape(len(V), -1), soc),
        "module": _bin_by_soc(mod_dev, soc),
        "temp": np.vstack([_bin_by_soc(pair, soc), _bin_by_soc(dev, soc)]),
    }


# ─────────────────────────────────────────────────────────────
# 검출기 1 — 로버스트 통계 (셀 단위 이상)
# ─────────────────────────────────────────────────────────────

def _robust_z(x: np.ndarray) -> np.ndarray:
    """팩 내부 로버스트 이상도. 중앙값에서 MAD 몇 배 떨어져 있는가.

    분모는 기록 분해능(MAD_FLOOR_MV) 아래로 내려가지 않는다. 왜 그래야
    하는지는 그 상수의 주석에 적었다 - 안 막으면 셀 스트림이 죽는다.
    """
    med = np.median(x)
    mad = np.median(np.abs(x - med)) * 1.4826
    return np.abs(x - med) / max(mad, MAD_FLOOR_MV)


def score_cell_stream(curves: dict[str, np.ndarray]) -> tuple[float, dict]:
    """셀 단위 이상 점수.

    팩 간 비교가 아니라 팩 내부 비교다. 팩마다 개성이 커서
    (모듈 편차 산포가 팩에 따라 6배 차이) 외부 기준을 쓰면 정상도 걸린다.

    SOC 16칸 각각에서 이상도를 재고 최댓값을 취한다.
    시간 평균을 쓰면 SOC 의존성이 사라져 검출력이 떨어진다.
    """
    best = {"score": -np.inf}
    for name, n_group in (("cell", N_CELL), ("wire", N_MODULE * (N_CELL_PER_MODULE - 1))):
        arr = curves[name]                          # (n_group, 16)
        for b in range(N_BIN):
            z = _robust_z(arr[:, b])
            j = int(np.argmax(z))
            if z[j] > best["score"]:
                best = {"score": float(z[j]), "kind": name, "index": j, "bin": b}
    return best["score"], best


def locate_temp(index: int) -> str:
    """온도 센서 인덱스(0~31)를 부품 이름으로 변환.

    2026-08-27 추가: 인수인계본은 이 자리를 T01~T32 로 불렀는데, 이 프로젝트의
    다른 곳(CSV 컬럼 M01T01~M16T02·데모 정답표·화면)은 전부 모듈을 붙인 표기다.
    같은 자리를 두 이름으로 부르면 판정을 정답과 대조할 때 사람이 헷갈린다.
    """
    m, s = divmod(index, 2)
    return f"M{m + 1:02d}T{s + 1:02d}"


def locate_cell(kind: str, index: int) -> str:
    """검출기 인덱스를 부품 이름으로 변환."""
    if kind == "cell":
        m, c = divmod(index, N_CELL_PER_MODULE)
        return f"M{m + 1:02d}CV{c + 1:02d}"
    m, i = divmod(index, N_CELL_PER_MODULE - 1)
    return f"M{m + 1:02d}CV{i + 1:02d}-CV{i + 2:02d}"


# ─────────────────────────────────────────────────────────────
# 검출기 2·3 — 오토인코더 (용접·센서)
# ─────────────────────────────────────────────────────────────

@dataclass
class AEStream:
    """정상 곡선만 학습하고, 복원 실패 정도를 이상도로 쓴다."""
    name: str
    net: MLPRegressor | None = None
    mu: np.ndarray | None = None
    sd: np.ndarray | None = None

    def fit(self, matrices: list[np.ndarray]) -> "AEStream":
        A = np.concatenate(matrices)
        self.mu = A.mean(axis=0)
        self.sd = A.std(axis=0) + 1e-9
        Z = (A - self.mu) / self.sd
        self.net = MLPRegressor(hidden_layer_sizes=AE_ARCH[self.name], **AE_KWARGS)
        with warnings.catch_warnings():
            # early stopping이 켜져 있으므로 max_iter 도달은 정상 종료다
            warnings.simplefilter("ignore", ConvergenceWarning)
            self.net.fit(Z, Z)
        return self

    def errors(self, matrix: np.ndarray) -> np.ndarray:
        """샘플별 재구성 RMSE."""
        Z = (matrix - self.mu) / self.sd
        return np.sqrt(((Z - self.net.predict(Z)) ** 2).mean(axis=1))

    def score(self, matrix: np.ndarray) -> tuple[float, int]:
        e = self.errors(matrix)
        j = int(np.argmax(e))
        return float(e[j]), j


# ─────────────────────────────────────────────────────────────
# 최종 모델
# ─────────────────────────────────────────────────────────────

@dataclass
class Verdict:
    pack_id: str
    passed: bool
    fault_types: list[str] = field(default_factory=list)
    detail: dict = field(default_factory=dict)

    def summary(self) -> str:
        if self.passed:
            return f"{self.pack_id}: 정상"
        return f"{self.pack_id}: 이상 — {', '.join(self.fault_types)}"


class BatteryAnomalyModel:
    """학습 → 임계 보정 → 추론.

    학습과 임계 보정이 분리되어 있다는 점이 중요하다.
      · 배포 모델은 정상 30팩 전부로 학습한다
      · 임계는 leave-one-pack-out 점수로 정한다
        (자기 자신이 학습에 포함된 채로 채점하면 점수가 낮게 나와
         임계가 과소 설정된다)
    """

    def __init__(self) -> None:
        self.ae: dict[str, AEStream] = {}
        self.threshold: dict[str, float] = {}
        self.calibration: dict[str, np.ndarray] = {}
        self.combined_fp: tuple[int, int] = (0, 0)

    # ── 학습 ────────────────────────────────────────────────
    def fit(self, packs: list[PackData]) -> "BatteryAnomalyModel":
        curves = {p.pack_id: build_curves(p) for p in packs}

        # 1) 배포용 AE — 전 팩으로 학습
        for name in ("module", "temp"):
            self.ae[name] = AEStream(name).fit([curves[p.pack_id][name] for p in packs])

        # 2) 임계 보정 — 팩 단위 leave-one-pack-out
        loo = {s: [] for s in STREAMS}
        for held in packs:
            rest = [p for p in packs if p.pack_id != held.pack_id]
            for name in ("module", "temp"):
                stream = AEStream(name).fit([curves[p.pack_id][name] for p in rest])
                loo[name].append(stream.score(curves[held.pack_id][name])[0])
            # 통계 검출기는 학습이 없으므로 그대로 채점
            loo["cell"].append(score_cell_stream(curves[held.pack_id])[0])

        for name in STREAMS:
            scores = np.asarray(loo[name])
            self.calibration[name] = np.sort(scores)[::-1]
            self.threshold[name] = float(np.sort(scores)[::-1][FP_RANK])

        # 통합 오탐률 — 세 스트림을 OR로 묶으면 팩 단위 오탐은 스트림별보다 높다.
        # 스트림당 1팩씩 허용하므로 최악의 경우 3팩까지 걸린다.
        flagged = np.zeros(len(packs), dtype=bool)
        for name in STREAMS:
            flagged |= np.asarray(loo[name]) > self.threshold[name]
        self.combined_fp = (int(flagged.sum()), len(packs))
        return self

    def report(self) -> str:
        """임계값과 오탐률 요약. 통합 오탐률을 반드시 함께 보고한다."""
        lines = [f"운영점: 스트림당 상위 {FP_RANK}팩 허용", ""]
        for name in STREAMS:
            cal = self.calibration[name]
            lines.append(
                f"  {STREAM_LABEL[name]:<10s} 임계 {self.threshold[name]:7.3f}"
                f"   정상팩 중앙 {np.median(cal):7.3f}   최대 {cal[0]:7.3f}"
            )
        n, total = self.combined_fp
        lines += [
            "",
            f"  스트림별 오탐  {100 * FP_RANK / total:.0f}% (팩 {FP_RANK}/{total})",
            f"  통합 오탐      {100 * n / total:.0f}% (팩 {n}/{total})  ← 실제 운영 수치",
            "",
            "  주의: 통합 오탐률의 신뢰구간은 표본 30팩 기준으로 매우 넓다.",
            "        정상 팩이 늘어나면 재보정할 것.",
        ]
        return "\n".join(lines)

    # ── 추론 ────────────────────────────────────────────────
    def predict(self, pack: PackData) -> Verdict:
        curves = build_curves(pack)
        detail, faults = {}, []

        # 셀 단위 이상 (통계)
        score, info = score_cell_stream(curves)
        hit = score > self.threshold["cell"]
        detail["cell"] = {
            "score": round(score, 3),
            "threshold": round(self.threshold["cell"], 3),
            "hit": hit,
            "component": locate_cell(info["kind"], info["index"]),
            "soc_bin": info["bin"],
        }
        if hit:
            faults.append(STREAM_LABEL["cell"])

        # 용접불량 (AE)
        score, j = self.ae["module"].score(curves["module"])
        hit = score > self.threshold["module"]
        detail["module"] = {
            "score": round(score, 3),
            "threshold": round(self.threshold["module"], 3),
            "hit": hit,
            "component": f"M{j + 1:02d}",
        }
        if hit:
            faults.append(STREAM_LABEL["module"])

        # 센서불량 (AE)
        score, j = self.ae["temp"].score(curves["temp"])
        hit = score > self.threshold["temp"]
        detail["temp"] = {
            "score": round(score, 3),
            "threshold": round(self.threshold["temp"], 3),
            "hit": hit,
            "component": (f"M{j + 1:02d} 센서쌍" if j < N_MODULE
                          else locate_temp(j - N_MODULE)),
        }
        if hit:
            faults.append(STREAM_LABEL["temp"])

        return Verdict(pack.pack_id, not faults, faults, detail)

    # ── 저장 / 불러오기 ─────────────────────────────────────
    def save(self, path: str | Path) -> None:
        """자체 정의 클래스를 피클에 넣지 않는다.

        스크립트를 __main__으로 실행하면 클래스 경로가 __main__.AEStream이 되어
        다른 모듈에서 불러올 때 깨진다. 원시 자료형과 sklearn 객체만 저장한다.
        """
        state = {
            "version": 1,
            "threshold": self.threshold,
            "combined_fp": self.combined_fp,
            "calibration": {k: v.tolist() for k, v in self.calibration.items()},
            "ae": {
                name: {"net": s.net, "mu": s.mu.tolist(), "sd": s.sd.tolist()}
                for name, s in self.ae.items()
            },
        }
        with open(path, "wb") as f:
            pickle.dump(state, f)

    @classmethod
    def load(cls, path: str | Path) -> "BatteryAnomalyModel":
        with open(path, "rb") as f:
            state = pickle.load(f)
        model = cls()
        model.threshold = dict(state["threshold"])
        model.combined_fp = tuple(state.get("combined_fp", (0, 0)))
        model.calibration = {k: np.asarray(v) for k, v in state["calibration"].items()}
        model.ae = {
            name: AEStream(name, d["net"], np.asarray(d["mu"]), np.asarray(d["sd"]))
            for name, d in state["ae"].items()
        }
        return model


# ─────────────────────────────────────────────────────────────
# 사용 예
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    cache = Path(sys.argv[1] if len(sys.argv) > 1 else "outputs/cache_chg")
    packs = [PackData.from_npz(p) for p in sorted(cache.glob("*.npz"))]
    print(f"정상 팩 {len(packs)}개 로드")

    model = BatteryAnomalyModel().fit(packs)
    print()
    print(model.report())

    model.save("battery_anomaly_model.pkl")

    print("\n학습 팩 재판정 (참고용 — 자기 학습 데이터이므로 낙관적)")
    for p in packs[:5]:
        print("  " + model.predict(p).summary())
