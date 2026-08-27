"""STEP 4. SOC 기준표 산출 — docs/battery_guide.md 구간 4 구현.

    SOC 26% ~ 89%, 0.5% 간격 -> 127개 구간
    구간별 med / mad / sigma(=1.4826*mad) / n_packs

SOC 는 데이터 변환이 아니라 기준 조회 키로만 쓴다. 조회 시 인접 두 행을 선형보간한다.

실행:
    python src/step4_reference.py           # train 기준표 + 검증표 생성
"""

# 왜 SOC 별 기준표가 필요한가:
#   정상 셀의 편차 폭(V1)이 SOC 26% 에서 ±3 mV, 89% 에서 ±15 mV 로 5배 벌어진다.
#   단일 임계값을 쓰면 저SOC 에서는 고장을 놓치고 고SOC 에서는 정상을 오탐한다.
#   그래서 "이 SOC 에서 정상은 어디까지인가"를 표로 만들어 놓고 조회한다.
#
# 왜 평균/표준편차가 아니라 중앙값/MAD 인가:
#   기준표를 만드는 데이터에 고장이 섞여 있을 수 있다. 평균·표준편차는 이상치 몇 개에
#   끌려가지만 중앙값·MAD 는 절반이 오염되기 전까지 버틴다(로버스트 통계).
#
# 표는 두 벌 만든다:
#   _train : 홀드아웃을 뺀 팩들. 실제 운영·평가에 쓴다(자기 자신을 평가하지 않도록).
#   _all   : 학습 가용 팩 전부. 가이드 4-4 원표와 숫자를 맞대보는 용도.

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import step1_clean as s1
import step3_features as s3

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR

SOC_LO, SOC_HI, SOC_STEP = 26.0, 89.0, 0.5
N_BINS = int(round((SOC_HI - SOC_LO) / SOC_STEP)) + 1        # 127
SOC_GRID = SOC_LO + SOC_STEP * np.arange(N_BINS)             # 26.0, 26.5, ... 89.0
MAD_TO_SIGMA = 1.4826        # 정규분포에서 MAD x 1.4826 = 표준편차
LOW_CONF_PACKS = 15          # 이 미만이면 저신뢰 표시

# B안 스위치. True 면 운영용 기준표·모델을 학습 가용 팩 '전부'로 만든다.
#   근거: 성능 측정을 5-fold 교차검증(src/evaluate.py)이 맡으므로 홀드아웃을
#   따로 남길 이유가 없다. 남기면 데이터를 두 번 손해본다(모델에도 못 쓰고,
#   임계값과 성능 측정이 같은 6팩을 써서 평가도 순환이 된다).
#   False 로 두면 예전 동작(train 24팩)으로 돌아간다.
FIT_ON_ALL = True
EPS_SIGMA = 1e-9             # sigma 0 방지

# SOC 기준표를 만드는 피처 (V4 는 팩당 스칼라, T1 은 보정값이라 제외)
TABLE_FEATURES = ["V1", "V2", "V5", "V6", "V8", "V9", "T2", "T3", "T5"]

# 가이드 4-4 실측 정상 범위 (V1, mV)
# SOC 가 오를수록 폭이 넓어지는 것을 그대로 보여주는 표다
EXPECTED_V1 = {
    26: {"p1": -3.0, "p99": 3.0, "sigma3": 4.4},
    44: {"p1": -4.5, "p99": 4.5, "sigma3": 4.4},
    62: {"p1": -9.0, "p99": 9.0, "sigma3": 8.3},
    80: {"p1": -13.8, "p99": 12.8, "sigma3": 11.4},
    89: {"p1": -15.5, "p99": 13.8, "sigma3": 13.3},
}
EXPECTED_COVERAGE = (29, 42)   # 구간별 기여 팩 수


IQR_TO_SIGMA = 1.0 / 1.349       # IQR -> sigma
P98_TO_SIGMA = 1.0 / 4.652       # (p99 - p01) -> sigma


def robust_sigma(values: np.ndarray, mad: float, p01: float, p99: float) -> tuple[float, str]:
    """MAD 가 0 인 구간을 위한 척도 추정 사다리.

    V6(IQR), V9(비율), 온도처럼 양자화·한쪽꼬리 분포는 값의 과반이 같아 MAD 가 0 이
    된다. 그대로 두면 sigma 가 0 이 되어 z 가 폭주하므로 IQR -> p1~p99 순으로 내려간다.
    """
    # 세 상수(1.4826 / 1/1.349 / 1/4.652)는 모두 "정규분포였다면 표준편차 몇 배인가"의
    # 환산 계수다. 어떤 사다리 단계를 썼는지 sigma_src 로 표에 남겨 추적 가능하게 한다.
    if mad > 0:
        return MAD_TO_SIGMA * mad, "mad"
    q25, q75 = np.percentile(values, [25, 75])
    if q75 > q25:
        return float((q75 - q25) * IQR_TO_SIGMA), "iqr"
    if p99 > p01:
        return float((p99 - p01) * P98_TO_SIGMA), "p1p99"
    return EPS_SIGMA, "eps"      # 값이 전부 동일 -> 사실상 정보 없음


def soc_bin_index(soc: np.ndarray) -> np.ndarray:
    """SOC -> 구간 인덱스. 범위 밖은 -1."""
    # rint(반올림)이라 26.24 -> 0, 26.26 -> 1 처럼 가장 가까운 격자에 붙는다
    idx = np.rint((soc - SOC_LO) / SOC_STEP).astype(int)
    return np.where((idx >= 0) & (idx < N_BINS), idx, -1)


def build_reference(pack_ids: list[int], mode: str = "chg",
                    features: list[str] | None = None,
                    verbose: bool = True) -> pd.DataFrame:
    """구간별 로버스트 통계표. 피처 1종씩 스트리밍해 메모리를 억제한다."""
    # 전 팩 x 전 피처를 한 번에 올리면 수 GB 가 된다.
    # 바깥 루프를 '피처'로 두고 팩을 훑으면, 동시에 들고 있는 건 피처 1종뿐이다.
    features = features or TABLE_FEATURES
    rows: list[dict] = []

    for feat in features:
        buckets: dict[int, list[np.ndarray]] = {}       # 구간 -> 값 배열 조각들
        packs_in_bin: dict[int, set[int]] = {}          # 구간 -> 기여한 팩 집합
        for pid in pack_ids:
            c = s3.load_cache(pid, mode)
            arr = s3.build_features(c)[feat]
            idx = soc_bin_index(c["soc"])
            for b in np.unique(idx[idx >= 0]):
                # 같은 SOC 구간에 속한 모든 시점 x 모든 열을 한 덩어리로 푼다.
                # 셀/모듈을 구분하지 않는 이유: 기준표는 "정상 셀이라면 이 정도"라는
                # 전체 분포이지 특정 셀의 개별 기준이 아니다.
                v = arr[idx == b].ravel()
                v = v[np.isfinite(v)]                   # NaN(V2 분모 보호 등) 제외
                if v.size:
                    buckets.setdefault(int(b), []).append(v.astype(np.float32))
                    packs_in_bin.setdefault(int(b), set()).add(pid)

        for b in range(N_BINS):
            if b not in buckets:
                # 데이터가 한 점도 없는 구간도 행은 만들어 둔다(보간이 격자를 전제로 함)
                rows.append({"feature": feat, "soc": SOC_GRID[b], "n_packs": 0,
                             "n_values": 0, "med": np.nan, "mad": np.nan,
                             "sigma": np.nan, "sigma_src": "empty",
                             "p01": np.nan, "p99": np.nan,
                             "low_conf": True})
                continue
            v = np.concatenate(buckets[b])
            med = float(np.median(v))
            mad = float(np.median(np.abs(v - med)))     # 중앙값 절대편차
            p01, p99 = (float(x) for x in np.percentile(v, [1, 99]))
            sigma, src = robust_sigma(v, mad, p01, p99)
            rows.append({
                "feature": feat, "soc": float(SOC_GRID[b]),
                "n_packs": len(packs_in_bin[b]), "n_values": int(v.size),
                "med": med, "mad": mad, "sigma": sigma, "sigma_src": src,
                "p01": p01, "p99": p99,
                # 기여 팩이 적은 구간은 통계가 흔들린다. 표시만 하고 버리지는 않는다
                "low_conf": len(packs_in_bin[b]) < LOW_CONF_PACKS,
            })
        if verbose:
            n = sum(1 for r in rows if r["feature"] == feat and r["n_values"] > 0)
            print(f"  {feat:<3} 구간 {n}/{N_BINS} 채움", flush=True)

    return pd.DataFrame(rows)


class ReferenceTable:
    """SOC 조회용 기준표. 인접 두 행을 선형보간한다 (가이드 4-5)."""

    # 표를 CSV 그대로 두고 매번 필터링하면 느리다. 생성 시 피처별 numpy 배열로
    # 펼쳐 두고, 조회는 np.interp 한 번으로 끝낸다(실시간 추론에서 매초 호출된다).
    def __init__(self, table: pd.DataFrame):
        self.table = table
        self._med: dict[str, np.ndarray] = {}
        self._sigma: dict[str, np.ndarray] = {}
        self._npacks: dict[str, np.ndarray] = {}
        for feat, g in table.groupby("feature"):
            g = g.sort_values("soc")            # SOC_GRID 와 같은 순서로 맞춘다
            self._med[feat] = g["med"].to_numpy()
            self._sigma[feat] = g["sigma"].to_numpy()
            self._npacks[feat] = g["n_packs"].to_numpy()

    @classmethod
    def load(cls, path: Path | str) -> "ReferenceTable":
        return cls(pd.read_csv(path))

    @property
    def features(self) -> list[str]:
        return sorted(self._med)

    def _interp(self, series: np.ndarray, soc: np.ndarray) -> np.ndarray:
        # 빈 구간(NaN)은 격자에서 빼고 보간한다 -> 구멍을 양옆 값이 자연스럽게 메운다.
        # clip 으로 표 범위 밖 SOC 는 양 끝 값을 그대로 쓴다(외삽 금지).
        ok = np.isfinite(series)
        if not ok.any():
            return np.full(np.shape(soc), np.nan)
        return np.interp(np.clip(soc, SOC_LO, SOC_HI), SOC_GRID[ok], series[ok])

    def lookup(self, feature: str, soc: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """(med, sigma) 보간 조회."""
        return (self._interp(self._med[feature], np.asarray(soc, dtype=float)),
                self._interp(self._sigma[feature], np.asarray(soc, dtype=float)))

    def n_packs(self, feature: str, soc: np.ndarray) -> np.ndarray:
        # 그 SOC 구간의 신뢰도(몇 팩이 기여했는지)를 같은 방식으로 조회
        return self._interp(self._npacks[feature].astype(float),
                            np.asarray(soc, dtype=float))

    def robust_z(self, feature: str, values: np.ndarray, soc: np.ndarray) -> np.ndarray:
        """STEP 5. 구간별 로버스트 Z-score."""
        # STEP 5 의 핵심 식이 실제로는 여기 한 줄이다.
        # values 가 (T, n) 이면 시점별 med/sigma 를 열 방향으로 브로드캐스트한다.
        med, sigma = self.lookup(feature, soc)
        if values.ndim == 2:
            med, sigma = med[:, None], sigma[:, None]
        return (values - med) / np.maximum(sigma, EPS_SIGMA)   # 0 나눗셈 방지


# ── 검증 ─────────────────────────────────────────────────────────────────────
def verify(table_all: pd.DataFrame, n_packs_used: int) -> bool:
    print("\n" + "=" * 78)
    print("STEP 4 검증")
    print("=" * 78)

    # 대조는 V1(셀 편차)만 한다. 가이드 4-4 표가 V1 기준으로 주어져 있다
    v1 = table_all[table_all["feature"] == "V1"].sort_values("soc").reset_index(drop=True)

    # 1) 구간 정의 — 26~89%, 0.5% 간격, 127칸이 맞는지
    ok_bins = len(v1) == N_BINS and abs(v1["soc"].iloc[0] - 26.0) < 1e-9 \
        and abs(v1["soc"].iloc[-1] - 89.0) < 1e-9
    print(f"\n  [1] 구간 정의: SOC {v1['soc'].iloc[0]:.1f}~{v1['soc'].iloc[-1]:.1f}%, "
          f"{SOC_STEP}% 간격, {len(v1)}개 (기대 127)  -> {'PASS' if ok_bins else 'FAIL'}")

    # 2) 커버리지 — 각 구간이 몇 개 팩의 데이터로 만들어졌는지
    filled = v1[v1["n_values"] > 0]
    lo, hi = int(filled["n_packs"].min()), int(filled["n_packs"].max())
    n_low = int((filled["n_packs"] < LOW_CONF_PACKS).sum())
    ok_cov = lo >= EXPECTED_COVERAGE[0] and hi <= max(EXPECTED_COVERAGE[1], n_packs_used)
    print(f"\n  [2] 커버리지 (기대 구간별 기여 팩 {EXPECTED_COVERAGE[0]}~{EXPECTED_COVERAGE[1]}개)")
    print(f"      실측 {lo}~{hi}개 / 사용 팩 {n_packs_used}개, 저신뢰(<{LOW_CONF_PACKS}) 구간 {n_low}개"
          f"  -> {'PASS' if ok_cov else 'FAIL'}")
    if not ok_cov:
        # 기대 상한 42 는 학습 가용 팩 39개보다 크다 = 원표는 더 많은 팩으로 만든 것이다
        print(f"      기대 상한 {EXPECTED_COVERAGE[1]}은 학습 가용 팩 {n_packs_used}개를 넘어 "
              f"도달 불가능하다. 하한도 {lo}개로 {EXPECTED_COVERAGE[0] - lo}개 부족하다")
        worst = filled.nsmallest(3, "n_packs")[["soc", "n_packs"]]
        print("      가장 얇은 구간: "
              + ", ".join(f"SOC {r.soc:.1f}% {int(r.n_packs)}팩" for r in worst.itertuples()))

    # 3) 실측 정상 범위 (V1) — 가이드 원표의 5개 지점과 직접 비교
    print("\n  [3] V1 실측 정상 범위 (mV)")
    print(f"      {'SOC':>5} {'p1 실측':>9}{'기대':>8} {'p99 실측':>10}{'기대':>8} "
          f"{'3σ 실측':>10}{'기대':>8}   판정")
    ok_range = True
    for soc, exp in EXPECTED_V1.items():
        row = v1.iloc[(v1["soc"] - soc).abs().idxmin()]      # 가장 가까운 구간 행
        p01, p99, s3v = row["p01"] * 1e3, row["p99"] * 1e3, row["sigma"] * 3e3
        # 절대 1.5 mV 또는 상대 20% 이내면 일치로 본다
        def close(a, b):
            return abs(a - b) <= max(1.5, 0.2 * abs(b))
        ok = close(p01, exp["p1"]) and close(p99, exp["p99"]) and close(s3v, exp["sigma3"])
        ok_range &= ok
        print(f"      {soc:>5}{p01:>9.1f}{exp['p1']:>8.1f}{p99:>10.1f}{exp['p99']:>8.1f}"
              f"{s3v:>10.1f}{exp['sigma3']:>8.1f}   {'PASS' if ok else 'FAIL'}")

    # 4) SOC 의존성 — 단일 임계값이 왜 안 되는지
    #    저SOC 와 고SOC 의 정상 폭을 나눠 몇 배 차이인지 보여준다
    r26 = v1.iloc[(v1["soc"] - 26).abs().idxmin()]
    r89 = v1.iloc[(v1["soc"] - 89).abs().idxmin()]
    span_ratio = (r89["p99"] - r89["p01"]) / (r26["p99"] - r26["p01"])
    sigma_ratio = r89["sigma"] / r26["sigma"]
    ok_dep = span_ratio >= 4.0
    print(f"\n  [4] 정상 편차의 SOC 의존성 (가이드 '5배 변한다')")
    print(f"      p1~p99 폭: {(r26['p99'] - r26['p01']) * 1e3:.1f} -> "
          f"{(r89['p99'] - r89['p01']) * 1e3:.1f} mV = {span_ratio:.1f}배  "
          f"-> {'PASS' if ok_dep else 'FAIL'}")
    print(f"      로버스트 3σ: {r26['sigma'] * 3e3:.1f} -> {r89['sigma'] * 3e3:.1f} mV "
          f"= {sigma_ratio:.1f}배 (가이드 원표도 3.0배)")

    # 5) 보간 — 인접 두 구간의 σ 차이가 가장 큰 지점에서 확인한다
    #    차이가 큰 곳을 고르는 이유: 여기서 계단이면 조회값이 튄다는 뜻이라 가장 민감하다
    ref = ReferenceTable(table_all)
    sig = v1["sigma"].to_numpy()
    j = int(np.nanargmax(np.abs(np.diff(sig))))
    a, b = float(v1["soc"].iloc[j]), float(v1["soc"].iloc[j + 1])
    mid = (a + b) / 2
    got = ref.lookup("V1", np.array([a, mid, b]))[1]
    # 중간 지점 값이 양끝 평균과 같으면 선형보간, 어느 한쪽과 같으면 계단(최근접)이다
    ok_interp = bool(np.isclose(got[1], (got[0] + got[2]) / 2, rtol=1e-9)
                     and abs(got[2] - got[0]) > 1e-9)
    print(f"\n  [5] 선형보간 (σ 변화가 가장 큰 구간 경계 {a:.1f}~{b:.1f}%)")
    print(f"      σ({a:.1f})={got[0]*1e3:.3f}  σ({mid:.2f})={got[1]*1e3:.3f}  "
          f"σ({b:.1f})={got[2]*1e3:.3f} mV")
    print(f"      중간값 == 인접 두 행의 평균, 계단 아님  -> {'PASS' if ok_interp else 'FAIL'}")

    print("=" * 78)
    return ok_bins and ok_cov and ok_range and ok_dep and ok_interp


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 4. SOC 기준표")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--verify-only", action="store_true", help="기존 기준표로 검증만 한다")
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    valid, train = man["valid"], man["train"]

    print(f"STEP 4 SOC 기준표 — 전체 {len(valid)}팩 / 학습 {len(train)}팩")
    if args.verify_only:
        table_all = pd.read_csv(OUT_DIR / f"step4_{args.mode}_reference_all.csv")
    else:
        # 검증용(학습 가용 전체): 가이드 원표와 숫자를 맞대보는 용도
        print(f"\n  [검증용] {len(valid)}팩 기준표")
        table_all = build_reference(valid, args.mode)
        table_all.to_csv(OUT_DIR / f"step4_{args.mode}_reference_all.csv", index=False)

        # 운영용(배포용) 기준표.
        #   B안: 성능 측정은 5-fold 교차검증(src/evaluate.py)이 담당하므로
        #   배포 모델은 홀드아웃을 남기지 않고 학습 가용 팩 전부를 쓴다.
        fit_packs = valid if FIT_ON_ALL else train
        print(f"\n  [운영용] 기준표 {len(fit_packs)}팩"
              + ("  (학습 가용 전체)" if FIT_ON_ALL else "  (홀드아웃 제외)"))
        table_train = build_reference(fit_packs, args.mode)
        table_train.to_csv(OUT_DIR / f"step4_{args.mode}_reference_train.csv", index=False)

    ok = verify(table_all, len(valid))
    print(f"\n  -> outputs/step4_{args.mode}_reference_all.csv (검증용, {len(valid)}팩)")
    print(f"  -> outputs/step4_{args.mode}_reference_train.csv (운영용, "
          f"{len(valid) if FIT_ON_ALL else len(train)}팩)")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
