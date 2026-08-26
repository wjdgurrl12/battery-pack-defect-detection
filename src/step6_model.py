"""STEP 6. 모델 학습 — docs/battery_guide.md 구간 6 구현.

    6-1 룰 먼저    R1 |z(V1)|>6, R2 |z(V5)|>5, R3 |z(T2)|>6   (성능 하한선)
    6-2 PCA        n_components=0.99, SPE = 재구성 잔차 제곱합 + 기여도 분해
    6-3 IF         IsolationForest(n_estimators=200, contamination='auto', random_state=0)
    6-4 통합       score = max(robust_z_max, w1*SPE_norm, w2*IF_norm)

세 점수를 같은 척도로 놓기 위해 SPE/IF 는 train 99.9 분위수가 룰 임계(6)와
같아지도록 스케일한다. 그래야 max 가 의미를 갖는다.

실행:
    python src/step6_model.py
"""

# 세 가지 탐지기를 겹쳐 쓰는 이유:
#   룰  : 해석이 명확하고 반드시 잡아야 하는 것을 보장한다(성능의 바닥).
#   PCA : 정상 데이터의 상관 구조를 배우고, 그 구조에서 벗어난 만큼(SPE)을 잰다.
#         열별로 쪼갤 수 있어서 "어느 셀 때문인지"까지 나온다.
#   IF  : 축 정렬이 아닌 이상, PCA 가 못 보는 형태의 이상을 보조한다.
# 모델은 두 벌을 만든다: 가이드 스펙 그대로(0.99)와, 팩 간 전이도로 성분 수를 고른 운영용.

from __future__ import annotations

import argparse
import json
import pickle
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fault_injection as fi
import step1_clean as s1
import step3_features as s3
import step4_reference as s4
import step5_normalize as s5

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR

RULES = {"R1": ("V1", 6.0), "R2": ("V5", 5.0), "R3": ("T2", 6.0),
         # R4 는 가이드 6-1 에 없는 추가 룰이다. 가이드 STEP 3 이 "시작부터 고장 난
         # 센서는 오프셋에 고장이 흡수되어 T2 가 놓친다. T3 는 오프셋 추정과 무관하게
         # 작동한다"고 적어두었는데, 정작 룰에는 T2 만 있다. 주입 실험에서 +2 °C 센서
         # 고장의 z(T2)=2.9(미발화) / z(T3)=14.2(발화)로 그 서술이 그대로 재현됐다.
         "R4": ("T3", 6.0)}

# 연속 점수(score['rule'])에 넣는 룰. V5 는 제외한다.
#   모듈 편차는 팩마다 산포가 1~6 mV 로 달라 전역 기준표로는 정상 팩에서도 25% 발화한다.
#   이걸 점수에 넣으면 임계값이 통째로 올라가 다른 고장의 감도까지 깎인다.
#   용접 고장은 모듈 11셀의 V1 이 함께 움직여 R1 이 대신 잡는다(실측 8 mV 에서 100%).
SCORE_RULES = ["R1", "R3", "R4"]
PCA_VAR = 0.99
IF_KWARGS = dict(n_estimators=200, contamination="auto", random_state=0)
FIT_STRIDE = 5          # 학습 표본 추출 간격 (초)
RULE_SCALE = 6.0        # SPE/IF 를 룰 임계와 같은 척도로 맞추는 기준점
SCORE_Q = 99.9          # 스케일링 분위수
W_SPE, W_IF = 1.0, 1.0

# B안 스위치 (step4_reference.FIT_ON_ALL 과 같은 뜻).
#   True 면 배포 모델을 학습 가용 팩 전부로 학습한다. 성능 측정은
#   5-fold 교차검증(src/evaluate.py)이 맡으므로 홀드아웃을 남기지 않는다.
FIT_ON_ALL = True


@dataclass
class Model:
    # 학습 결과 + 점수 계산에 필요한 스케일 상수를 한 덩어리로 들고 다닌다.
    # spe_ref / if_ref / if_med 가 없으면 세 점수를 같은 저울에 올릴 수 없다.
    pca: PCA
    iforest: IsolationForest
    spe_ref: float          # train 99.9 분위 SPE
    if_ref: float           # train 99.9 분위 IF 이상도
    if_med: float           # IF 이상도 중앙값 (하한 기준)
    n_components: int
    dim: int

    # ── 점수 ────────────────────────────────────────────────────────────
    def spe(self, Z: np.ndarray) -> np.ndarray:
        # SPE(Squared Prediction Error): 주성분 공간으로 눌렀다 편 뒤 남은 오차의 제곱합.
        # "정상이라면 이런 조합으로 설명될 텐데, 설명 안 되는 양"이 곧 이상 정도다.
        return np.square(self.residual(Z)).sum(axis=1)

    def residual(self, Z: np.ndarray) -> np.ndarray:
        # 원본 - (투영 후 복원). transform -> inverse_transform 이 그 왕복이다
        return Z - self.pca.inverse_transform(self.pca.transform(Z))

    def spe_contrib(self, Z: np.ndarray) -> np.ndarray:
        """열별 SPE 기여도 (T, D). 어느 셀이 원인인지 그대로 나온다."""
        # 합치기 전 단계라 열 = 셀/모듈/센서 이름(COL_LABELS)으로 바로 되돌릴 수 있다
        return np.square(self.residual(Z))

    def t2(self, Z: np.ndarray) -> np.ndarray:
        """Hotelling T^2 (주성분 공간 안쪽의 이상)."""
        # SPE 는 '모델 밖', T^2 는 '모델 안에서 너무 멀리 간' 경우를 본다.
        # 현재 점수 통합에는 쓰지 않지만 진단용으로 남겨둔 표준 지표다.
        s = self.pca.transform(Z)
        return np.square(s / np.sqrt(self.pca.explained_variance_)).sum(axis=1)

    def if_score(self, Z: np.ndarray) -> np.ndarray:
        # score_samples 는 정상일수록 큰 값이라 부호를 뒤집어 '이상도'로 만든다
        return -self.iforest.score_samples(Z)

    def score(self, Z: np.ndarray) -> dict[str, np.ndarray]:
        """6-4 이상점수 통합.

        score  : 가이드 식 그대로 max(robust_z_max, w1*SPE, w2*IF)
        rule   : 6-1 룰을 연속 점수로 만든 것. 룰 임계로 나눠 6 을 곱했으므로
                 값이 6 을 넘는 순간이 곧 룰 발화다. SPE 가 784열 합이라 국소
                 고장을 희석시키는 반면, 이쪽은 해당 피처만 본다.
        """
        zmax = np.abs(Z).max(axis=1)                       # 가장 많이 벗어난 열 하나
        # SPE 를 train 99.9 분위로 나누고 6 을 곱하면 "정상 상위 0.1% = 6" 이 된다.
        # 룰 임계도 6 이므로 세 점수가 같은 눈금 위에 놓인다.
        spe_n = RULE_SCALE * self.spe(Z) / self.spe_ref
        if_raw = self.if_score(Z)
        # IF 는 0 근처가 아니라 중앙값이 바닥이므로, 중앙값을 빼서 원점을 맞춘 뒤 스케일한다
        if_n = RULE_SCALE * (if_raw - self.if_med) / max(self.if_ref - self.if_med, 1e-12)
        # 각 룰의 |z| 를 그 룰의 임계로 나눈 뒤 최댓값 -> 6 을 곱한다(발화선이 정확히 6)
        rule = RULE_SCALE * np.max([np.abs(Z[:, s5.COL_SLICE[RULES[r][0]]]).max(axis=1)
                                    / RULES[r][1] for r in SCORE_RULES], axis=0)
        return {"z_max": zmax, "spe": spe_n, "iforest": if_n, "rule": rule,
                "score": np.maximum.reduce([zmax, W_SPE * spe_n, W_IF * if_n])}


def rule_flags(z: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """6-1. 시점별 룰 발화 여부 (T,)."""
    # 열 하나라도 임계를 넘으면 그 시점은 발화(any). R2 를 포함한 4개 룰 전부 본다
    return {name: (np.abs(z[feat]) > thr).any(axis=1) for name, (feat, thr) in RULES.items()}


def build_train_matrix(pack_ids: list[int], ref: s4.ReferenceTable, mode: str,
                       stride: int = FIT_STRIDE) -> np.ndarray:
    # 1초 간격 원본을 그대로 쓰면 행이 수십만이고, 이웃한 초끼리 거의 같은 값이라
    # 정보가 늘지 않는다. 5초 간격으로 솎아 학습 시간을 1/5 로 줄인다.
    mats = []
    for pid in pack_ids:
        Z, _ = s5.pack_matrix(pid, ref, mode)
        mats.append(Z[::stride].astype(np.float32))
    return np.concatenate(mats)


N_COMP_CANDIDATES = [10, 20, 40, 80, 160, 320]
TRANSFER_MAX = 2.0        # 미학습 팩 SPE 중앙값 / 학습 팩 SPE 중앙값 허용 상한


def spe_transfer(Z_fit: np.ndarray, Z_val: np.ndarray, n_components) -> tuple[float, float, float]:
    """(fit SPE 중앙값, val SPE 중앙값, 비율). 비율이 1 에 가까울수록 팩 간 전이가 좋다."""
    # 같은 정상 데이터인데 학습에 안 쓴 팩의 SPE 가 훨씬 크다면,
    # 그 PCA 는 '정상의 구조'가 아니라 '학습 팩의 개성'을 외운 것이다.
    p = PCA(n_components=n_components, svd_solver="full").fit(Z_fit)
    f = lambda Z: np.square(Z - p.inverse_transform(p.transform(Z))).sum(axis=1)
    a, b = float(np.median(f(Z_fit))), float(np.median(f(Z_val)))
    return a, b, b / max(a, 1e-12)


def select_n_components(Z_fit: np.ndarray, Z_val: np.ndarray,
                        candidates: list[int] | None = None,
                        max_ratio: float = TRANSFER_MAX,
                        verbose: bool = True) -> tuple[int, list[tuple]]:
    """팩 단위 홀드아웃으로 주성분 수를 고른다.

    n_components=0.99 는 정규화된 784차원이 거의 등방이라 471개를 뽑고, 버려진
    313차원이 '학습 팩의 잡음 방향'을 외운다. 그러면 처음 보는 팩의 SPE 가 통째로
    부풀어 임계값이 무의미해진다. 전이 비율이 허용치 이내인 가장 큰 n 을 쓴다.
    """
    rows = []
    for n in candidates or N_COMP_CANDIDATES:
        a, b, r = spe_transfer(Z_fit, Z_val, n)
        rows.append((n, a, b, r))
        if verbose:
            print(f"    n={n:>4}  fit {a:>8.1f}  val {b:>8.1f}  val/fit {r:>5.2f}", flush=True)
    ok = [r for r in rows if r[3] <= max_ratio]
    # 조건을 만족하는 것 중 가장 큰 n (표현력은 최대로, 과적합은 허용선 안에서).
    # 하나도 없으면 차선책으로 전이 비율이 가장 좋은 n 을 쓴다.
    best = max(ok, key=lambda r: r[0])[0] if ok else min(rows, key=lambda r: r[3])[0]
    return best, rows


def fit(Z_train: np.ndarray, n_components=PCA_VAR) -> Model:
    # n_components 가 실수(0.99)면 sklearn 이 '설명분산 99%' 로 해석하고,
    # 정수면 성분 개수로 해석한다. 같은 함수로 두 모델을 다 만들 수 있는 이유다.
    pca = PCA(n_components=n_components, svd_solver="full").fit(Z_train)
    iforest = IsolationForest(**IF_KWARGS).fit(Z_train)

    # 일단 더미 스케일(1.0)로 만든 뒤, 그 모델로 train 점수를 재서 스케일을 채운다
    m = Model(pca, iforest, 1.0, 1.0, 0.0, pca.n_components_, Z_train.shape[1])
    m.spe_ref = float(np.percentile(m.spe(Z_train), SCORE_Q))
    raw = m.if_score(Z_train)
    m.if_med = float(np.median(raw))
    m.if_ref = float(np.percentile(raw, SCORE_Q))
    return m


def save(model: Model, path: Path) -> None:
    """구성요소를 딕셔너리로 저장한다. 스크립트를 __main__ 으로 돌려도 다시 읽힌다."""
    # Model 객체째로 pickle 하면 클래스 경로가 '__main__.Model' 로 박혀서,
    # 다른 스크립트(step7 등)에서 로드할 때 깨진다. dict 로 풀어 저장해 그 문제를 피한다.
    with open(path, "wb") as f:
        pickle.dump({"pca": model.pca, "iforest": model.iforest,
                     "spe_ref": model.spe_ref, "if_ref": model.if_ref,
                     "if_med": model.if_med, "n_components": model.n_components,
                     "dim": model.dim}, f)


def load(path: Path) -> Model:
    with open(path, "rb") as f:
        d = pickle.load(f)
    # 예전 형식(객체째 저장)도 읽을 수 있게 분기해 둔다
    return Model(**d) if isinstance(d, dict) else d


def top_contributors(model: Model, Z: np.ndarray, k: int = 5) -> list[tuple[str, float]]:
    """SPE 기여도 상위 열 -> [(라벨, 기여율)]."""
    # 시간축으로 합쳐 구간 전체에서 누가 제일 많이 기여했는지 보고,
    # 전체 대비 비율로 바꿔 "이 셀이 몇 % 를 설명한다"로 읽히게 한다
    contrib = model.spe_contrib(Z).sum(axis=0)
    total = contrib.sum()
    idx = np.argsort(contrib)[::-1][:k]
    return [(s5.COL_LABELS[i], float(contrib[i] / total)) for i in idx]


# ── 검증 ─────────────────────────────────────────────────────────────────────
def verify(model: Model, ref: s4.ReferenceTable, man: dict, mode: str,
           spec: bool = True) -> bool:
    # spec=True 면 가이드 스펙(설명분산 99%)을 요구하고, False 면 운영 모델 기준으로 본다
    print("\n" + "=" * 78)
    print("STEP 6 검증")
    print("=" * 78)

    # 6-1 룰 발화율 (정상 팩에서는 낮아야 한다)
    #   룰은 '하한선'이므로 정상에서 자주 울리면 임계값이 잘못 잡힌 것이다
    print("\n  [1] 룰 기반 하한선 — 정상 팩 발화율 (시점 기준)")
    print(f"      {'룰':<4}{'피처':<5}{'임계':>6}{'train %':>10}{'holdout %':>12}")
    rates = {}
    for split in ("train", "holdout"):
        fired = {r: [] for r in RULES}
        for pid in man[split]:
            c = s3.load_cache(pid, mode)
            z = s5.normalize(s3.build_features(c), ref, c["soc"])
            for r, f in rule_flags(z).items():
                fired[r].append(f)
        rates[split] = {r: 100.0 * np.concatenate(v).mean() for r, v in fired.items()}
    for r, (feat, thr) in RULES.items():
        print(f"      {r:<4}{feat:<5}{thr:>6.0f}{rates['train'][r]:>9.3f}%"
              f"{rates['holdout'][r]:>11.3f}%")
    ok_rules = all(v < 5.0 for v in rates["holdout"].values())
    print(f"      정상 팩 발화율이 모두 5% 미만  -> {'PASS' if ok_rules else 'FAIL'}")

    # 6-2 PCA. 운영 모델은 99% 규칙 대신 팩 간 전이도로 성분 수를 골랐으므로 기준이 다르다
    ev = model.pca.explained_variance_ratio_.sum()
    ok_pca = model.n_components < model.dim and (ev >= PCA_VAR - 1e-6 or not spec)
    basis = f"기대 >= {PCA_VAR * 100:.0f}%" if spec else "선택 기준은 설명분산이 아니라 팩 간 전이도"
    print(f"\n  [2] PCA: {model.dim}차원 -> 주성분 {model.n_components}개, "
          f"설명 분산 {ev * 100:.2f}% ({basis})  -> {'PASS' if ok_pca else 'FAIL'}")

    # SPE 기여도로 원인 셀 특정 — 홀드아웃 전 팩에서 반복
    #   "이상하다"만으로는 정비가 불가능하다. 어느 셀인지 짚어야 쓸모가 있다.
    cell_idx = 77                                   # M08CV01
    want = s1.CELL_COLS[cell_idx]
    print(f"\n  [3] SPE 기여도 원인 특정 ({want} 에 -12 mV 주입, 홀드아웃 {len(man['holdout'])}팩)")
    print(f"      {'팩':>6}{'주입 셀 순위':>12}{'1순위 기여 열':>26}")
    ranks, gains = [], []
    for pid in man["holdout"]:
        c = s3.load_cache(pid, mode)
        bad = fi.inject(c, "capacity", -0.012, cell=cell_idx)
        Zb = s5.feature_matrix(s5.normalize(s3.build_features(bad), ref, bad["soc"]))
        # 전체 열을 순위로 받아 주입한 셀이 몇 등인지 찾는다(못 찾으면 999)
        top = top_contributors(model, Zb, k=len(s5.COL_LABELS))
        rank = next((i + 1 for i, (lab, _) in enumerate(top) if want in lab), 999)
        ranks.append(rank)
        print(f"      {pid:>6}{rank:>12}{top[0][0]:>26}")
        # 같은 팩의 정상 점수 대비 몇 배 올랐는지(민감도)도 함께 모은다
        Zn = s5.feature_matrix(s5.normalize(s3.build_features(c), ref, c["soc"]))
        gains.append(np.median(model.score(Zb)["score"]) / np.median(model.score(Zn)["score"]))
    hit3 = float(np.mean([r <= 3 for r in ranks]))
    ok_attr = hit3 >= 0.8
    print(f"      상위 3위 안에 든 비율 {hit3 * 100:.0f}% (중앙 순위 {int(np.median(ranks))})"
          f"  -> {'PASS' if ok_attr else 'FAIL'}")

    # 6-3 IF + 6-4 통합
    pid = man["holdout"][0]
    c = s3.load_cache(pid, mode)
    Zn = s5.feature_matrix(s5.normalize(s3.build_features(c), ref, c["soc"]))
    sn = model.score(Zn)
    print(f"\n  [4] 이상점수 통합 (팩 {pid} 정상 구간, 중앙값)")
    print(f"      z_max {np.median(sn['z_max']):.2f} / SPE {np.median(sn['spe']):.2f} "
          f"/ IF {np.median(sn['iforest']):.2f} -> score {np.median(sn['score']):.2f}")
    # 통합식이 진짜 max 인지(가중치 오적용 등이 없는지) 그대로 재계산해 대조한다
    ok_fuse = bool(np.allclose(sn["score"], np.maximum.reduce(
        [sn["z_max"], W_SPE * sn["spe"], W_IF * sn["iforest"]])))
    gain = float(np.median(gains))
    ok_sep = gain >= 1.05
    print(f"      통합식이 세 성분의 max 와 일치  -> {'PASS' if ok_fuse else 'FAIL'}")
    print(f"      -12 mV 주입 시 점수 상승 (홀드아웃 중앙값) {gain:.2f}배 (>= 1.05)"
          f"  -> {'PASS' if ok_sep else 'FAIL'}")
    # 정상 구간에서 이미 어떤 성분이 점수를 지배하는지 알려준다.
    # 지배 성분이 SPE 라면, 국소 고장 1개가 784열 합에 묻혀 점수가 잘 안 오른다는 뜻이다.
    dom = max(("z_max", "spe", "iforest"), key=lambda k: np.median(sn[k]))
    print(f"      [주의] 정상 데이터에서 통합 점수를 지배하는 성분: {dom}"
          f" (median {np.median(sn[dom]):.1f}). 국소 고장은 지속시간·기여도로 판별해야 한다")
    ok_sep = ok_sep and ok_fuse

    # IF 점수가 NaN/inf 없이 계산되는지만 확인(하이퍼파라미터는 가이드 지정값 고정)
    ok_if = np.isfinite(sn["iforest"]).all()
    print(f"\n  [5] IsolationForest: n_estimators={IF_KWARGS['n_estimators']}, "
          f"contamination={IF_KWARGS['contamination']!r}, random_state={IF_KWARGS['random_state']}"
          f"  -> {'PASS' if ok_if else 'FAIL'}")
    print("=" * 78)
    return ok_rules and ok_pca and ok_attr and ok_sep and ok_if


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 6. 모델 학습")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--refit", action="store_true", help="모델을 다시 학습한다")
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    ref = s4.ReferenceTable.load(OUT_DIR / f"step4_{args.mode}_reference_train.csv")
    path = OUT_DIR / f"model_{args.mode}.pkl"           # 가이드 스펙 모델
    path_op = OUT_DIR / f"model_{args.mode}_op.pkl"     # 운영 모델

    fit_packs = man["valid"] if FIT_ON_ALL else man["train"]
    print(f"STEP 6 모델 학습 — {len(fit_packs)}팩"
          + ("  (학습 가용 전체 · B안)" if FIT_ON_ALL else "  (train)"))
    if args.refit or not path.exists() or not path_op.exists():
        Z = build_train_matrix(fit_packs, ref, args.mode)
        print(f"  학습 행렬 {Z.shape} (stride {FIT_STRIDE}초)")

        # 가이드 스펙 모델
        model = fit(Z, PCA_VAR)
        save(model, path)
        print(f"  [가이드] n_components={PCA_VAR} -> 주성분 {model.n_components}개")

        # 운영 모델: 팩 단위 홀드아웃으로 주성분 수 선택
        #   시점을 섞어 나누면(랜덤 분할) 같은 팩이 양쪽에 들어가 전이도가 과대평가된다.
        #   그래서 train 33팩을 앞 75% / 뒤 25% 로 '팩 단위'로 자른다.
        cut = int(len(fit_packs) * 0.75)
        Zf = build_train_matrix(fit_packs[:cut], ref, args.mode)
        Zv = build_train_matrix(fit_packs[cut:], ref, args.mode)
        print(f"  [운영] 주성분 수 선택 (fit {cut}팩 / val {len(fit_packs) - cut}팩)")
        n_best, rows = select_n_components(Zf, Zv)
        _, _, r99 = spe_transfer(Zf, Zv, PCA_VAR)     # 가이드 스펙의 전이도도 같이 잰다
        print(f"    n=0.99 기준(471) 전이 비율 {r99:.1f} -> 선택 n={n_best}")
        # 성분 수만 바꿔서 train 전체로 다시 학습한다
        model_op = fit(Z, n_best)
        save(model_op, path_op)
    else:
        model, model_op = load(path), load(path_op)
        print(f"  기존 모델 로드 ({path.name}, {path_op.name})")

    # 두 모델에 같은 검증을 돌려 차이를 눈으로 비교할 수 있게 한다
    ok = verify(model, ref, man, args.mode)
    print(f"\n  [운영 모델] 주성분 {model_op.n_components}개로 같은 검증 반복")
    ok_op = verify(model_op, ref, man, args.mode, spec=False)
    print(f"\n  -> outputs/model_{args.mode}.pkl (가이드 스펙, 주성분 {model.n_components})")
    print(f"  -> outputs/model_{args.mode}_op.pkl (운영, 주성분 {model_op.n_components})")
    return 0 if (ok and ok_op) else 1


if __name__ == "__main__":
    raise SystemExit(main())
