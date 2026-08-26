"""학습된 모델이 제대로 학습됐는지 검사한다.

라벨이 없는 One-class 모델은 '정확도' 로 확인할 수 없다. 대신
**학습하지 않은 것과 비교** 하는 방식으로 검사한다.

    ① 정합성   기준표·모델·정규화가 서로 같은 피처 집합을 쓰는가
    ② 정보량   정상 / 주입 / 무작위잡음 을 점수가 구분하는가
    ③ 기준표   SOC 를 섞거나 상수로 두면 성능이 무너지는가
    ④ PCA      무작위 10차원 부분공간보다 잔차를 잘 줄이는가
    ⑤ 재현성   같은 입력이면 같은 산출물이 나오는가

③④ 가 핵심이다. "학습했다" 를 주장하려면 **학습 안 한 대조군보다 나아야** 한다.
코드를 고치다 학습이 망가지면 여기서 잡힌다.

실행:
    python src/verify_model.py            # 전체 검사
    python src/verify_model.py --quick    # ⑤ 재현성 검사 생략 (빠름)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fault_injection as fi
import step1_clean as s1
import step3_features as s3
import step4_reference as s4
import step5_normalize as s5
import step6_model as s6

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT = s1.OUT_DIR
SEED = 0
NOISE_V = 0.020        # ② 무작위 잡음 크기 (셀 잔차에 더한다)
INJECT_V = -0.020      # ② 주입 고장 크기
N_RAND_SUBSPACE = 10   # ④ 무작위 부분공간 차원 (모델 주성분 수와 맞춘다)


def mad(x: np.ndarray) -> float:
    x = x[np.isfinite(x)]
    return float(np.median(np.abs(x - np.median(x))) * s4.MAD_TO_SIGMA)


# ── ① 정합성 ────────────────────────────────────────────────────────────────
def check_consistency(man: dict, ref: s4.ReferenceTable, model: s6.Model) -> bool:
    print("\n  [1] 산출물 정합성 — 파일끼리 앞뒤가 맞는가")
    # 기준표와 모델이 서로 다른 피처 집합을 쓰고 있으면 조용히 틀린 결과가 나온다
    rows = [
        ("기준표 행 수", len(ref.table), len(s5.FEATURE_ORDER) * s4.N_BINS),
        ("모델 입력 차원", model.dim, s5.N_DIM),
        ("주성분 <= 입력차원", model.n_components <= model.dim, True),
        ("기준표 피처 == 정규화 피처",
         sorted(ref.table["feature"].unique()) == sorted(s5.FEATURE_ORDER), True),
        ("학습 팩 수", len(man["valid"]), len(man["valid"])),
    ]
    ok = True
    for lab, got, want in rows:
        good = got == want
        ok &= good
        print(f"      {lab:<26}{str(got):>10}  기대 {str(want):<10}{'PASS' if good else 'FAIL'}")
    return ok


# ── ② 정보량 ────────────────────────────────────────────────────────────────
def check_informative(c: dict, ref: s4.ReferenceTable, model: s6.Model,
                      thr: float) -> bool:
    """정상 / 주입 / 무작위잡음 을 점수가 구분하는가.

    무작위 잡음에서 점수가 안 오르면 모델이 아무것도 배우지 않은 것이다.
    """
    print("\n  [2] 점수가 정보를 담고 있는가 — 세 종류 입력 비교")
    rng = np.random.default_rng(SEED)
    noisy = dict(c)
    noisy["cell_res"] = c["cell_res"] + rng.normal(0, NOISE_V, c["cell_res"].shape)
    injected = fi.inject(c, "capacity", INJECT_V, cell=88, start_frac=0.0)

    def score(cc: dict) -> np.ndarray:
        z = s5.normalize(s3.build_features(cc), ref, cc["soc"])
        return model.score(s5.feature_matrix(z))["score"]

    print(f"      {'입력':<22}{'중앙':>9}{'p99':>9}{'최대':>9}{'임계초과':>10}")
    med = {}
    for lab, cc in (("① 실제 정상", c),
                    (f"② {INJECT_V * 1000:.0f} mV 주입", injected),
                    (f"③ 무작위 잡음 {NOISE_V * 1000:.0f} mV", noisy)):
        s = score(cc)
        med[lab[0]] = float(np.median(s))
        print(f"      {lab:<22}{np.median(s):>9.2f}{np.percentile(s, 99):>9.2f}"
              f"{s.max():>9.2f}{100 * (s > thr).mean():>9.2f}%")
    # 잡음이 정상보다 훨씬 커야 하고, 주입은 그 사이에 있어야 한다
    ok = med["③"] > med["②"] > med["①"]
    print(f"      순서 ③ > ② > ① -> {'PASS' if ok else 'FAIL'}"
          f"   (잡음이 정상의 {med['③'] / max(med['①'], 1e-9):.0f}배)")
    return ok


# ── ③ 기준표 ────────────────────────────────────────────────────────────────
def check_reference(packs: list[int], ref: s4.ReferenceTable) -> bool:
    """SOC 구간별 기준표가 실제로 학습됐는가.

    귀무가설: SOC 로 나눈 게 의미 없고 아무 값이나 넣어도 같다.
    대조군 둘을 만들어 비교한다 — SOC 를 섞은 표, SOC 의존성을 없앤 상수 표.

    주의: |z| 의 중앙값과 MAD 는 셋 다 1.00 으로 같게 나온다. z 가 1 mV 단위로
    양자화돼 있어서다. 판정은 **꼬리(p99, |z|>6)** 와 **저/고SOC 균일성** 으로 한다.
    """
    print("\n  [3] 기준표가 SOC 를 배웠는가 — 가짜 기준표와 비교")
    rng = np.random.default_rng(SEED)
    tab = ref.table.copy()

    shuf = tab.copy()          # 대조군 A: 구간 순서를 섞는다
    for _, g in tab.groupby("feature"):
        idx = g.index.to_numpy()
        shuf.loc[idx, ["med", "sigma"]] = tab.loc[rng.permutation(idx),
                                                  ["med", "sigma"]].to_numpy()
    flat = tab.copy()          # 대조군 B: 전 구간 공통 상수 (SOC 의존성 제거)
    for _, g in tab.groupby("feature"):
        flat.loc[g.index, "sigma"] = g["sigma"].median()
        flat.loc[g.index, "med"] = g["med"].median()

    print(f"      {'기준표':<22}{'|z| p99':>9}{'|z|>6':>9}{'저SOC MAD':>11}"
          f"{'고SOC MAD':>11}{'비율':>7}")
    res = {}
    for lab, R in (("실제 (SOC 구간별)", ref),
                   ("대조 A: SOC 섞음", s4.ReferenceTable(shuf)),
                   ("대조 B: SOC 의존 제거", s4.ReferenceTable(flat))):
        allz, lo_, hi_ = [], [], []
        for p in packs:
            c = s3.load_cache(p)
            z = np.clip(s5.normalize(s3.build_features(c), R, c["soc"])["V1"],
                        -s5.Z_CLIP, s5.Z_CLIP)
            soc = c["soc"]
            allz.append(z[::7].ravel())
            if (soc < 45).any():
                lo_.append(z[soc < 45].ravel())
            if (soc > 80).any():
                hi_.append(z[soc > 80].ravel())
        A = np.concatenate(allz)
        L, H = mad(np.concatenate(lo_)), mad(np.concatenate(hi_))
        res[lab] = (float(np.percentile(np.abs(A), 99)),
                    100 * float((np.abs(A) > 6).mean()), H / max(L, 1e-9))
        print(f"      {lab:<22}{res[lab][0]:>9.2f}{res[lab][1]:>8.3f}%"
              f"{L:>11.2f}{H:>11.2f}{res[lab][2]:>7.2f}")

    real = res["실제 (SOC 구간별)"]
    ok = all(real[0] < res[k][0] and real[1] < res[k][1]
             and abs(real[2] - 1) < abs(res[k][2] - 1) for k in res if k != "실제 (SOC 구간별)")
    print(f"      실제가 세 지표 모두 대조군보다 낫다 -> {'PASS' if ok else 'FAIL'}")
    return ok


# ── ④ PCA ───────────────────────────────────────────────────────────────────
def check_pca(Z: np.ndarray, model: s6.Model) -> bool:
    """PCA 가 상관 구조를 배웠는가.

    귀무가설: 어떤 10차원 부분공간이든 SPE 가 같다.
    무작위 직교 부분공간에 투영했을 때보다 잔차를 잘 줄여야 '배웠다' 고 할 수 있다.
    """
    print("\n  [4] PCA 가 구조를 배웠는가 — 무작위 부분공간과 비교")
    rng = np.random.default_rng(SEED)
    k = min(N_RAND_SUBSPACE, model.n_components)
    Q, _ = np.linalg.qr(rng.standard_normal((Z.shape[1], k)))
    Zc = Z - Z.mean(axis=0)
    spe_real = float(np.median(model.spe(Z)))
    spe_rand = float(np.median(np.square(Zc - (Zc @ Q) @ Q.T).sum(axis=1)))
    total = float(np.median(np.square(Zc).sum(axis=1)))

    print(f"      {'투영':<24}{'SPE 중앙':>12}{'전체 대비':>10}")
    print(f"      {f'PCA {model.n_components}성분 (학습)':<24}{spe_real:>12.1f}"
          f"{100 * spe_real / total:>9.1f}%")
    print(f"      {f'무작위 {k}차원':<24}{spe_rand:>12.1f}{100 * spe_rand / total:>9.1f}%")
    print(f"      {'투영 없음':<24}{total:>12.1f}{100.0:>9.1f}%")
    gain = 1 - spe_real / max(spe_rand, 1e-12)
    ok = gain > 0.10        # 무작위 대비 10% 이상 줄여야 의미가 있다
    print(f"      무작위 대비 잔차 {100 * gain:.1f}% 감소 -> {'PASS' if ok else 'FAIL'}")
    return ok


# ── ⑤ 재현성 ────────────────────────────────────────────────────────────────
def check_reproducible() -> bool:
    """같은 입력이면 같은 산출물이 나오는가. 기준표를 다시 만들어 해시를 비교한다."""
    print("\n  [5] 재현성 — 기준표를 다시 만들어도 같은가")
    path = OUT / "step4_chg_reference_train.csv"
    before = hashlib.md5(path.read_bytes()).hexdigest()
    subprocess.run([sys.executable, str(Path(__file__).parent / "step4_reference.py")],
                   capture_output=True, cwd=str(s1.ROOT))
    after = hashlib.md5(path.read_bytes()).hexdigest()
    ok = before == after
    print(f"      md5 {before[:12]} -> {after[:12]}   {'PASS' if ok else 'FAIL'}")
    print(f"      IsolationForest random_state={s6.IF_KWARGS['random_state']} · "
          f"make_folds 난수 없음")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="학습된 모델 검사")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--quick", action="store_true", help="재현성 검사 생략")
    args = ap.parse_args()

    man = json.loads((OUT / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    packs = sorted(man["valid"])
    ref = s4.ReferenceTable.load(OUT / f"step4_{args.mode}_reference_train.csv")
    model = s6.load(OUT / f"model_{args.mode}_op.pkl")
    thr = json.loads((OUT / f"step7_{args.mode}_alarm_config_op.json")
                     .read_text(encoding="utf-8"))["threshold"]

    print("=" * 78)
    print("학습 모델 검사 — 라벨 없는 One-class 모델은 '학습 안 한 것' 과 비교해 확인한다")
    print("=" * 78)
    print(f"  모델 주성분 {model.n_components} · 입력 {model.dim}차원 · 임계 {thr:.2f}"
          f" · 학습 팩 {len(packs)}개")

    c = s3.load_cache(packs[0], args.mode)
    Z = s6.build_train_matrix(packs, ref, args.mode)

    results = {
        "정합성": check_consistency(man, ref, model),
        "정보량": check_informative(c, ref, model, thr),
        "기준표": check_reference(packs, ref),
        "PCA": check_pca(Z, model),
    }
    if not args.quick:
        results["재현성"] = check_reproducible()

    print("\n" + "=" * 78)
    n_ok = sum(results.values())
    for k, v in results.items():
        print(f"  {k:<10}{'PASS' if v else 'FAIL'}")
    print(f"\n  전체 {n_ok}/{len(results)} 통과")
    print("\n  [확인 못 하는 것] '실제 불량 배터리를 잡는가' 는 라벨이 없어 원리적으로 불가.")
    print("  위 검사는 '정상의 구조를 배웠고 그로부터 벗어난 것을 감지한다' 까지만 확인한다.")
    print("=" * 78)
    return 0 if n_ok == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
