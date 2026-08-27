"""LOPO 교차검증 — 30팩 전체 검출 한계 곡선.

팩을 하나씩 빼면서 30번 반복한다. 폴드마다
    1. 나머지 29팩으로 SOC 기준표를 새로 만든다   (고정하면 z 계산부터 누수)
    2. 나머지 29팩으로 PCA + IsolationForest 를 학습한다
    3. 임계값을 29팩 점수의 99.9 분위에서 뽑는다   (홀드팩에서 뽑으면 순환)
    4. 홀드팩을 여러 벌로 채점한다
         - 원본 그대로            -> 오탐률 (실측)
         - 4유형 x 여러 크기 주입  -> 검출 한계 곡선

산출물
    outputs/cv_<mode>_folds.csv      폴드별 임계값·baseline·오탐
    outputs/cv_<mode>_detection.csv  폴드 x 유형 x 크기 -> 검출/분류
    outputs/cv_<mode>_summary.json   그룹별 검출 한계 집계

실행:
    python src/cross_validate.py                 # 30폴드 전체 (약 12분)
    python src/cross_validate.py --packs 1000 1001   # 일부만
    python src/cross_validate.py --n-components 40
"""

# 왜 이 파일이 따로 있는가:
#   STEP 1~9 는 "train 으로 학습하고 holdout 으로 본다"는 단일 분할 구조다.
#   그 구조에서는 임계값(STEP 7)과 성능 측정(validate.py)이 같은 6팩을 쓰기 때문에
#   오탐률이 정의상 0.1% 로 고정되어 측정이 되지 않는다.
#   여기서는 폴드마다 임계값을 학습쪽에서 뽑아 그 순환을 끊는다.

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
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
import step7_alarm as s7
import step8_classify as s8

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR

N_COMPONENTS = 10        # STEP 6 운영 모델과 동일 (팩 간 전이 비율로 선택된 값)
THRESHOLD_Q = 99.9       # STEP 7 과 동일. 단 학습쪽에서 뽑는다
PERSIST = s7.PERSIST_SEC  # 행 단위. STEP 1 이 5초/행으로 통일 -> 2행 = 10초
SCORE_KEYS = ("score", "rule")   # 통합 점수 / 룰 연속 점수

# 검출 한계 곡선용 스윕.
#   크기를 촘촘히 두는 이유: 단일 값(-12mV)만 재면 "몇 mV 부터 잡히는가"를 못 그린다.
#   0 은 대조군(주입 없음)이라 별도로 한 번만 돌린다.
SWEEP = [
    ("용량불량",      "capacity",     [-0.002, -0.004, -0.006, -0.008,
                                       -0.010, -0.012, -0.016, -0.020], "V"),
    ("용접불량",      "welding",      [-0.002, -0.004, -0.006, -0.008,
                                       -0.012, -0.016, -0.020], "V"),
    ("센싱와이어불량", "sensing_wire", [-0.004, -0.008, -0.012, -0.016, -0.020], "V"),
    ("센서불량",      "sensor",       [0.3, 0.5, 1.0, 1.5, 2.0, 3.0], "°C"),
]


def logging_group(c: dict) -> str:
    """행당 SOC 증가량으로 로깅 그룹을 판별한다 (A: 1행~1초 / BC: 1행~5초).

    SerialNumber 는 파일마다 어긋나 있어(1012 는 구간 내부에서 바뀐다) 쓸 수 없다.
    """
    # 솎기 후에는 전 팩이 같은 %SOC/행 이 되므로 SOC 로는 못 가른다.
    # STEP 1 이 요약표에 남긴 stride 를 읽는다 (5 = 원본 1초/행, 1 = 원본 5초/행).
    import pandas as _pd
    t = _pd.read_csv(OUT_DIR / "step1_chg_summary.csv").set_index("pack_id")
    return "A" if int(t.loc[c["pack_id"], "stride"]) > 1 else "BC"


def inject_target(kind: str, fold: int) -> dict:
    """폴드마다 주입 위치를 옮긴다.

    한 셀에 고정하면 그 위치의 특성만 재게 된다. 특히 모듈 양 끝 셀(CV01/CV11)은
    이웃이 1개뿐이라 V9(고립도)가 다르게 나오고 유형 분류 결과가 달라진다.
    소수 37 을 곱해 176 으로 나머지를 취하면 176 개 자리를 골고루 순회한다.
    """
    if kind == "sensor":
        return {"sensor": (fold * 7) % 32}
    if kind == "welding":
        return {"module": (fold * 5) % 16}
    cell = (fold * 37) % 176
    if kind == "sensing_wire":
        return {"cell": cell, "n_cells": 2}
    return {"cell": cell}


def alarm_run(score: np.ndarray, thr: float, persist: int = PERSIST) -> int:
    """임계 초과가 연속으로 몇 행까지 이어지는지 (최댓값)."""
    # STEP 7 의 find_alarms 와 같은 판정을 쓰되, 최장 연속 길이를 그대로 돌려준다.
    # persist 이상이면 알람 1건으로 친다.
    over = score > thr
    if not over.any():
        return 0
    padded = np.concatenate(([False], over, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    return int((edges[1::2] - edges[0::2]).max())


def count_alarms(score: np.ndarray, thr: float, persist: int = PERSIST) -> tuple[int, int]:
    """(알람 건수, 알람 지속 행수)."""
    over = score > thr
    if not over.any():
        return 0, 0
    padded = np.concatenate(([False], over, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    runs = edges[1::2] - edges[0::2]
    keep = runs >= persist
    return int(keep.sum()), int(runs[keep].sum())


class Fold:
    """폴드 1개: 29팩으로 학습한 기준표 + 모델 + 임계값."""

    def __init__(self, fit_packs: list[int], mode: str, n_components: int):
        # 1) 기준표를 홀드팩 없이 다시 만든다. 이게 LOPO 구현에서 가장 놓치기 쉽다
        self.ref = s4.ReferenceTable(s4.build_reference(fit_packs, mode, verbose=False))
        # 2) 모델
        Z = s6.build_train_matrix(fit_packs, self.ref, mode)
        # 표본 수보다 많은 주성분은 뽑을 수 없다. --packs 로 일부만 돌릴 때 걸린다
        self.n_components = min(n_components, *Z.shape)
        self.model = s6.fit(Z, self.n_components)
        # 3) 임계값 — 학습쪽 분포에서 뽑는다
        fit_scores = self.model.score(Z)
        self.thr = {k: float(np.percentile(fit_scores[k], THRESHOLD_Q)) for k in SCORE_KEYS}
        self.n_fit = len(fit_packs)
        # 4) 라벨 없이 잴 수 있는 건전성 지표: 학습 SPE 중앙값.
        #    홀드팩의 SPE 중앙값과 나누면 '팩 간 전이 비율'이 되고,
        #    1 에 가까울수록 모델이 학습 팩의 개성이 아니라 정상의 구조를 배운 것이다.
        self.fit_spe_med = float(np.median(self.model.spe(Z)))

    def evaluate(self, c: dict) -> dict:
        """캐시 1벌 -> 점수·알람·유형판정."""
        feats = s3.build_features(c)
        z = s5.normalize(feats, self.ref, c["soc"])
        Z = s5.feature_matrix(z)
        sc = self.model.score(Z)
        out = {"n_rows": len(c["soc"]),
               "spe_med": float(np.median(self.model.spe(Z)))}
        for k in SCORE_KEYS:
            n_alarm, n_sec = count_alarms(sc[k], self.thr[k])
            out[f"med_{k}"] = float(np.median(sc[k]))
            out[f"max_{k}"] = float(sc[k].max())
            out[f"run_{k}"] = alarm_run(sc[k], self.thr[k])
            out[f"hit_{k}"] = bool(out[f"run_{k}"] >= PERSIST)
            out[f"n_alarm_{k}"] = n_alarm
            out[f"n_alarm_sec_{k}"] = n_sec
        # 둘 중 하나라도 잡으면 검출로 본다(운영에서 둘을 함께 쓰는 경우)
        out["hit_either"] = bool(out["hit_score"] or out["hit_rule"])
        # 유형 판정은 검출됐을 때만 의미가 있다
        out["fault_type"] = (s8.classify(c, z, feats).fault_type
                             if out["hit_either"] else "")
        return out


def _ckpt(mode: str) -> tuple[Path, Path]:
    return (OUT_DIR / f"cv_{mode}_folds.csv", OUT_DIR / f"cv_{mode}_detection.csv")


def run(packs: list[int], mode: str = "chg", n_components: int = N_COMPONENTS,
        verbose: bool = True, resume: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    # 폴드마다 PCA full SVD 가 (수만 x 784) 행렬을 다뤄 메모리를 크게 쓴다.
    # 30폴드를 한 번에 돌리다 죽으면 전부 잃으므로 폴드마다 CSV 에 이어붙이고,
    # --resume 으로 이미 끝난 팩을 건너뛴다.
    f_path, d_path = _ckpt(mode)
    folds, detect, done = [], [], set()
    if resume and f_path.exists() and d_path.exists():
        prev_f, prev_d = pd.read_csv(f_path), pd.read_csv(d_path)
        done = set(prev_f["held_pack"].tolist())
        folds, detect = prev_f.to_dict("records"), prev_d.to_dict("records")
        if verbose:
            print(f"  이어하기: 완료된 {len(done)}팩 건너뜀 {sorted(done)}")
    t_all = time.time()

    for i, held in enumerate(packs):
        if held in done:
            continue
        t0 = time.time()
        fit_packs = [p for p in packs if p != held]
        fold = Fold(fit_packs, mode, n_components)
        c = s3.load_cache(held, mode)
        grp = logging_group(c)

        # ── 대조군: 손대지 않은 홀드팩. 여기서 나온 알람이 곧 오탐이다 ──
        base = fold.evaluate(c)
        folds.append({
            "held_pack": held, "group": grp, "n_fit": fold.n_fit,
            "n_rows": base["n_rows"],
            "thr_score": fold.thr["score"], "thr_rule": fold.thr["rule"],
            "base_med_score": base["med_score"], "base_med_rule": base["med_rule"],
            "fa_alarm_score": base["n_alarm_score"], "fa_sec_score": base["n_alarm_sec_score"],
            "fa_alarm_rule": base["n_alarm_rule"], "fa_sec_rule": base["n_alarm_sec_rule"],
            "fa_either": base["hit_either"],
            # 팩 간 전이 비율: 처음 보는 팩의 SPE 가 학습 팩의 몇 배인가
            "fit_spe_med": fold.fit_spe_med, "held_spe_med": base["spe_med"],
            "transfer_ratio": base["spe_med"] / max(fold.fit_spe_med, 1e-12),
        })
        # 대조군도 detection 표에 크기 0 으로 넣어둔다 (곡선의 왼쪽 끝)
        for label, kind, _, unit in SWEEP:
            detect.append({"held_pack": held, "group": grp, "fault": label,
                           "kind": kind, "magnitude": 0.0, "unit": unit,
                           "target": "", **{k: base[k] for k in
                                            ("hit_score", "hit_rule", "hit_either",
                                             "run_score", "run_rule",
                                             "max_score", "max_rule", "fault_type")}})

        # ── 주입 스윕 ──
        for label, kind, mags, unit in SWEEP:
            kw = inject_target(kind, i)
            tgt = ",".join(fi.target_columns(kind, **kw))
            for g in mags:
                r = fold.evaluate(fi.inject(c, kind, g, **kw))
                detect.append({"held_pack": held, "group": grp, "fault": label,
                               "kind": kind, "magnitude": g, "unit": unit,
                               "target": tgt, **{k: r[k] for k in
                                                 ("hit_score", "hit_rule", "hit_either",
                                                  "run_score", "run_rule",
                                                  "max_score", "max_rule", "fault_type")}})
        # 폴드마다 저장한다. 다음 폴드에서 죽어도 여기까지는 남는다
        pd.DataFrame(folds).to_csv(f_path, index=False)
        pd.DataFrame(detect).to_csv(d_path, index=False)

        if verbose:
            print(f"  [{i + 1:>2}/{len(packs)}] held={held} ({grp})  "
                  f"thr {fold.thr['score']:6.2f}/{fold.thr['rule']:5.2f}  "
                  f"오탐 {base['n_alarm_score']}+{base['n_alarm_rule']}건  "
                  f"({time.time() - t0:.0f}s)", flush=True)

        # PCA/IF 와 학습 행렬을 즉시 놓아준다. 안 하면 30폴드 누적으로 죽는다
        del fold
        gc.collect()

    if verbose:
        print(f"\n  총 {time.time() - t_all:.0f}s")
    return pd.DataFrame(folds), pd.DataFrame(detect)


# ── 집계 ─────────────────────────────────────────────────────────────────────
def detection_curve(det: pd.DataFrame, key: str = "hit_either") -> pd.DataFrame:
    """유형 x 크기 -> 검출률. 그룹별로도 나눈다."""
    # 그룹을 뭉치면 "A 100% / BC 0%" 가 "50%" 로 보인다. 반드시 나눠서 본다
    rows = []
    for (fault, mag, unit), g in det.groupby(["fault", "magnitude", "unit"]):
        row = {"fault": fault, "magnitude": mag, "unit": unit, "n": len(g),
               "detect_all": float(g[key].mean())}
        for grp in ("A", "BC"):
            sub = g[g["group"] == grp]
            row[f"detect_{grp}"] = float(sub[key].mean()) if len(sub) else np.nan
            row[f"n_{grp}"] = len(sub)
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["fault", "magnitude"], ascending=[True, False])


def limit_at(curve: pd.DataFrame, fault: str, col: str, target: float) -> float:
    """검출률이 target 을 처음 넘는 고장 크기 (선형보간). 없으면 NaN."""
    # "몇 mV 부터 50% 검출되는가" 를 곡선에서 읽어내는 함수.
    # 크기 절댓값 오름차순으로 정렬해 처음 target 을 넘는 지점을 보간한다.
    g = curve[(curve["fault"] == fault) & curve[col].notna()].copy()
    if g.empty:
        return float("nan")
    g["absmag"] = g["magnitude"].abs()
    g = g.sort_values("absmag")
    x, y = g["absmag"].to_numpy(), g[col].to_numpy()
    hit = np.flatnonzero(y >= target)
    if not hit.size:
        return float("nan")           # 최대 크기에서도 target 미달
    j = hit[0]
    if j == 0 or y[j] == y[j - 1]:
        return float(x[j])
    # 직전 점과 선형보간
    return float(x[j - 1] + (target - y[j - 1]) * (x[j] - x[j - 1]) / (y[j] - y[j - 1]))


def summarize(folds: pd.DataFrame, det: pd.DataFrame) -> dict:
    out = {"n_folds": len(folds),
           "groups": folds["group"].value_counts().to_dict(),
           "false_alarm": {}, "curves": {}, "limits": {}}

    # 오탐 — 대조군에서 나온 알람. 이게 유일하게 '측정'인 부분이다
    hours = float(folds["n_rows"].sum()) / 3600.0
    for k in SCORE_KEYS:
        n = int(folds[f"fa_alarm_{k}"].sum())
        out["false_alarm"][k] = {
            "n_alarm": n, "n_rows": int(folds["n_rows"].sum()),
            "packs_with_alarm": int((folds[f"fa_alarm_{k}"] > 0).sum()),
            "per_hour_1s_basis": round(n / hours, 4) if hours else None,
        }

    for key, name in (("hit_either", "either"), ("hit_score", "score"), ("hit_rule", "rule")):
        curve = detection_curve(det, key)
        out["curves"][name] = curve.to_dict(orient="records")
        out["limits"][name] = {}
        for fault in curve["fault"].unique():
            out["limits"][name][fault] = {
                col: {f"p{int(t * 100)}": limit_at(curve, fault, col, t)
                      for t in (0.5, 0.9)}
                for col in ("detect_all", "detect_A", "detect_BC")
            }
    return out


def report(folds: pd.DataFrame, det: pd.DataFrame, summary: dict) -> None:
    print("\n" + "=" * 78)
    print("LOPO 교차검증 결과")
    print("=" * 78)
    print(f"\n  폴드 {len(folds)}개  (A {summary['groups'].get('A', 0)} / "
          f"BC {summary['groups'].get('BC', 0)})")
    print(f"  임계값  통합 {folds['thr_score'].min():.2f}~{folds['thr_score'].max():.2f}  "
          f"룰 {folds['thr_rule'].min():.2f}~{folds['thr_rule'].max():.2f}")

    print("\n  [1] 오탐 (대조군 = 주입 없는 홀드팩. 이 부분만 실측이다)")
    for k in SCORE_KEYS:
        fa = summary["false_alarm"][k]
        print(f"      {k:<6} 알람 {fa['n_alarm']}건 / {fa['n_rows']:,}행, "
              f"울린 팩 {fa['packs_with_alarm']}/{len(folds)}개")

    print("")
    print("  [1-b] 모델 건전성 (라벨 없이 측정 가능)")
    tr = folds["transfer_ratio"]
    print(f"      팩 간 전이 비율  중앙 {tr.median():.2f}  범위 {tr.min():.2f}~{tr.max():.2f}"
          f"   (1 에 가까울수록 좋음. 2 초과면 학습 팩을 외운 것)")
    print(f"      2.0 초과 폴드    {int((tr > 2.0).sum())}/{len(folds)}개")
    bm = folds["base_med_score"]
    print(f"      정상 baseline    중앙 {bm.median():.2f}  범위 {bm.min():.2f}~{bm.max():.2f}")
    ts = folds["thr_score"]
    print(f"      임계값 안정성    {ts.min():.2f}~{ts.max():.2f}  "
          f"(폴드마다 학습 {int(folds['n_fit'].iloc[0])}팩에서 산출)")

    print("\n  [2] 검출 한계 곡선 (통합 또는 룰 중 하나라도 검출)")
    curve = detection_curve(det, "hit_either")
    for fault in [s[0] for s in SWEEP]:
        g = curve[curve["fault"] == fault].sort_values("magnitude", ascending=False)
        unit = g["unit"].iloc[0]
        print(f"\n      {fault}")
        print(f"        {'크기':>10}{'전체':>9}{'A그룹':>9}{'BC그룹':>9}")
        for r in g.itertuples():
            mag = "0 (대조군)" if r.magnitude == 0 else (
                f"{r.magnitude * 1000:.0f} mV" if unit == "V" else f"{r.magnitude:.1f} °C")
            print(f"        {mag:>10}{100 * r.detect_all:>8.0f}%"
                  f"{100 * r.detect_A:>8.0f}%{100 * r.detect_BC:>8.0f}%")

    print("\n  [3] 검출 한계 (검출률 50% / 90% 에 도달하는 고장 크기)")
    print(f"      {'유형':<14}{'전체 50%':>10}{'전체 90%':>10}{'A 50%':>9}{'BC 50%':>9}")
    for fault, _, _, unit in SWEEP:
        lim = summary["limits"]["either"][fault]
        fmt = lambda v: ("—" if not np.isfinite(v) else
                         (f"{v * 1000:.1f} mV" if unit == "V" else f"{v:.2f} °C"))
        print(f"      {fault:<14}{fmt(lim['detect_all']['p50']):>10}"
              f"{fmt(lim['detect_all']['p90']):>10}"
              f"{fmt(lim['detect_A']['p50']):>9}{fmt(lim['detect_BC']['p50']):>9}")

    print("\n  [4] 점수 방식 비교 (최대 주입 크기에서의 검출률)")
    print(f"      {'유형':<14}{'통합':>8}{'룰':>8}{'둘 중 하나':>12}")
    for fault, _, mags, _ in SWEEP:
        big = max(mags, key=abs)
        g = det[(det["fault"] == fault) & (det["magnitude"] == big)]
        print(f"      {fault:<14}{100 * g['hit_score'].mean():>7.0f}%"
              f"{100 * g['hit_rule'].mean():>7.0f}%{100 * g['hit_either'].mean():>11.0f}%")

    print("\n  [5] 유형 분류 정확도 (검출된 건에 한해)")
    print(f"      {'주입 유형':<14}{'검출 건수':>10}{'정답':>8}{'정확도':>9}   주요 오분류")
    for fault, _, _, _ in SWEEP:
        g = det[(det["fault"] == fault) & (det["magnitude"] != 0) & det["hit_either"]]
        if not len(g):
            print(f"      {fault:<14}{0:>10}{'—':>8}{'—':>9}")
            continue
        ok = int((g["fault_type"] == fault).sum())
        wrong = g.loc[g["fault_type"] != fault, "fault_type"].value_counts().head(2).to_dict()
        print(f"      {fault:<14}{len(g):>10}{ok:>8}{100 * ok / len(g):>8.0f}%   {wrong}")
    print("=" * 78)


def main() -> int:
    ap = argparse.ArgumentParser(description="LOPO 교차검증")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--packs", type=int, nargs="*", default=None,
                    help="일부 팩만 폴드로 돌린다 (기본: manifest 의 valid 전체)")
    ap.add_argument("--n-components", type=int, default=N_COMPONENTS)
    ap.add_argument("--resume", action="store_true",
                    help="기존 cv_*.csv 에 있는 팩은 건너뛰고 이어서 돌린다")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    packs = sorted(args.packs or man["valid"])

    print(f"LOPO 교차검증 — {len(packs)}팩, 폴드마다 기준표·모델·임계값 전부 재생성")
    print(f"  주성분 {args.n_components}개, 임계값 학습쪽 {THRESHOLD_Q}분위, "
          f"지속 {PERSIST}행")
    folds, det = run(packs, args.mode, args.n_components,
                     verbose=not args.quiet, resume=args.resume)

    summary = summarize(folds, det)
    folds.to_csv(OUT_DIR / f"cv_{args.mode}_folds.csv", index=False)
    det.to_csv(OUT_DIR / f"cv_{args.mode}_detection.csv", index=False)
    (OUT_DIR / f"cv_{args.mode}_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, default=float), encoding="utf-8")

    report(folds, det, summary)
    print(f"\n  -> outputs/cv_{args.mode}_folds.csv")
    print(f"  -> outputs/cv_{args.mode}_detection.csv")
    print(f"  -> outputs/cv_{args.mode}_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
