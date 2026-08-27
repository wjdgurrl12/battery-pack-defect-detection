"""시나리오 기반 평가 — 팩 단위 K-fold 교차검증 + 고장 주입.

    분할   팩을 단위로 K 등분한다 (GroupKFold). 한 팩의 행이 train 과 test 로
           갈리는 일이 없어야 하므로 행이 아니라 팩을 나눈다.
    학습   fold 의 train 팩만으로 기준표·PCA·IF·임계값을 전부 새로 만든다.
    주입   test 팩에만 넣는다. train 에 넣으면 모델이 그 고장을 정상으로 학습한다.
    지표   DR / False Alarm Rate / Detection Delay  (src/metrics.py)

K 선택 (기본 5):
    K=5 -> 폴드 크기 7/7/6/5/5, train 23~25, A:BC 1.33~2.00.
           18/5=3.6, 12/5=2.4 라 나머지가 앞쪽 폴드에 쌓인다.
    K=6 -> gcd(18,12)=6 이라 폴드마다 test 5팩(A3+BC2), train 25 로 정확히 나뉜다.
           실측 대조에서 전체 DR 표준편차 9.2% -> 8.5%.
    두 방식 모두 30팩을 정확히 한 번씩 test 하므로 평가 규모는 같다.
    K 별 결과는 outputs/eval_k{3,5,6,10,30}_* 에 보존돼 있다.

    주의: make_folds 는 K > 12 에서 빈 폴드를 만든다(A18 은 folds[0..17],
    BC12 는 folds[0..11] 로만 배분). --folds 30 은 LOPO 가 아니라 18폴드가 된다.

실행:
    python src/evaluate.py                    # 5-fold 전체 스윕 (기본)
    python src/evaluate.py --folds 30         # LOPO
    python src/evaluate.py --headline-only    # 대표 조건 1개만 (빠름)
"""

# 왜 별도 파일인가:
#   cross_validate.py 는 '검출 한계 곡선' 전용이라 크기 스윕만 한다.
#   여기서는 시나리오(파형) x 크기 x 발생시점 x 위치 를 축으로 두고
#   DR/FAR/Delay 세 지표를 한 번에 낸다. Fold 클래스는 그대로 재사용한다.

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
import cross_validate as cv
import fault_injection as fi
import metrics as M
import step1_clean as s1
import step3_features as s3
import step5_normalize as s5
import step8_classify as s8

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR
SEC_PER_ROW = s1.TARGET_SEC_PER_ROW      # 5.0
N_COMPONENTS = 10                        # STEP 6 운영 모델과 동일
# 고장 발생 후 몇 행까지 볼 것인가. 60행 = 300초.
#   고정하지 않으면 팩 길이(687~963행)와 onset 비율에 따라 관측 구간이 달라져
#   긴 팩과 이른 onset 이 구조적으로 유리해진다. DR 은 "5분 내 검출률" 이 된다.
WINDOW_ROWS = 60

# ── 시나리오 정의 ────────────────────────────────────────────────────────────
# duration 은 '초'로 적고 행으로 환산한다. 25초 = 5행.
# 대표 조건(headline)은 사용자가 보고할 문장의 조건이다.
SCENARIOS = [
    dict(sid="S1", label="셀 전압 spike",      kind="spike",         dur_sec=25,
         mags=[-0.008, -0.012, -0.020, -0.030]),
    dict(sid="S2", label="지속적 전압 저하",    kind="capacity",      dur_sec=None,
         mags=[-0.008, -0.012, -0.020, -0.030]),
    dict(sid="S3", label="점진적 셀 열화",      kind="capacity",      dur_sec=None, ramp=True,
         mags=[-0.012, -0.020, -0.030, -0.050]),
    dict(sid="S4", label="셀 전압 상승",        kind="capacity",      dur_sec=None,
         mags=[+0.008, +0.012, +0.020, +0.030]),
    # '여러 셀 동시' 는 형태가 셋이고 V5/V8/V9 반응이 전혀 다르므로 따로 시험한다.
    #   비인접 -> 용량불량 여러 건 / 인접 2셀 -> 센싱와이어 / 모듈 11셀 -> 용접
    dict(sid="S5a", label="여러 셀 동시(비인접 3셀)", kind="multi_cell",   dur_sec=None, n_cells=3,
         mags=[-0.008, -0.012, -0.020, -0.030]),
    dict(sid="S5b", label="인접 2셀(센싱와이어)",   kind="sensing_wire", dur_sec=None, n_cells=2,
         mags=[-0.008, -0.012, -0.020, -0.030]),
    dict(sid="S5c", label="모듈 11셀(용접)",       kind="welding",      dur_sec=None,
         mags=[-0.004, -0.008, -0.012, -0.020]),
    dict(sid="S7", label="온도 센서 이상",      kind="sensor",        dur_sec=None,
         mags=[0.5, 1.0, 2.0, 3.0]),
    dict(sid="S8", label="셀간 온도 편차",      kind="temp_gradient", dur_sec=None,
         mags=[0.5, 1.0, 2.0, 3.0]),
]

# 시나리오 6 '이상 위치 변경' 은 별도 항목이 아니라 모든 시나리오에 걸리는 축이다.
#   - onset : 고장 발생 시점. SOC 와 직결되므로 감도에 크게 영향한다
#             (기준표 sigma 가 SOC 26% 1.48mV -> 89% 4.45mV 로 3배 벌어진다)
#   - pos   : 셀/모듈/센서 위치. 모듈 끝 셀은 이웃이 1개뿐이라 V9 가 다르게 나온다
DEFAULT_ONSETS = (0.10, 0.30, 0.50, 0.70)
DEFAULT_POSITIONS = (
    dict(name="모듈내부", cell=3 * 11 + 4, module=3, sensor=7),    # M04CV05
    dict(name="모듈끝",   cell=8 * 11 + 0, module=8, sensor=16),   # M09CV01
)

# 사용자가 보고할 대표 조건
HEADLINE = dict(sid="S1", magnitude=-0.020, dur_sec=25)


# ── 팩 단위 K-fold 분할 ──────────────────────────────────────────────────────
def make_folds(packs: list[int], k: int, summary: pd.DataFrame) -> list[list[int]]:
    """팩을 K 등분한다. 그룹(A/BC)과 모듈편차를 동시에 층화한다.

    그냥 순서대로 자르면 한 폴드가 통째로 한 그룹이 된다. 실제로 step6 의
    주성분 선택 내부 분할이 그 상태다(val 9팩이 전부 BC그룹).
    """
    grp = {p: ("A" if int(summary.loc[p, "stride"]) > 1 else "BC") for p in packs}
    dev = {p: float(summary.loc[p, "mod_dev_std_mV"]) for p in packs}
    folds: list[list[int]] = [[] for _ in range(k)]
    # 그룹별로 편차 순 정렬 후 라운드로빈 -> 두 축이 동시에 고르게 퍼진다
    for g in ("A", "BC"):
        ordered = sorted([p for p in packs if grp[p] == g], key=lambda p: dev[p])
        for i, p in enumerate(ordered):
            folds[i % k].append(p)
    return [sorted(f) for f in folds if f]


# ── 시험 조합 ────────────────────────────────────────────────────────────────
def build_trials(onsets, positions, scenarios=None, headline_only=False) -> list[dict]:
    """(시나리오, 크기, 발생시점, 위치) 조합 목록."""
    out = []
    for scn in (scenarios or SCENARIOS):
        mags = scn["mags"]
        if headline_only:
            if scn["sid"] != HEADLINE["sid"]:
                continue
            mags = [HEADLINE["magnitude"]]
        for mag in mags:
            for onset in onsets:
                for pos in positions:
                    out.append({"sid": scn["sid"], "label": scn["label"],
                                "kind": scn["kind"], "magnitude": mag,
                                "dur_sec": scn.get("dur_sec"), "ramp": scn.get("ramp", False),
                                "n_cells": scn.get("n_cells", 2),
                                "onset": onset, "pos": pos["name"], "_pos": pos})
    return out


def run_trial(fold: cv.Fold, c: dict, t: dict, sec_per_row: float,
              window_rows: int = WINDOW_ROWS) -> dict:
    """주입 1건 -> 검출 여부·지연·유형판정."""
    dur = None if t["dur_sec"] is None else max(1, int(round(t["dur_sec"] / sec_per_row)))
    p = t["_pos"]
    cc = fi.inject(c, t["kind"], t["magnitude"],
                   cell=p["cell"], module=p["module"], sensor=p["sensor"],
                   n_cells=t["n_cells"], duration=dur,
                   ramp=t["ramp"], start_frac=t["onset"])
    feats = s3.build_features(cc)
    z = s5.normalize(feats, fold.ref, cc["soc"])
    sc = fold.model.score(s5.feature_matrix(z))
    onset = cc["onset_row"]

    # 두 점수 중 먼저 확정되는 쪽을 채택한다 (운영에서 둘을 함께 쓰는 경우).
    # window_rows 로 관측 창을 고정한다 — 팩 길이가 687~963행으로 다르고 onset 도
    # 비율이라, 고정하지 않으면 긴 팩과 이른 onset 이 구조적으로 유리해진다.
    best, which = None, ""
    for key in cv.SCORE_KEYS:
        d = M.detection(sc[key], fold.thr[key], onset, persist=cv.PERSIST,
                        sec_per_row=sec_per_row, window_rows=window_rows)
        if d["detected"] and (best is None or d["delay_sec"] < best["delay_sec"]):
            best, which = d, key
    if best is None:
        best = M.detection(sc["score"], fold.thr["score"], onset, persist=cv.PERSIST,
                           sec_per_row=sec_per_row, window_rows=window_rows)

    return {"detected": bool(best["detected"]), "by": which,
            "delay_sec": best["delay_sec"], "delay_rows": best["delay_rows"],
            # 고장 이전에 이미 울고 있었으면 이 건의 '검출' 은 고장 덕분이 아니다.
            # DR 분모에서 제외한다 (summarize 가 valid_dr 로 거른다).
            "pre_existing_alarm": bool(best["pre_existing_alarm"]),
            "onset_row": onset, "n_rows": len(cc["soc"]),
            "window_rows": best["window_rows"],
            "soc_at_onset": float(cc["soc"][onset]),
            "fault_type": s8.classify(cc, z, feats).fault_type if best["detected"] else "",
            "target": ",".join(cc["fault_meta"]["target"][:3])}


# ── 실행 ─────────────────────────────────────────────────────────────────────
def run(packs: list[int], k: int, mode: str, n_components: int,
        onsets, positions, headline_only: bool, verbose: bool = True,
        window_rows: int = WINDOW_ROWS):
    summary = pd.read_csv(OUT_DIR / f"step1_{mode}_summary.csv").set_index("pack_id")
    fold_packs = make_folds(packs, k, summary)
    trials = build_trials(onsets, positions, headline_only=headline_only)
    if verbose:
        print(f"  폴드 {len(fold_packs)}개, 폴드당 test {[len(f) for f in fold_packs]}팩")
        print(f"  시험 조합 {len(trials)}종 x test 팩 {len(packs)}개 = "
              f"{len(trials) * len(packs):,}건 + 대조군 {len(packs)}건\n")

    fold_rows, trial_rows = [], []
    t_all = time.time()
    for fi_idx, test_packs in enumerate(fold_packs, 1):
        t0 = time.time()
        train_packs = [p for p in packs if p not in test_packs]
        fold = cv.Fold(train_packs, mode, n_components)

        for held in test_packs:
            c = s3.load_cache(held, mode)
            grp = "A" if int(summary.loc[held, "stride"]) > 1 else "BC"
            spr = float(summary.loc[held, "sec_per_row"]) * int(summary.loc[held, "stride"])

            # ── 대조군: 주입 없는 원본. FAR 은 여기서만 나온다 ──
            base = fold.evaluate(c)
            fa = {}
            for key in cv.SCORE_KEYS:
                z = s5.normalize(s3.build_features(c), fold.ref, c["soc"])
                sc = fold.model.score(s5.feature_matrix(z))[key]
                fa[key] = M.false_alarm(sc, fold.thr[key], cv.PERSIST, spr)
            fold_rows.append({
                "fold": fi_idx, "held_pack": held, "group": grp,
                "n_train": len(train_packs), "n_rows": base["n_rows"],
                "sec_per_row": spr, "hours": fa["score"]["hours"],
                "thr_score": fold.thr["score"], "thr_rule": fold.thr["rule"],
                "base_med_score": base["med_score"],
                "transfer_ratio": base["spe_med"] / max(fold.fit_spe_med, 1e-12),
                "fa_n_alarm": fa["score"]["n_alarm"] + fa["rule"]["n_alarm"],
                "fa_fired": fa["score"]["fired"] or fa["rule"]["fired"],
                "fa_n_alarm_score": fa["score"]["n_alarm"],
                "fa_n_alarm_rule": fa["rule"]["n_alarm"],
            })

            # ── 주입 시험 ──
            for t in trials:
                r = run_trial(fold, c, t, spr, window_rows)
                trial_rows.append({
                    "fold": fi_idx, "held_pack": held, "group": grp,
                    "scenario": t["sid"], "label": t["label"], "kind": t["kind"],
                    "magnitude": t["magnitude"], "dur_sec": t["dur_sec"],
                    "onset": t["onset"], "pos": t["pos"], **r})

        if verbose:
            print(f"  [{fi_idx}/{len(fold_packs)}] train {len(train_packs)}팩 / "
                  f"test {test_packs}  thr {fold.thr['score']:.2f}/{fold.thr['rule']:.2f}  "
                  f"({time.time() - t0:.0f}s)", flush=True)
        del fold
        gc.collect()

    if verbose:
        print(f"\n  총 {time.time() - t_all:.0f}s")
    return pd.DataFrame(fold_rows), pd.DataFrame(trial_rows)


# ── 집계 ─────────────────────────────────────────────────────────────────────
def valid_trials(trials: pd.DataFrame) -> pd.DataFrame:
    """DR 분모에 넣을 수 있는 시험만 남긴다.

    고장 발생 이전에 이미 알람이 울고 있던 건은 그 '검출' 이 고장 덕분이 아니므로
    분모에서 뺀다. 남기면 DR 이 부풀려진다.
    """
    return trials[~trials["pre_existing_alarm"]]


def per_fold_dr(trials: pd.DataFrame, **filt) -> pd.Series:
    """조건을 만족하는 시험들의 폴드별 DR. '평균 +- 표준편차' 의 재료."""
    g = valid_trials(trials)
    for k, v in filt.items():
        g = g[g[k] == v]
    return g.groupby("fold")["detected"].mean()


def headline_stat(trials: pd.DataFrame) -> dict:
    """보고 문장에 쓸 대표 조건의 통계."""
    dr = per_fold_dr(trials, scenario=HEADLINE["sid"], magnitude=HEADLINE["magnitude"])
    g = valid_trials(trials)
    g = g[(g["scenario"] == HEADLINE["sid"]) &
          (g["magnitude"] == HEADLINE["magnitude"])]
    det = g[g["detected"]]
    return {
        "scenario": HEADLINE["sid"], "magnitude_mV": HEADLINE["magnitude"] * 1e3,
        "duration_sec": HEADLINE["dur_sec"],
        "n_folds": int(dr.size), "n_trials": int(len(g)),
        "dr_per_fold": [round(float(x), 4) for x in dr.tolist()],
        # ddof=1 = 표본 표준편차. 폴드가 모집단이 아니라 표본이므로
        "dr_mean": float(dr.mean()), "dr_std": float(dr.std(ddof=1)),
        "dr_pooled": float(g["detected"].mean()),
        "delay_med_sec": float(det["delay_sec"].median()) if len(det) else float("nan"),
        "delay_p90_sec": float(det["delay_sec"].quantile(0.9)) if len(det) else float("nan"),
    }


def summarize(folds: pd.DataFrame, trials: pd.DataFrame) -> dict:
    far = M.far_summary(folds)
    out = {"n_folds": int(folds["fold"].nunique()), "n_test_packs": int(len(folds)),
           "n_train_per_fold": int(folds["n_train"].iloc[0]),
           "n_trials": int(len(trials)),
           "n_excluded_pre_alarm": int(trials["pre_existing_alarm"].sum()),
           "window_rows": int(trials["window_rows"].median()),
           "false_alarm": far,
           "transfer_ratio": {"median": float(folds["transfer_ratio"].median()),
                              "min": float(folds["transfer_ratio"].min()),
                              "max": float(folds["transfer_ratio"].max()),
                              "over_2": int((folds["transfer_ratio"] > 2).sum())},
           "threshold": {"score_min": float(folds["thr_score"].min()),
                         "score_max": float(folds["thr_score"].max()),
                         "rule_min": float(folds["thr_rule"].min()),
                         "rule_max": float(folds["thr_rule"].max())},
           "headline": headline_stat(trials)}
    curve = M.dr_curve(valid_trials(trials), group_cols=("scenario", "magnitude"))
    out["dr_curve"] = curve.to_dict(orient="records")
    out["lod"] = {s: {"p50": M.lod(curve, s, 0.5), "p90": M.lod(curve, s, 0.9)}
                  for s in curve["scenario"].unique()}
    return out


def report(folds: pd.DataFrame, trials: pd.DataFrame, summary: dict) -> None:
    W = 78
    print("\n" + "=" * W)
    print("시나리오 평가 결과 — 팩 단위 K-fold")
    print("=" * W)
    print(f"\n  폴드 {summary['n_folds']}개 · 폴드당 train {summary['n_train_per_fold']}팩")
    print(f"  test 팩 {summary['n_test_packs']}개 · 주입 시험 {summary['n_trials']:,}건")
    th = summary["threshold"]
    print(f"  임계값  통합 {th['score_min']:.2f}~{th['score_max']:.2f}  "
          f"룰 {th['rule_min']:.2f}~{th['rule_max']:.2f}")

    fa = summary["false_alarm"]
    print(f"\n  [1] False Alarm Rate — 주입 없는 대조군 (유일한 실측 지표)")
    print(f"      알람 {fa['n_alarm']}건 / {fa['hours']:.1f}시간 = "
          f"{fa['per_hour']:.3f} 건/시간")
    print(f"      울린 팩 {fa['n_packs_fired']}/{fa['n_packs']}개 "
          f"({100 * fa['pack_fire_rate']:.1f}%)")

    tr = summary["transfer_ratio"]
    print(f"\n  [2] 모델 건전성 — 팩 간 전이 비율 "
          f"중앙 {tr['median']:.2f} ({tr['min']:.2f}~{tr['max']:.2f}), "
          f"2.0 초과 {tr['over_2']}/{summary['n_test_packs']}팩")

    print(f"\n  [3] 시나리오별 DR (전 크기 통합)")
    print(f"      {'시나리오':<18}{'n':>6}{'DR':>8}{'중앙 delay':>11}{'p90 delay':>11}")
    for sid, g in trials.groupby("scenario"):
        det = g[g["detected"]]
        lab = f"{sid} {g['label'].iloc[0]}"
        med = f"{det['delay_sec'].median():.0f}s" if len(det) else "—"
        p90 = f"{det['delay_sec'].quantile(0.9):.0f}s" if len(det) else "—"
        print(f"      {lab:<18}{len(g):>6}{100 * g['detected'].mean():>7.1f}%{med:>11}{p90:>11}")

    print(f"\n  [4] 검출 한계 (LOD50 / LOD90)")
    for sid, v in summary["lod"].items():
        unit = "mV" if trials[trials.scenario == sid]["kind"].iloc[0] not in fi.TEMP_KINDS else "°C"
        f = (lambda x: "—" if not np.isfinite(x) else
             (f"{x * 1000:.1f} {unit}" if unit == "mV" else f"{x:.2f} {unit}"))
        print(f"      {sid:<6}LOD50 {f(v['p50']):>10}   LOD90 {f(v['p90']):>10}")

    print(f"\n  [5] 발생 시점(SOC)별 DR — sigma 가 SOC 에 따라 3배 변하는 효과")
    piv = trials.pivot_table(index="scenario", columns="onset",
                             values="detected", aggfunc="mean")
    print("      " + "시나리오".ljust(10) + "".join(f"{o:>9.2f}" for o in piv.columns))
    for sid, row in piv.iterrows():
        print(f"      {sid:<10}" + "".join(f"{100 * v:>8.0f}%" for v in row))

    h = summary["headline"]
    print("\n" + "=" * W)
    print("  보고용 대표 조건")
    print("=" * W)
    print(f"  {h['scenario']} · {h['magnitude_mV']:+.0f} mV · {h['duration_sec']}초 지속")
    print(f"  폴드별 DR : {[f'{100 * x:.1f}%' for x in h['dr_per_fold']]}")
    print(f"  시험 건수 : {h['n_trials']:,}건 ({h['n_folds']}폴드)")
    print(f"\n  >>> 평균 {100 * h['dr_mean']:.1f}% ± {100 * h['dr_std']:.1f}%  "
          f"(pooled {100 * h['dr_pooled']:.1f}%)")
    print(f"  >>> 중앙 검출 지연 {h['delay_med_sec']:.0f}초 / p90 {h['delay_p90_sec']:.0f}초")
    print("=" * W)


def main() -> int:
    ap = argparse.ArgumentParser(description="시나리오 기반 K-fold 평가")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--folds", type=int, default=5,
                    help="팩 단위 폴드 수. K <= 12 만 정상 동작한다(make_folds 제약). "
                         "기본 5. K=6 이면 gcd(A 18, BC 12)=6 이라 A:BC 가 폴드마다 "
                         "정확히 1.50 이 되지만, K=5 는 3.6/2.4 로 나머지가 앞쪽 폴드에 "
                         "쌓여 크기 7/7/6/5/5, A:BC 1.33~2.00 이 된다")
    ap.add_argument("--n-components", type=int, default=N_COMPONENTS)
    ap.add_argument("--onsets", type=float, nargs="*", default=list(DEFAULT_ONSETS))
    ap.add_argument("--window", type=int, default=WINDOW_ROWS,
                    help="고장 발생 후 관측할 행 수 (기본 60행 = 300초)")
    ap.add_argument("--headline-only", action="store_true",
                    help="대표 조건 1개만 돌린다 (빠른 확인용)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    packs = sorted(man["valid"])
    k = min(args.folds, len(packs))

    print(f"시나리오 평가 — 팩 {len(packs)}개, {k}-fold "
          f"({'LOPO' if k == len(packs) else '팩 단위 GroupKFold'})")
    print(f"  주성분 {args.n_components}, 임계 학습쪽 {cv.THRESHOLD_Q}분위, "
          f"지속 {cv.PERSIST}행(={cv.PERSIST * SEC_PER_ROW:.0f}초), "
          f"관측창 {args.window}행(={args.window * SEC_PER_ROW:.0f}초)")
    folds, trials = run(packs, k, args.mode, args.n_components,
                        tuple(args.onsets), DEFAULT_POSITIONS,
                        args.headline_only, verbose=not args.quiet,
                        window_rows=args.window)

    summ = summarize(folds, trials)
    folds.to_csv(OUT_DIR / f"eval_{args.mode}_folds.csv", index=False)
    trials.to_csv(OUT_DIR / f"eval_{args.mode}_trials.csv", index=False)
    (OUT_DIR / f"eval_{args.mode}_summary.json").write_text(
        json.dumps(summ, indent=2, ensure_ascii=False, default=float), encoding="utf-8")

    report(folds, trials, summ)
    print(f"\n  -> outputs/eval_{args.mode}_folds.csv")
    print(f"  -> outputs/eval_{args.mode}_trials.csv")
    print(f"  -> outputs/eval_{args.mode}_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
