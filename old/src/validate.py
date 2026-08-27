"""검증 계획 — docs/battery_guide.md '검증 계획' 절 구현.

불량 테스트 데이터(Test0X_NG_*.csv)와 라벨이 없으므로 가이드가 제시한 대체 검증을 한다.

    오탐률   홀드아웃 정상 팩에서 알람 발생 빈도
    검출력   정상 팩에 인위적 고장을 주입하고 크기를 낮춰가며 최소 검출 한계 측정
    유형     주입한 고장을 STEP 8 이 맞게 분류하는지

가이드 기준값 F-score 0.7956 은 NG 라벨이 있어야 계산 가능하므로 여기서는 대조 불가.

실행:
    python src/validate.py
"""

# 이 파일이 최종 성적표다. 세 점수 방식(가이드 스펙 / 운영 PCA / 룰)에 대해
# 같은 측정을 반복해서, "어느 방식을 운영에 쓸 것인가"를 숫자로 고를 수 있게 한다.
# 오탐률과 검출력은 트레이드오프라 반드시 같이 봐야 한다:
#   임계값을 낮추면 검출력은 오르지만 오탐이 늘고, 높이면 반대가 된다.

from __future__ import annotations

import argparse
import json
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
import step7_alarm as s7
import step8_classify as s8

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR

# 검출력 스윕 (전압 mV / 온도 °C)
# 0.0 을 포함하는 이유: 주입하지 않았을 때의 검출률 = 오탐률이라, 같은 표에서 대조된다
V_LEVELS = [0.0, -2e-3, -4e-3, -6e-3, -8e-3, -12e-3, -20e-3]
T_LEVELS = [0.0, 0.3, 0.5, 1.0, 2.0, 3.0]
CASES = [
    ("용량불량", "capacity", V_LEVELS, dict(cell=77)),
    ("용접불량", "welding", V_LEVELS, dict(module=5)),
    ("센싱와이어불량", "sensing_wire", V_LEVELS, dict(cell=77, n_cells=2)),
    ("센서불량", "sensor", T_LEVELS, dict(sensor=9)),
]


def score_pack(model, ref, c: dict, cfg, key: str = "score") -> np.ndarray:
    # 캐시(정상 또는 주입본) -> 시점별 점수. 초기 60초 보류까지 반영된다
    Z = s5.feature_matrix(s5.normalize(s3.build_features(c), ref, c["soc"]))
    return s7.pack_score(model, Z, cfg, key)


def false_alarm(model, ref, cfg, packs: list[int], mode: str, key: str = "score") -> pd.DataFrame:
    # 오탐률: 아무 고장도 없는 정상 팩에서 알람이 몇 번 뜨는가.
    # 건수뿐 아니라 '시간당 건수'와 '알람 상태로 보낸 시간 비율'을 함께 낸다.
    # 운영에서 중요한 건 절대 건수가 아니라 "얼마나 자주 사람을 부르는가"라서다.
    rows = []
    for pid in packs:
        c = s3.load_cache(pid, mode)
        sc = score_pack(model, ref, c, cfg, key)
        evs = s7.find_alarms(sc, cfg)
        hours = len(sc) / 3600          # 1행 = 1초
        rows.append({"pack_id": pid, "n_sec": len(sc), "n_alarm": len(evs),
                     "alarm_sec": sum(e.duration for e in evs),
                     "alarms_per_hour": len(evs) / hours,
                     "alarm_time_pct": 100 * sum(e.duration for e in evs) / len(sc)})
    return pd.DataFrame(rows)


def detection_sweep(model, ref, cfg, packs: list[int], mode: str,
                    key: str = "score") -> pd.DataFrame:
    # 검출력: 고장 크기를 낮춰가며 (검출률, 유형 분류 정확도)를 잰다.
    # 유형별 x 크기별 x 팩별 3중 루프라 가장 오래 걸리는 부분이다.
    rows = []
    for label, kind, levels, kw in CASES:
        for mag in levels:
            det, cls = 0, 0
            for pid in packs:
                c = s3.load_cache(pid, mode)
                bad = fi.inject(c, kind, mag, **kw)
                sc = score_pack(model, ref, bad, cfg, key)
                evs = s7.find_alarms(sc, cfg)
                if evs:
                    det += 1
                    # 검출된 경우에만 유형 분류를 채점한다(첫 알람 구간 기준)
                    d = s8.diagnose_pack(bad, ref, slice(evs[0].start, evs[0].end))
                    cls += int(d.fault_type == label)
            rows.append({"fault": label, "magnitude": mag, "unit": "V" if kind != "sensor" else "°C",
                         "n_packs": len(packs), "detected": det, "classified": cls,
                         "detect_rate": det / len(packs), "class_rate": cls / max(det, 1)})
    return pd.DataFrame(rows)


def rule_only_detection(ref, packs: list[int], mode: str) -> pd.DataFrame:
    """STEP 6-1 룰만으로 얻는 하한선 (모델 없이)."""
    # PCA/IF 를 다 걷어내고 룰 4개만 남겼을 때의 성능. 모델이 이보다 못하면
    # 복잡한 모델을 쓸 이유가 없다는 판단 기준선이 된다.
    rows = []
    for label, kind, levels, kw in CASES:
        for mag in levels:
            det = 0
            for pid in packs:
                c = s3.load_cache(pid, mode)
                bad = fi.inject(c, kind, mag, **kw)
                z = s5.normalize(s3.build_features(bad), ref, bad["soc"])
                flags = s6.rule_flags(z)
                fired = np.logical_or.reduce(list(flags.values()))   # 룰 중 하나라도 발화
                # 10초 지속 조건을 룰에도 동일 적용
                pad = np.concatenate(([False], fired, [False]))
                e = np.flatnonzero(pad[1:] != pad[:-1])
                det += int(any(b - a >= s7.PERSIST_SEC for a, b in zip(e[0::2], e[1::2])))
            rows.append({"fault": label, "magnitude": mag,
                         "detect_rate": det / len(packs)})
    return pd.DataFrame(rows)


def limit_table(sweep: pd.DataFrame) -> dict[str, float]:
    """유형별 최소 검출 크기 (검출률 100% 를 만족하는 가장 작은 크기)."""
    # 100% 를 기준으로 잡는 이유: 안전 관련 진단에서 "가끔 놓친다"는 보장이 되지 않는다.
    # 어떤 크기에서도 100% 가 안 나오면 NaN 으로 남겨 '한계 미확인'을 드러낸다.
    out = {}
    for label, g in sweep.groupby("fault"):
        ok = g[g["detect_rate"] >= 1.0]
        out[label] = float(ok["magnitude"].abs().min()) if len(ok) else float("nan")
    return out


def report(model_label: str, model, ref, cfg, man: dict, mode: str,
           key: str = "score") -> dict:
    # 한 점수 방식에 대한 전체 성적표를 찍고, 같은 내용을 dict 로도 돌려준다
    # (화면 출력은 사람용, 반환값은 validation_<mode>.json 용)
    print(f"\n{'#' * 78}\n# 검증 — {model_label} (주성분 {model.n_components}, "
          f"임계 {cfg.threshold:.2f})\n{'#' * 78}")

    # [1] 오탐률 — 홀드아웃(임계값을 잡은 그 팩들)에서 먼저 본다
    fa = false_alarm(model, ref, cfg, man["holdout"], mode, key)
    print("\n  [1] 오탐률 — 홀드아웃 정상 팩")
    print(f"      {'팩':>6}{'구간(초)':>10}{'알람 건수':>10}{'알람 시간%':>12}{'건/시간':>10}")
    for r in fa.itertuples():
        print(f"      {r.pack_id:>6}{r.n_sec:>10}{r.n_alarm:>10}"
              f"{r.alarm_time_pct:>11.2f}%{r.alarms_per_hour:>10.2f}")
    print(f"      합계: {int(fa['n_alarm'].sum())}건 / {fa['n_sec'].sum() / 3600:.1f}시간 "
          f"= {fa['n_alarm'].sum() / (fa['n_sec'].sum() / 3600):.2f} 건/시간")

    # train 팩도 참고로 잰다. 임계값을 홀드아웃으로 잡았으므로 train 쪽이 더 낮게 나온다
    fa_tr = false_alarm(model, ref, cfg, man["train"], mode, key)
    print(f"      (참고) train {len(man['train'])}팩: {int(fa_tr['n_alarm'].sum())}건 / "
          f"{fa_tr['n_sec'].sum() / 3600:.1f}시간 = "
          f"{fa_tr['n_alarm'].sum() / (fa_tr['n_sec'].sum() / 3600):.2f} 건/시간")

    # [2] 검출력 — 크기를 낮춰가며 검출률이 어디서 무너지는지 본다
    sweep = detection_sweep(model, ref, cfg, man["holdout"], mode, key)
    print("\n  [2] 검출력 — 홀드아웃 팩에 고장 주입 (검출률 / 유형 분류 정확도)")
    # 헤더는 전압 기준으로 찍고, 유형별 실제 주입 크기는 각 행 아래 다시 적는다
    # (센서불량만 단위가 °C 라 열이 어긋나기 때문)
    print(f"      {'유형':<14}" + "".join(f"{abs(m) * (1e3 if abs(m) < 1 else 1):>8.1f}"
                                         for m in V_LEVELS) + "   (mV, 온도는 °C)")
    for label, g in sweep.groupby("fault", sort=False):
        cells = "".join(f"{r.detect_rate * 100:>7.0f}%" for r in g.itertuples())
        units = "".join(f"{abs(r.magnitude) * (1e3 if abs(r.magnitude) < 1 else 1):>8.1f}"
                        for r in g.itertuples())
        print(f"      {label:<14}{cells}")
        print(f"      {'  (주입 크기)':<14}{units}")
    # 검출된 건 중 유형까지 맞힌 비율. 검출과 분류를 분리해서 봐야 어디가 약한지 안다
    print("\n      유형 분류 정확도 (검출된 건 기준)")
    for label, g in sweep.groupby("fault", sort=False):
        det = g[g["detected"] > 0]
        rate = det["classified"].sum() / max(det["detected"].sum(), 1)
        print(f"      {label:<14}{rate * 100:>6.0f}%  ({int(det['classified'].sum())}/"
              f"{int(det['detected'].sum())})")

    # [3] 최소 검출 한계 — 현장에 "몇 mV 부터 잡힌다"고 말할 수 있는 수치
    lim = limit_table(sweep)
    print("\n  [3] 최소 검출 한계 (검출률 100% 기준)")
    for k, v in lim.items():
        unit = "°C" if k == "센서불량" else "mV"
        val = v if k == "센서불량" else v * 1e3
        print(f"      {k:<14}{val:>8.1f} {unit}")
    return {"false_alarm": fa.to_dict("records"), "sweep": sweep.to_dict("records"),
            "limits": lim, "threshold": cfg.threshold}


def main() -> int:
    ap = argparse.ArgumentParser(description="검증 계획 실행")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    ref = s4.ReferenceTable.load(OUT_DIR / f"step4_{args.mode}_reference_train.csv")

    print("검증 계획 — NG 라벨이 없어 오탐률·검출력으로 대체")
    out = {}
    # STEP 7 에서 만든 세 임계값 파일과 짝을 맞춰 같은 측정을 세 번 반복한다
    variants = [("", "score", "가이드 스펙 PCA(0.99) + 통합 점수"),
                ("_op", "score", "운영 PCA(교차검증) + 통합 점수"),
                ("_rule", "rule", "룰 기반 연속 점수 (STEP 6-1)")]
    for suffix, key, label in variants:
        model = s6.load(OUT_DIR / f"model_{args.mode}{'_op' if suffix else ''}.pkl")
        cfg = s7.AlarmConfig(**json.loads(
            (OUT_DIR / f"step7_{args.mode}_alarm_config{suffix}.json").read_text(encoding="utf-8")))
        out[label] = report(label, model, ref, cfg, man, args.mode, key)

    # 모델을 전혀 쓰지 않은 룰 단독 성능. 위 세 결과를 판단하는 기준선이다
    print(f"\n{'#' * 78}\n# 룰만 사용한 하한선 (STEP 6-1, 모델 없이)\n{'#' * 78}")
    rules = rule_only_detection(ref, man["holdout"], args.mode)
    for lbl, g in rules.groupby("fault", sort=False):
        print(f"      {lbl:<14}" + "".join(f"{r.detect_rate * 100:>7.0f}%" for r in g.itertuples()))
    out["rules_only"] = rules.to_dict("records")

    # 가이드의 F-score 는 정답 라벨이 있어야 계산되는 지표라 여기서는 낼 수 없다.
    # 무엇이 없어서 못 하는지 파일명까지 명시해 둔다.
    print("\n  [4] 가이드 기준값 대조")
    print("      F-score 0.7956 : NG 라벨 파일(Test0X_NG_*_Label.csv)이 없어 계산 불가")
    print("      필요 파일: Test05_NG_chg.csv, Test07_NG_dchg.csv, Test08_NG_chg.csv + 라벨")

    # ensure_ascii=False: 한글 라벨을 그대로 저장. default=float: numpy 타입 직렬화 대응
    (OUT_DIR / f"validation_{args.mode}.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False, default=float), encoding="utf-8")
    print(f"\n  -> outputs/validation_{args.mode}.json")
    # 검증 스크립트는 측정이 목적이라 기대값 대조가 없다. 항상 0 을 반환한다
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
