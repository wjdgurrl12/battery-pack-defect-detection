"""STEP 8. 불량 유형 분류 — docs/battery_guide.md 구간 8 구현.

    용량불량        V1 큼 + V9 높음 + V4 큼
    용접불량        V5 큼 (모듈 통째) 또는 셀 결측/0
    센싱와이어불량   V1 큼 + V8 작음 + V9 낮음 + 연속 셀 2개 이상
    센서불량        T2 큼 + T3 벌어짐, 전압은 정상

V9 가 유형 분류의 핵심이다. 용량불량과 센싱와이어불량은 둘 다 V1 이 크게 나와
V1 만으로는 구분되지 않는다.

    구분          V1    V8            V9
    용량불량       큼    큼(혼자 튐)    높음
    센싱와이어     큼    작음(같이 감)  낮음

실행:
    python src/step8_classify.py
"""

# 판정은 학습 모델이 아니라 규칙 트리다. 이유:
#   - 불량 라벨 데이터가 없어 분류기를 학습시킬 수 없다.
#   - 유형별 물리적 특징이 명확해서 규칙으로 충분하다.
#   - 정비 담당자에게 "왜 이렇게 판정했나"를 evidence 딕셔너리로 그대로 보여줄 수 있다.
# 판정 순서(센서 -> 용접 -> 센싱와이어 -> 용량)는 배타성이 강한 것부터다.

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

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
N_MOD, N_PM = s3.N_MOD, s3.N_PM

# 판정 임계. train 33팩 중 12팩의 정상 분포(각 통계량 p90의 최대값)에 여유를 둬 잡았다.
#   정상 최대: V1 6.1 / V5 7.4 / V8 4.7 / V9 12.0 / T2 4.0 / T3 6.7 / 연속셀 1
# 즉 "정상에서 관측된 최대치보다 확실히 위"에 선을 그어 오분류를 막는다.
Z_V1_BIG = 8.0        # V1 큼
Z_V5_BIG = 9.0        # V5 큼 (모듈 통째)
Z_V8_BIG = 6.0        # V8 큼 = 이웃과 따로 논다
Z_T2_BIG = 5.0        # T2 큼
Z_T3_BIG = 8.0        # T3 벌어짐
V9_HIGH = 15.0        # V9 높음 (혼자 튐)
V9_LOW = 2.0          # V9 낮음 (이웃과 같이 감)
V4_BIG = 0.006        # V4 큼 (6 mV 이상 성장)
CELL_DEAD_V = 0.5     # 셀 결측/0 판정 전압
RUN_MODULE = 6        # 모듈 11셀 중 과반이 함께 튀면 '모듈 통째'
RUN_WIRE = 2          # 센싱와이어는 연속 셀 2개 이상


@dataclass
class Diagnosis:
    # 판정 결과 한 건. evidence 에 판정 근거 수치를 통째로 담아
    # 나중에 "왜 이 유형인가"를 재계산 없이 설명할 수 있게 한다.
    fault_type: str
    confidence: float
    cell: str = ""
    module: str = ""
    sensor: str = ""
    evidence: dict = field(default_factory=dict)

    def describe(self) -> str:
        loc = self.cell or self.module or self.sensor or "-"
        return f"{self.fault_type}({loc}, conf {self.confidence:.2f})"


def classify(c: dict, z: dict[str, np.ndarray], feats: dict[str, np.ndarray],
             window: slice | None = None) -> Diagnosis:
    """알람 구간(window)의 패턴으로 불량 유형을 판정한다."""
    # window 를 받는 이유: 전 구간 평균을 쓰면 고장이 뒤늦게 시작된 경우 신호가 묻힌다.
    # 운영에서는 STEP 7 이 띄운 알람 구간만 잘라 넘긴다.
    w = window or slice(None)
    zv1 = np.abs(z["V1"][w])
    zv5 = np.abs(z["V5"][w])
    zv8 = np.abs(z["V8"][w])
    zt2 = np.abs(z["T2"][w])
    zt3 = np.abs(z["T3"][w])

    # 시간 대표값은 p90. 평균은 SOC 에 따라 감도가 5배 변하는 구간을 뭉개 신호를 죽인다
    # (최대값을 쓰면 반대로 순간 스파이크 하나에 끌려간다. p90 이 그 사이 절충이다)
    rep = lambda a: np.percentile(a, 90, axis=0)
    v1_cell = rep(zv1)                               # 셀별 |z| p90
    i_cell = int(np.argmax(v1_cell))                 # 가장 의심스러운 셀
    m, j = divmod(i_cell, N_PM)                      # -> (모듈 m, 모듈 안 위치 j)
    v1_max = float(v1_cell[i_cell])

    v5_mod = rep(zv5)
    i_mod = int(np.argmax(v5_mod))
    v5_max = float(v5_mod[i_mod])

    t2_sensor = rep(zt2)
    i_sensor = int(np.argmax(t2_sensor))
    t2_max = float(t2_sensor[i_sensor])
    t3_max = float(rep(zt3).max())

    # V9 와 V4 는 z 가 아니라 원래 값으로 본다(둘 다 그 자체로 해석 가능한 지표라서)
    v9 = float(np.percentile(feats["V9"][w][:, i_cell], 90))
    v4 = feats["V4"][i_cell]
    v4_big = bool(np.isfinite(v4) and abs(v4) >= V4_BIG)

    # V8: 대상 셀이 낀 모듈 내부 쌍 (i-1,i), (i,i+1)
    #   혼자 튀면 양쪽 쌍이 모두 크고(용량불량), 이웃과 같이 가면 최소 한 쪽이 작다
    #   (센싱와이어). 2셀 고장은 바깥 경계 쌍이 크게 나오므로 최대값으로 보면 안 된다.
    #   -> 그래서 최소값(v8_min)이 판정의 핵심이고, 최대값은 참고로만 남긴다.
    pairs = [m * (N_PM - 1) + k for k in (j - 1, j) if 0 <= k <= N_PM - 2]
    v8_pairs = rep(zv8)[pairs] if pairs else np.zeros(1)
    v8_cell, v8_min = float(v8_pairs.max()), float(v8_pairs.min())

    # 같은 모듈에서 함께 튄 연속 셀 개수
    #   1개 = 용량불량, 2~3개 = 센싱와이어, 6개 이상 = 모듈 통째(용접)
    mod_hot = v1_cell.reshape(N_MOD, N_PM)[m] > Z_V1_BIG
    run = 0
    best_run = 0
    for hot in mod_hot:
        run = run + 1 if hot else 0
        best_run = max(best_run, run)

    # 셀 결측/0 — 전압이 0.5 V 밑이면 물리적으로 불가능한 값이라 결선/기록 이상이다
    raw = fi.raw_cells(c)[w]
    dead = np.flatnonzero((raw < CELL_DEAD_V).any(axis=0))

    # 판정 근거를 한 딕셔너리에 모아 결과와 함께 반환한다
    ev = {"z_V1_max": v1_max, "z_V5_max": v5_max, "z_V8_cell": v8_cell, "z_V8_min": v8_min,
          "V9": v9, "V4_mV": float(v4 * 1e3) if np.isfinite(v4) else float("nan"),
          "z_T2_max": t2_max, "z_T3_max": t3_max, "run_len": int(best_run),
          "n_dead_cells": int(dead.size)}

    volt_normal = v1_max < Z_V1_BIG and best_run < RUN_WIRE

    # 1) 센서불량 — 전압은 정상인데 온도만 튄다.
    #    주입 실험 결과 시작부터 있는 센서 고장은 T1 오프셋에 흡수돼 T2 가 놓치고(z 2.9)
    #    T3 가 잡는다(z 14.2). 가이드가 T3 를 따로 둔 이유가 그대로 확인된다.
    #    가장 먼저 보는 이유: '전압 정상'이라는 조건이 다른 유형과 완전히 배타적이라서.
    if (t2_max >= Z_T2_BIG or t3_max >= Z_T3_BIG) and volt_normal:
        # 신뢰도는 임계 대비 몇 배인지로 환산한다(임계의 2배면 1.0)
        conf = min(1.0, max(t2_max / Z_T2_BIG, t3_max / Z_T3_BIG) / 2)
        return Diagnosis("센서불량", conf, sensor=s1.TEMP_COLS[i_sensor], evidence=ev)

    # 2) 용접불량 — 모듈이 통째로 어긋나거나 셀이 죽었다.
    #    V5 의 절대 z 는 팩마다 산포가 커서(정상도 7.4까지) 단독 근거로 못 쓴다.
    #    모듈 11셀 중 과반이 함께 튀는지를 같이 본다.
    if dead.size or best_run >= RUN_MODULE or (v5_max >= Z_V5_BIG and v1_max >= Z_V1_BIG):
        conf = 1.0 if dead.size else min(1.0, max(best_run / N_PM, v5_max / (2 * Z_V5_BIG)))
        # 위치 표기: V5 로 잡혔으면 V5 가 지목한 모듈, 아니면 V1 이 지목한 셀의 모듈
        return Diagnosis("용접불량", conf, module=f"M{(i_mod if v5_max >= Z_V5_BIG else m) + 1:02d}",
                         cell=s1.CELL_COLS[int(dead[0])] if dead.size else "", evidence=ev)

    # 3) 센싱와이어불량 — V1 큼 + V8 작음 + V9 낮음 + 연속 셀 2개 이상
    #    와이어 하나가 두 셀의 측정에 걸쳐 있어 인접 셀이 같이 움직인다.
    #    "같이 움직인다" = 둘 사이 차(V8)가 작다는 뜻이므로 v8_min 으로 판별한다.
    if v1_max >= Z_V1_BIG and best_run >= RUN_WIRE and v8_min < Z_V8_BIG:
        return Diagnosis("센싱와이어불량", min(1.0, v1_max / (2 * Z_V1_BIG)),
                         cell=s1.CELL_COLS[i_cell], module=f"M{m + 1:02d}", evidence=ev)

    # 4) 용량불량 — V1 큼 + 혼자 튐(V8 양쪽 다 큼 또는 V9 높음) (+ V4 큼이면 확정)
    #    V4(저SOC->고SOC 편차 성장)는 용량 저하의 직접 증거라 신뢰도에 가산한다.
    if v1_max >= Z_V1_BIG and (v8_min >= Z_V8_BIG or v9 >= V9_HIGH):
        conf = min(1.0, v1_max / (2 * Z_V1_BIG) + (0.2 if v4_big else 0.0))
        return Diagnosis("용량불량", conf, cell=s1.CELL_COLS[i_cell],
                         module=f"M{m + 1:02d}", evidence=ev)

    # 어디에도 맞지 않으면 미분류로 남긴다 (억지 판정보다 낫다)
    #   뭔가 크긴 한데 패턴이 안 맞는 경우 -> 사람이 보게 넘긴다
    if v1_max >= Z_V1_BIG or t2_max >= Z_T2_BIG or t3_max >= Z_T3_BIG:
        return Diagnosis("미분류", 0.3, cell=s1.CELL_COLS[i_cell], evidence=ev)
    return Diagnosis("정상", 0.0, evidence=ev)


def diagnose_pack(c: dict, ref: s4.ReferenceTable, window: slice | None = None) -> Diagnosis:
    # 캐시 -> 피처 -> 정규화 -> 판정. 배치 경로에서 쓰는 진입점이다
    # (실시간 경로는 STEP 9 가 매초 계산한 z/feats 를 직접 classify 에 넘긴다)
    feats = s3.build_features(c)
    z = s5.normalize(feats, ref, c["soc"])
    return classify(c, z, feats, window)


# ── 검증 ─────────────────────────────────────────────────────────────────────
# 분류 로직 검증이 목적이므로 확실히 검출되는 크기를 쓴다. 최소 검출 한계는 validate.py 에서 잰다
CASES = [
    ("용량불량", dict(kind="capacity", magnitude=-0.020, cell=77)),
    ("용접불량", dict(kind="welding", magnitude=-0.015, module=5)),
    ("센싱와이어불량", dict(kind="sensing_wire", magnitude=-0.020, cell=77, n_cells=2)),
    ("센서불량", dict(kind="sensor", magnitude=2.0, sensor=9)),
]


def verify(mode: str = "chg") -> bool:
    man = json.loads((OUT_DIR / f"step1_{mode}_manifest.json").read_text(encoding="utf-8"))
    ref = s4.ReferenceTable.load(OUT_DIR / f"step4_{mode}_reference_train.csv")
    packs = man["holdout"]

    print("\n" + "=" * 78)
    print("STEP 8 검증 — 홀드아웃 팩에 유형별 고장 주입 후 판정")
    print("=" * 78)

    # 운영과 동일하게, STEP 7 이 띄운 알람 구간을 받아서 판정한다
    # (여기서 import 하는 이유: step7 이 step8 을 부르지 않게 해 순환 import 를 피한다)
    import step6_model as s6
    import step7_alarm as s7
    # 검출은 감도가 가장 좋은 룰 기반 연속 점수를 쓴다 (validate.py 비교 결과)
    model = s6.load(OUT_DIR / f"model_{mode}_op.pkl")
    cfg = s7.AlarmConfig(**json.loads(
        (OUT_DIR / f"step7_{mode}_alarm_config_rule.json").read_text(encoding="utf-8")))

    def alarm_window(c: dict) -> slice | None:
        # 점수 -> 알람 구간 -> 그중 최고점 구간을 판정 창으로 쓴다
        Z = s5.feature_matrix(s5.normalize(s3.build_features(c), ref, c["soc"]))
        evs = s7.find_alarms(s7.pack_score(model, Z, cfg, "rule"), cfg)
        if not evs:
            return None
        e = max(evs, key=lambda e: e.peak)
        return slice(e.start, e.end)

    # [1] 유형별 판정 정확도. '미검출'(알람 자체가 안 뜬 경우)은 분류 실패와 구분해 센다
    print(f"\n  [1] 유형 판정 정확도 ({len(packs)}팩 x {len(CASES)}유형, 알람 구간 기준)")
    print(f"      {'주입 유형':<14}{'정답':>6}{'미검출':>8}{'판정 결과':>26}")
    ok_all = True
    for want, kw in CASES:
        got, miss = [], 0
        for pid in packs:
            c = s3.load_cache(pid, mode)
            bad = fi.inject(c, **kw)
            bad["pack_id"] = pid
            w = alarm_window(bad)
            if w is None:
                miss += 1
                continue
            got.append(diagnose_pack(bad, ref, w).fault_type)
        hit = sum(g == want for g in got)
        ok = bool(got) and hit == len(got)     # 검출된 건은 전부 맞아야 통과
        ok_all &= ok
        uniq = ", ".join(sorted(set(got))) or "-"
        print(f"      {want:<14}{hit}/{len(got):<4}{miss:>8}{uniq:>26}  {'PASS' if ok else 'FAIL'}")

    # [2] 무주입 정상 팩은 '정상'(또는 최소한 특정 유형으로 단정하지 않는 '미분류')이어야 한다
    normal = [diagnose_pack(s3.load_cache(pid, mode), ref).fault_type for pid in packs]
    ok_norm = all(t in ("정상", "미분류") for t in normal)
    print(f"\n  [2] 무주입 정상 팩 판정: {', '.join(sorted(set(normal)))}  "
          f"-> {'PASS' if ok_norm else 'FAIL'}")

    # [3] V9 판별표 재현 — 같은 크기·같은 셀인데 '단독 vs 연속 2셀'만 다르게 주입해
    #     V8·V9 가 실제로 갈리는지 확인한다. 유형 분류의 근거가 되는 표다.
    print("\n  [3] V9 판별표 (가이드: 용량불량 V8 큼·V9 높음 / 센싱와이어 V8 작음·V9 낮음)")
    print(f"      {'구분':<14}{'|z| V1':>9}{'|z| V8':>9}{'V9':>8}")
    rows = {}
    pid = packs[0]
    c = s3.load_cache(pid, mode)
    for name, kw in (("용량불량", dict(kind="capacity", magnitude=-0.020, cell=77)),
                     ("센싱와이어", dict(kind="sensing_wire", magnitude=-0.020, cell=77, n_cells=2))):
        bad = fi.inject(c, **kw)
        d = diagnose_pack(bad, ref)
        rows[name] = d.evidence
        print(f"      {name:<14}{d.evidence['z_V1_max']:>9.1f}{d.evidence['z_V8_cell']:>9.1f}"
              f"{d.evidence['V9']:>8.2f}")
    ok_v9 = (rows["용량불량"]["V9"] > rows["센싱와이어"]["V9"]
             and rows["용량불량"]["z_V8_cell"] > rows["센싱와이어"]["z_V8_cell"])
    print(f"      용량불량이 V8·V9 둘 다 크다  -> {'PASS' if ok_v9 else 'FAIL'}")
    print("=" * 78)
    return ok_all and ok_norm and ok_v9


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 8. 불량 유형 분류")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    args = ap.parse_args()
    print("STEP 8 불량 유형 분류")
    return 0 if verify(args.mode) else 1


if __name__ == "__main__":
    raise SystemExit(main())
