"""STEP 9. 실시간 추론 — docs/battery_guide.md 구간 9 구현.

매초 들어오는 BMS 1행(176셀 + 32온도 + RSOCavg)을 그대로 처리한다.

    1. 수신 -> 2. 계층 분해 -> 3. RSOCavg 로 기준표 조회(선형보간) -> 4. robust z
    -> 5. PCA/IF 점수 -> 6. 임계 비교, 10초 연속 시 알람 -> 7. SPE 기여도로 원인 특정
    -> 8. 유형 판정

SOC 는 기준 조회에만 쓰므로 리샘플링·재정렬이 없고 지연이 붙지 않는다.
V2(기울기)는 최근 60초 링버퍼, T1(오프셋)은 초기 60초 누적으로 처리한다.

실행:
    python src/step9_realtime.py
"""

# 배치 경로(STEP 3~8)와 실시간 경로의 차이는 딱 세 군데다.
#   V2 : 배치는 전후 60초 중심차분, 실시간은 과거 60초만 쓰는 후방차분
#   T1 : 배치는 초기 60초를 한 번에 평균, 실시간은 매초 누적 평균으로 갱신
#   V4 : 배치는 SOC 30/85% 근방 중앙값, 실시간은 그 SOC 를 지나갈 때 스냅샷
# 나머지(분해·기준표 조회·정규화·점수·판정)는 배치와 완전히 같은 함수를 호출한다.
# verify() 가 배치 점수와 스트리밍 점수의 상관으로 그 동등성을 확인한다.

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

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
N_MOD, N_PM, N_CELLS = s3.N_MOD, s3.N_PM, s3.N_CELLS


@dataclass
class Reading:
    """매초 수신되는 1행."""
    # BMS 가 보내주는 최소 단위. 이 셋만 있으면 판정이 가능하다는 뜻이기도 하다
    cells: np.ndarray      # (176,) V
    temps: np.ndarray      # (32,) °C
    soc: float             # RSOCavg %


@dataclass
class Verdict:
    # 1초 처리 결과. cause/fault_type 은 알람이 뜬 시점에만 채운다
    t: int
    score: float
    z_max: float
    alarm: bool
    cause: str = ""
    fault_type: str = ""
    warmup: bool = False
    detail: dict = field(default_factory=dict)


class RealtimeDetector:
    """상태를 들고 매초 1행씩 판정한다."""

    def __init__(self, ref: s4.ReferenceTable, model: s6.Model,
                 cfg: s7.AlarmConfig, slope_half: int = s3.SLOPE_HALF,
                 score_key: str = "score"):
        self.ref, self.model, self.cfg = ref, model, cfg
        self.slope_half, self.score_key = slope_half, score_key
        self.reset()

    def reset(self) -> None:
        # 여기 있는 것이 이 검출기가 들고 있어야 하는 상태 전부다.
        # 메모리는 O(1)(링버퍼 61행 + 스칼라 몇 개)이라 임베디드로 옮기기 쉽다.
        self.t = 0
        self.v1_buf: deque[np.ndarray] = deque(maxlen=2 * self.slope_half + 1)
        self.soc_buf: deque[float] = deque(maxlen=2 * self.slope_half + 1)
        self.warm_res: list[np.ndarray] = []      # T1 추정용 초기 잔차
        self.t_offset = np.zeros(32)
        self.run_len = 0                           # 연속 초과 초
        self.v1_at_soc: dict[int, np.ndarray] = {}  # V4 용 스냅샷

    # ── 1행 처리 ────────────────────────────────────────────────────────
    def step(self, r: Reading) -> Verdict:
        warm = self.t < self.cfg.warmup_sec        # 초기 60초인가

        # 2. 계층 분해 — STEP 2 와 같은 식을 1행(T=1)에 적용한다
        grid = r.cells.reshape(1, N_MOD, N_PM)
        v_mod = np.median(grid, axis=2)                       # (1,16)
        v_pack = np.median(v_mod, axis=1)                     # (1,)
        cell_res = r.cells[None, :] - np.repeat(v_mod, N_PM, axis=1)
        mod_dev = v_mod - v_pack[:, None]
        v1 = cell_res + np.repeat(mod_dev, N_PM, axis=1)

        # 온도 오프셋 (T1): 초기 60초 누적으로 추정
        #   매초 평균을 다시 계산해 갱신하고, 60초가 지나면 값이 고정된다
        t_res = r.temps - np.median(r.temps)
        if warm:
            self.warm_res.append(t_res)
            self.t_offset = np.mean(self.warm_res, axis=0)

        # V2: 링버퍼 양끝으로 SOC 기울기
        #   배치는 (t-30, t+30)을 보지만 실시간은 미래를 못 보므로 (t-60, t) 를 쓴다.
        #   그래서 배치와 완전히 같은 값이 나오지 않는다(verify 는 상관으로만 확인).
        self.v1_buf.append(v1[0])
        self.soc_buf.append(r.soc)
        if len(self.soc_buf) >= 2:
            d = self.soc_buf[-1] - self.soc_buf[0]
            slope = ((self.v1_buf[-1] - self.v1_buf[0]) / d
                     if abs(d) >= s3.SLOPE_MIN_DSOC else np.zeros(N_CELLS))
        else:
            slope = np.zeros(N_CELLS)              # 버퍼가 1행뿐이면 기울기 없음

        # V4: SOC 30% / 85% 통과 시점 스냅샷
        #   그 SOC 를 지날 때 V1 을 찍어 두고 둘 다 모이면 차이를 낸다.
        #   85% 를 지나기 전에는 NaN 이라 V4 기반 판정이 자동으로 보류된다.
        for key in (30, 85):
            if key not in self.v1_at_soc and abs(r.soc - key) <= s3.GROWTH_TOL:
                self.v1_at_soc[key] = v1[0].copy()
        v4 = (self.v1_at_soc[85] - self.v1_at_soc[30]
              if 30 in self.v1_at_soc and 85 in self.v1_at_soc else np.full(N_CELLS, np.nan))

        # STEP 3 함수들이 기대하는 캐시 형태(시간축이 있는 2차원)로 맞춰준다.
        # T=1 이라 그대로 재사용할 수 있고, 덕분에 피처 정의가 배치와 100% 같아진다.
        c = {"cell_res": cell_res, "mod_dev": mod_dev, "v_pack": v_pack,
             "temp": r.temps[None, :], "soc": np.array([r.soc])}
        feats = {
            "V1": v1, "V2": slope[None, :], "V4": v4,
            "V5": mod_dev, "V6": s3.v6_mod_spread(c), "V8": s3.v8_adj_diff(c),
            "V9": s3.v9_isolation(c),
            "T2": (t_res - self.t_offset)[None, :],
            "T3": s3.t3_pair(r.temps[None, :]), "T5": s3.t5_mod_dev(r.temps[None, :]),
        }

        # 3~4. 기준표 조회 + robust z (배치와 동일 함수)
        soc = np.array([r.soc])
        z = s5.normalize(feats, self.ref, soc)
        Z = s5.feature_matrix(z)

        # 5. 점수 (초기 60초는 T2 판정 보류 -> 온도 열 0)
        if warm:
            Z = Z.copy()
            for f in ("T2", "T3", "T5"):
                Z[:, s5.COL_SLICE[f]] = 0.0
        sc = self.model.score(Z)
        score = float(sc[self.score_key][0])

        # 6. 지속시간 조건 — 배치의 find_alarms 를 상태 하나(run_len)로 대체한다
        self.run_len = self.run_len + 1 if score > self.cfg.threshold else 0
        alarm = self.run_len >= self.cfg.persist_sec

        v = Verdict(self.t, score, float(sc["z_max"][0]), alarm, warmup=warm)
        if alarm:
            # 7. SPE 기여도로 원인 특정 (알람일 때만 계산해 평시 비용을 낮춘다)
            top = s6.top_contributors(self.model, Z, k=3)
            v.cause = top[0][0]
            # 8. 유형 판정 — 이 시점 1행만으로 판정한다(window 없음)
            d = s8.classify(c, z, feats)
            v.fault_type = d.fault_type
            v.detail = {"top": top, "diag": d.describe()}
        self.t += 1
        return v


def replay(pack_id: int, ref, model, cfg, mode: str = "chg",
           inject: dict | None = None, score_key: str = "score") -> tuple[list[Verdict], float]:
    """캐시된 팩을 매초 스트림처럼 재생한다. 반환: (판정 목록, 1행당 평균 ms)."""
    # 캐시에는 원본 셀 전압이 없으므로 raw_cells 로 되살려 '수신 데이터'를 만든다.
    # 실제 BMS 연동 시 이 함수만 소켓 수신 루프로 바꾸면 된다.
    c = s3.load_cache(pack_id, mode)
    if inject:
        c = fi.inject(c, **inject)
    cells = fi.raw_cells(c)
    det = RealtimeDetector(ref, model, cfg, score_key=score_key)
    out = []
    t0 = time.perf_counter()
    for i in range(len(c["soc"])):
        out.append(det.step(Reading(cells[i], c["temp"][i], float(c["soc"][i]))))
    ms = (time.perf_counter() - t0) * 1e3 / len(out)     # 1행당 평균 처리 시간
    return out, ms


# ── 검증 ─────────────────────────────────────────────────────────────────────
def verify(ref, model, cfg, man: dict, mode: str, key: str = "score") -> bool:
    print("\n" + "=" * 78)
    print("STEP 9 검증 — 매초 스트리밍 추론")
    print("=" * 78)

    # SOC 구간을 가장 넓게 도는 홀드아웃 팩으로 시연한다.
    # 고SOC만 도는 팩(예: 1004는 60.9%부터 시작)은 기준표 σ 가 커서 감도가 가장 낮다.
    import pandas as pd
    tbl = pd.read_csv(OUT_DIR / f"step1_{mode}_summary.csv").set_index("pack_id")
    pid = min(man["holdout"], key=lambda p: tbl.loc[p, "soc_min"])

    # 1) 배치 결과와 일치하는가 — 실시간 구현이 배치와 다른 답을 내면 안 된다
    Zb, soc = s5.pack_matrix(pid, ref, mode)
    batch = s7.pack_score(model, Zb, cfg, key)
    stream, ms = replay(pid, ref, model, cfg, mode, score_key=key)
    s_stream = np.array([v.score for v in stream])
    n = min(len(batch), len(s_stream))
    # V2 는 스트리밍에서 과거만 쓰므로(배치는 전후 60초) 완전 일치가 아니다
    # -> 등가성 판정을 절대차가 아니라 상관계수로 한다
    corr = float(np.corrcoef(batch[:n], s_stream[:n])[0, 1])
    diff = float(np.abs(batch[:n] - s_stream[:n]).mean())
    ok_match = corr > 0.9
    print(f"\n  [1] 배치 대비 스트리밍 점수 (팩 {pid}, {n}초)")
    print(f"      상관 {corr:.4f}, 평균 절대차 {diff:.3f}  -> {'PASS' if ok_match else 'FAIL'}")
    print(f"      (V2 기울기는 배치가 전후 60초, 스트리밍은 과거 60초만 쓰므로 완전 동일하지 않다)")

    # 2) 실시간성 — 데이터가 1초에 한 행 오므로 예산은 1000 ms 다
    ok_rt = ms < 1000.0
    print(f"\n  [2] 1초당 처리 시간 {ms:.2f} ms (1000 ms 예산)  -> {'PASS' if ok_rt else 'FAIL'}")
    print(f"      여유 {1000 / ms:.0f}배")

    # 3) 초기 60초 T2 보류 — 보류 표시가 정확히 60개 시점에만 붙는지
    warm = [v for v in stream if v.warmup]
    ok_warm = len(warm) == cfg.warmup_sec
    print(f"\n  [3] 초기 {cfg.warmup_sec}초 보류 구간 {len(warm)}초  "
          f"-> {'PASS' if ok_warm else 'FAIL'}")

    # 4) 고장 주입 시 원인·유형 출력 — 검출 -> 원인 -> 유형까지 전 경로를 한 번에 확인
    print(f"\n  [4] 고장 주입 재생 (팩 {pid})")
    # 검출 한계 위쪽 크기로 스트리밍 전 경로(검출 -> 원인 -> 유형)를 확인한다.
    # 한계 자체는 validate.py 가 크기를 낮춰가며 잰다.
    cases = [("용량불량", dict(kind="capacity", magnitude=-0.020, cell=77)),
             ("센서불량", dict(kind="sensor", magnitude=2.0, sensor=9))]
    ok_detect = True
    for want, kw in cases:
        vs, _ = replay(pid, ref, model, cfg, mode, inject=kw, score_key=key)
        al = [v for v in vs if v.alarm]
        first = al[0] if al else None
        # 알람이 뜨는 것만으로는 부족하고, 유형까지 맞아야 통과다
        hit = bool(al) and any(v.fault_type == want for v in al)
        ok_detect &= hit
        if first:
            share = first.detail["top"][0][1] if first.detail else 0
            print(f"      {want:<8} 알람 {len(al):>5}초, 최초 t={first.t}s, "
                  f"원인 {first.cause} ({share * 100:.0f}%), 유형 {first.fault_type}"
                  f"  {'PASS' if hit else 'FAIL'}")
        else:
            print(f"      {want:<8} 알람 없음  FAIL")
    print("=" * 78)
    return ok_match and ok_rt and ok_warm and ok_detect


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 9. 실시간 추론")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    # 기본이 rule 인 이유: validate.py 비교에서 룰 기반 연속 점수의 검출 감도가 가장 높았다
    ap.add_argument("--model", default="rule", choices=["op", "guide", "rule"])
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    ref = s4.ReferenceTable.load(OUT_DIR / f"step4_{args.mode}_reference_train.csv")
    suffix = {"op": "_op", "guide": "", "rule": "_rule"}[args.model]   # 임계값 파일 접미사
    key = "rule" if args.model == "rule" else "score"                  # 쓸 점수 종류
    # guide 만 가이드 스펙 모델(model_chg.pkl), 나머지는 운영 모델(_op)을 쓴다
    model = s6.load(OUT_DIR / f"model_{args.mode}{'' if args.model == 'guide' else '_op'}.pkl")
    cfg_d = json.loads((OUT_DIR / f"step7_{args.mode}_alarm_config{suffix}.json")
                       .read_text(encoding="utf-8"))
    cfg = s7.AlarmConfig(**cfg_d)

    print(f"STEP 9 실시간 추론 — 점수 {args.model}, 임계 {cfg.threshold:.2f}")
    ok = verify(ref, model, cfg, man, args.mode, key)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
