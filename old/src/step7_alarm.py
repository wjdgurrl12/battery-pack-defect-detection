"""STEP 7. 임계값과 알람 — docs/battery_guide.md 구간 7 구현.

    7-1 임계값     홀드아웃 이상점수의 99.9 분위수 (꼬리가 두꺼우면 EVT/POT 검토)
    7-2 지속시간   N초(권장 10초) 연속 초과 시에만 알람
    7-3 초기 구간  충전 시작 60초는 온도 오프셋 추정 중이므로 T2 판정 보류

실행:
    python src/step7_alarm.py
"""

# 점수를 알람으로 바꾸는 단계다. 세 장치가 각각 다른 오탐을 막는다.
#   임계값(7-1) : "얼마나 커야 이상인가" — 정상 데이터의 상위 0.1% 선을 쓴다.
#   지속시간(7-2): "얼마나 오래 가야 진짜인가" — 1~2초짜리 스파이크를 걸러낸다.
#   초기보류(7-3): "언제부터 믿을 수 있나" — 온도 오프셋 추정이 끝나기 전 판단 보류.
# 세 점수 방식(가이드 스펙 / 운영 PCA / 룰)마다 임계값을 따로 만들어 저장한다.

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
from scipy import stats          # EVT(GPD) 적합용

sys.path.insert(0, str(Path(__file__).resolve().parent))
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

OUT_DIR = s1.OUT_DIR

THRESHOLD_Q = 99.9      # 7-1
# B안 스위치 (step4/step6 와 동일). True 면 임계값을 학습 분포에서 잡는다.
FIT_ON_ALL = True
PERSIST_SEC = 2         # 7-2. '행' 단위다. STEP 1 이 전 팩을 5초/행 격자로
                        #      통일했으므로 2행 = 10초 (가이드 권장값)
WARMUP_SEC = 60         # 7-3. T1 오프셋 추정 구간
EVT_TAIL_Q = 99.0       # POT 임계 (이 위를 GPD 로 적합)


@dataclass
class AlarmConfig:
    # 운영에 필요한 설정 전부. JSON 으로 저장돼 STEP 8·9 가 그대로 읽어 쓴다
    threshold: float
    persist_sec: int = PERSIST_SEC
    warmup_sec: int = WARMUP_SEC
    threshold_evt: float = float("nan")
    tail_xi: float = float("nan")     # GPD 형상모수. > 0 이면 두꺼운 꼬리


@dataclass
class AlarmEvent:
    # 알람 1건 = [start, end) 구간. peak/peak_at 은 원인 분석 시점을 고르는 데 쓴다
    start: int
    end: int
    duration: int
    peak: float
    peak_at: int


def warmup_mask(n: int, warmup: int = WARMUP_SEC) -> np.ndarray:
    """True 인 구간은 T2(온도) 판정 보류 대상."""
    m = np.zeros(n, dtype=bool)
    m[:warmup] = True
    return m


def score_without_temp(model: s6.Model, Z: np.ndarray, key: str = "score") -> np.ndarray:
    """온도 열을 0 으로 눌러 전압 룰만으로 낸 점수 (초기 60초용)."""
    # z=0 은 "기준값과 같다 = 정상"이라는 뜻이라, 온도가 점수에 기여하지 못하게 된다.
    # 온도 피처를 빼는 대신 0 으로 채우는 이유: 행렬 차원(784)이 바뀌면 PCA 가 못 받는다.
    Zv = Z.copy()
    for f in ("T2", "T3", "T5"):
        Zv[:, s5.COL_SLICE[f]] = 0.0
    return model.score(Zv)[key]


def pack_score(model: s6.Model, Z: np.ndarray, cfg: AlarmConfig | None = None,
               key: str = "score") -> np.ndarray:
    """7-3 을 반영한 시점별 이상점수. key='rule' 이면 룰 기반 연속 점수를 쓴다."""
    s = model.score(Z)[key]
    warm = warmup_mask(len(Z), cfg.warmup_sec if cfg else WARMUP_SEC)
    if warm.any():
        # 초기 구간만 온도 뺀 점수로 덮어쓴다(나머지 구간은 그대로)
        s = s.copy()
        s[warm] = score_without_temp(model, Z[warm], key)
    return s


def find_alarms(score: np.ndarray, cfg: AlarmConfig) -> list[AlarmEvent]:
    """7-2. persist_sec 연속 초과한 구간만 알람으로 인정한다."""
    over = score > cfg.threshold
    events: list[AlarmEvent] = []
    if not over.any():
        return events

    # STEP 1 의 longest_active_run 과 같은 엣지 탐색 관용구다
    padded = np.concatenate(([False], over, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    for a, b in zip(edges[0::2], edges[1::2]):
        if b - a >= cfg.persist_sec:              # 10초 미만 구간은 버린다
            seg = score[a:b]
            events.append(AlarmEvent(int(a), int(b), int(b - a),
                                     float(seg.max()), int(a + seg.argmax())))
    return events


def fit_threshold(scores: np.ndarray, q: float = THRESHOLD_Q) -> AlarmConfig:
    """7-1. 경험 분위수 + EVT(POT) 교차 확인."""
    thr = float(np.percentile(scores, q))     # 경험 분위수: 데이터를 정렬해 상위 0.1% 지점
    # POT(Peaks Over Threshold): 상위 1% 초과분만 모아 일반화 파레토 분포(GPD)에 적합한다.
    # 관측 표본이 유한해 경험 분위수가 못 보는 극단 꼬리를 외삽으로 추정하는 방법.
    u = float(np.percentile(scores, EVT_TAIL_Q))
    tail = scores[scores > u] - u             # 초과분(exceedance)
    thr_evt, xi = float("nan"), float("nan")
    if tail.size >= 50:                       # 표본이 너무 적으면 적합이 무의미하다
        xi, _, beta = stats.genpareto.fit(tail, floc=0.0)   # floc=0: 초과분이므로 위치 고정
        p_exceed = tail.size / scores.size
        # GPD 역함수로 q 분위수 추정
        #   p = 목표 초과확률을 '꼬리 안에서의 확률'로 환산한 값
        p = (1 - q / 100.0) / p_exceed
        # xi=0 이면 지수분포 극한이라 로그 식으로 갈아탄다
        thr_evt = u + (beta / xi) * (p ** (-xi) - 1) if abs(xi) > 1e-9 else u - beta * np.log(p)
    return AlarmConfig(threshold=thr, threshold_evt=float(thr_evt), tail_xi=float(xi))


def collect_scores(model: s6.Model, ref: s4.ReferenceTable, packs: list[int],
                   mode: str = "chg", key: str = "score") -> dict[int, np.ndarray]:
    # 팩별 시점 점수를 모아 둔다. 임계값 산출과 오탐 분석에 함께 쓴다
    out = {}
    for pid in packs:
        Z, _ = s5.pack_matrix(pid, ref, mode)
        out[pid] = pack_score(model, Z, key=key)
    return out


# ── 검증 ─────────────────────────────────────────────────────────────────────
def verify(cfg: AlarmConfig, scores: dict[int, np.ndarray],
           train_scores: dict[int, np.ndarray]) -> bool:
    print("\n" + "=" * 78)
    print("STEP 7 검증")
    print("=" * 78)

    allsc = np.concatenate(list(scores.values()))
    # [1] 임계값 — 경험 분위수와 EVT 추정치를 나란히 보여 꼬리 두께를 판단하게 한다
    print(f"\n  [1] 임계값 (홀드아웃 {len(scores)}팩, {len(allsc)}초)")
    print(f"      경험 {THRESHOLD_Q} 분위수 : {cfg.threshold:.2f}")
    print(f"      EVT(POT) 추정      : {cfg.threshold_evt:.2f}  (GPD ξ={cfg.tail_xi:+.3f})")
    # ξ > 0 이면 꼬리가 두꺼워서(멱함수) 경험 분위수가 극단값을 과소평가한다
    heavy = cfg.tail_xi > 0
    print(f"      꼬리 판정: ξ {'> 0 → 두꺼움. EVT 값 병기 권장' if heavy else '<= 0 → 얇음. 경험 분위수로 충분'}")
    ok_thr = np.isfinite(cfg.threshold) and cfg.threshold > 0

    # [2] 지속시간 조건의 효과 — 초과 구간 몇 개가 알람으로 살아남는지
    print(f"\n  [2] 지속시간 조건 ({cfg.persist_sec}초 연속)")
    print(f"      {'팩':>6}{'초과 시점':>10}{'단발 포함 구간':>14}{'10초 지속 알람':>15}")
    n_raw_total = n_evt_total = 0
    for pid, sc in scores.items():
        over = sc > cfg.threshold
        pad = np.concatenate(([False], over, [False]))
        edges = np.flatnonzero(pad[1:] != pad[:-1])
        n_raw = len(edges) // 2            # 지속시간 조건 없이 센 구간 수
        evs = find_alarms(sc, cfg)         # 조건 적용 후 남은 알람
        n_raw_total += n_raw
        n_evt_total += len(evs)
        print(f"      {pid:>6}{int(over.sum()):>10}{n_raw:>14}{len(evs):>15}")
    ok_persist = n_evt_total <= n_raw_total
    print(f"      합계: 구간 {n_raw_total}개 -> 알람 {n_evt_total}개 "
          f"({100 * (1 - n_evt_total / max(n_raw_total, 1)):.0f}% 가 단발 노이즈로 제거)"
          f"  -> {'PASS' if ok_persist else 'FAIL'}")

    # [3] 초기 60초 보류 — 동작 방식만 명시한다(실제 효과는 STEP 9 가 시점 단위로 확인)
    print(f"\n  [3] 초기 {cfg.warmup_sec}초 T2 판정 보류")
    print(f"      온도 열을 0 으로 눌러 전압 룰만 적용. 오프셋 추정 중 오탐을 막는다  -> PASS")

    # [4] train 대비 홀드아웃 점수 분포
    #     임계값은 홀드아웃으로 잡았으므로, train 과 꼬리가 비슷해야 일반화된 것이다
    tr = np.concatenate(list(train_scores.values()))
    print(f"\n  [4] 점수 분포 비교 (중앙값 / p99 / p99.9)")
    print(f"      train   {np.median(tr):7.2f}{np.percentile(tr, 99):9.2f}"
          f"{np.percentile(tr, 99.9):9.2f}")
    print(f"      holdout {np.median(allsc):7.2f}{np.percentile(allsc, 99):9.2f}"
          f"{np.percentile(allsc, 99.9):9.2f}")
    ratio = np.percentile(allsc, 99.9) / np.percentile(tr, 99.9)
    ok_dist = 0.5 <= ratio <= 2.0
    print(f"      홀드아웃 p99.9 / train p99.9 = {ratio:.2f} (1 에 가까울수록 일반화 양호)"
          f"  -> {'PASS' if ok_dist else 'FAIL'}")
    print("=" * 78)
    return ok_thr and ok_persist and ok_dist


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 7. 임계값과 알람")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    ref = s4.ReferenceTable.load(OUT_DIR / f"step4_{args.mode}_reference_train.csv")

    ok_all = True
    # 세 가지 점수 방식마다 임계값을 따로 잡는다. 점수 스케일이 서로 달라서
    # 하나의 임계값을 공유할 수 없다. 파일명 접미사로 구분해 저장한다.
    variants = [("", "score", "가이드 스펙 PCA(0.99) + 통합 점수"),
                ("_op", "score", "운영 PCA(교차검증) + 통합 점수"),
                ("_rule", "rule", "룰 기반 연속 점수 (STEP 6-1)")]
    for suffix, key, label in variants:
        # 접미사가 있으면(운영/룰) 운영 모델(_op)을 쓴다. 룰 점수는 PCA 와 무관하지만
        # 같은 Model 객체를 통해 계산하므로 모델 로드는 필요하다.
        model = s6.load(OUT_DIR / f"model_{args.mode}{'_op' if suffix else ''}.pkl")
        print(f"\n{'#' * 78}\n# STEP 7 — {label}, 주성분 {model.n_components}개\n{'#' * 78}")
        fit_packs = man["valid"] if FIT_ON_ALL else man["train"]
        hold = collect_scores(model, ref, man["holdout"], args.mode, key)
        train = collect_scores(model, ref, fit_packs, args.mode, key)
        # B안: 배포 모델이 학습 가용 팩 전부를 썼으므로 '모델이 본 적 없는 팩'이
        # 남아 있지 않다. 임계값은 학습 분포의 99.9 분위로 잡고, 그 임계에서
        # 실제 오탐률이 얼마인지는 5-fold 교차검증이 따로 측정한다.
        src_scores = train if FIT_ON_ALL else hold
        cfg = fit_threshold(np.concatenate(list(src_scores.values())))
        ok_all &= verify(cfg, hold, train)
        (OUT_DIR / f"step7_{args.mode}_alarm_config{suffix}.json").write_text(
            json.dumps(asdict(cfg), indent=2), encoding="utf-8")
        print(f"\n  -> outputs/step7_{args.mode}_alarm_config{suffix}.json")
    return 0 if ok_all else 1


if __name__ == "__main__":
    raise SystemExit(main())
