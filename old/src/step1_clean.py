"""STEP 1. 데이터 정제 — docs/battery_guide.md 구간 1 구현.

1-1. 충전 구간 추출 : |Current| > 1.0 A 의 최장 연속 구간, 전류 급변 후 5초 제외
1-2. 무효 파일 제외 : 셀 전압 결함(frozen / stale) 팩 배제
1-3. 검증셋 분리    : 학습 가용 팩 중 홀드아웃 분리

실행:
    python src/step1_clean.py                 # 전체 chg 파일 정제 + 기대값 검증
    python src/step1_clean.py --mode dchg     # 방전 파일에 동일 규칙 적용
"""

# 이 파일이 파이프라인 전체의 입구다. 이후 모든 단계는 여기서 정한
#   (a) 어떤 팩을 쓸 것인가(valid),
#   (b) 각 팩의 어느 구간을 쓸 것인가(과도구간을 제외한 최장 통전 구간),
#   (c) 어떤 팩을 학습에서 빼둘 것인가(holdout)
# 세 가지를 outputs/step1_<mode>_manifest.json 으로 읽어 그대로 따른다.

from __future__ import annotations

import argparse
import hashlib          # 충전 구간 내용 해시 → 파일명만 다른 중복 팩 탐지
import importlib.util   # parquet 엔진 설치 여부 확인용
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# Windows 기본 콘솔(cp949)에서 한글·기호 출력이 깨지지 않도록
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

ROOT = Path(__file__).resolve().parents[1]   # src/ 의 부모 = 프로젝트 루트
DATA_DIR = ROOT / "data"                     # 원본 CSV (<pack_id>_chg.csv 형식)
OUT_DIR = ROOT / "outputs"                   # 산출물

# ── 컬럼 정의 ────────────────────────────────────────────────────────────────
# 팩 구조: 모듈 16개 x 모듈당 셀 11개 = 셀 176개, 모듈당 온도센서 2개 = 32개.
# CSV 컬럼명이 M01CV01 ... M16CV11 / M01T01 ... M16T02 규칙이라 그대로 생성한다.
# 이 리스트의 "순서"가 곧 이후 모든 배열의 열 순서다. 바꾸면 안 된다
# (STEP 2 의 reshape(-1, 16, 11) 이 이 순서를 전제로 모듈 블록을 자른다).
CELL_COLS = [f"M{m:02d}CV{c:02d}" for m in range(1, 17) for c in range(1, 12)]  # 176
TEMP_COLS = [f"M{m:02d}T{t:02d}" for m in range(1, 17) for t in range(1, 3)]    # 32
# BMS 가 직접 계산해 주는 대표값들. Vmin/Vmax 는 셀 전압 결함 판정의 기준선으로 쓴다
# (BMS 는 정상인데 셀 컬럼만 안 움직이면 그 파일의 셀 기록이 고장난 것이다).
META_COLS = ["Date", "Time", "SerialNumber", "Voltage", "Current",
             "RSOCavg", "Vmin", "Vmax", "Tmin", "Tmax", "Tavg"]

# ── STEP 1 파라미터 ──────────────────────────────────────────────────────────
CURRENT_ON = 1.0    # A. 통전 판정 (충전 전류는 음수)
STEP_DELTA = 5.0    # A. 전류 급변 판정. 정상 리플의 초당 변화는 최대 ~2.4 A
SETTLE_SEC = 5      # s. 급변 시점 이후 제외할 과도구간 길이
FROZEN_RATIO = 0.5  # cell_swing < 0.5 * bms_swing  → 완전 고정
STALE_RATIO = 0.05  # nunique 중앙값 < 0.05 * 길이  → 계단형 갱신
MIN_SEG_SEC = 60    # s. 이보다 짧은 충전 구간은 분석 불가로 간주
MIN_RUN_SEC = 60    # s. 이어붙일 때 이보다 짧은 통전 조각은 무시 (블립 제외)

# ── 1-3. 시간 격자 통일 ──────────────────────────────────────────────────────
# 원본 기록 주기가 팩마다 다르다. 절반은 1초/행, 절반은 5초/행이다.
# 그대로 두면 STEP 7 의 "10초 지속" 같은 행 단위 조건이 팩마다 5배 다른 시간을
# 뜻하게 되고, 기준표(STEP 4)도 행이 많은 팩 쪽으로 5배 가중된다.
# 그래서 성긴 쪽(5초)에 맞춰 촘촘한 쪽을 솎아낸다.
#
# 평균이 아니라 '솎아내기(decimation)'인 이유: 5초 팩의 각 행은 순간값 1개이지
# 5초 평균이 아니다. 평균을 내면 스파이크가 사라져 원본과 성질이 달라진다.
TARGET_SEC_PER_ROW = 5.0
# dSOC/(A·s). 1초/행 팩들에서 실측한 중앙값. 행당 실제 초를 역산하는 데 쓴다.
# Time 컬럼을 못 쓰는 이유: 일부 팩(1018~1021 등)은 5초 간격 기록인데 타임스탬프가
# 1초씩 증가하도록 잘못 적혀 있다. SOC 증가량은 BMS 자체 쿨롱카운트라 신뢰할 수 있다.
SOC_PER_A_SEC = 0.0002478

# 가이드 STEP 1-2 기대값
# 아래 12개 팩은 가이드가 "제외되어야 한다"고 명시한 목록이다.
# verify() 가 우리 규칙(frozen/stale)이 정확히 이 목록을 재현하는지 대조한다.
EXPECTED_REJECT = [1009, 1017, 1019, 1021, 1026, 1030,
                   1032, 1035, 1036, 1038, 1040, 1043]
EXPECTED_VALID_COUNT = 39   # frozen/stale 규칙만 적용했을 때 남는 수 (51 - 12)

# ── 최종 학습 후보 (chg) ─────────────────────────────────────────────────────
# 여기 없는 팩은 frozen/stale 판정과 무관하게 학습·검증 어디에도 쓰지 않는다.
# 이후 모든 STEP 이 manifest 의 valid/train/holdout 만 읽으므로, 이 목록 하나가
# 파이프라인 전체의 데이터 범위를 결정한다.
#
#   51 - 결함 12(frozen 10 / stale 2) - 중복 사본 8 - 1012 = 30
#
#   1012 제외 사유: 충전 구간 내부(row 1797)에서 SerialNumber 가 1012 -> 5191 로
#   바뀐다. 물리 신호는 연속이지만(셀 개성 상관 0.986) 팩 식별이 모호해 뺀다.
TRAIN_CANDIDATES = {
    1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1010,
    1011, 1013, 1014, 1015, 1016, 1018, 1020, 1022, 1023, 1024,
    1025, 1027, 1028, 1029, 1031, 1033, 1034, 1044, 1045, 1046,
}


@dataclass
class PackResult:
    """팩 1개의 정제·검증 결과."""

    # 이 데이터클래스 한 줄이 곧 step1_<mode>_summary.csv 한 행이 된다.
    # 판정에 쓰인 중간 통계까지 전부 남겨서, 나중에 "왜 이 팩이 빠졌나"를
    # 원본 CSV 를 다시 열지 않고도 설명할 수 있게 한다.
    pack_id: int
    n_rows: int = 0                 # 원본 행 수
    seg_start: int = -1             # 원본 기준 충전 구간 시작 행 (포함)
    seg_end: int = -1               # 원본 기준 충전 구간 끝 행 (미포함)
    n_seg: int = 0                  # 이어붙인 통전 구간 총 길이
    n_runs: int = 0                 # 이어붙인 통전 조각 수 (1이면 끊김 없음)
    n_gap: int = 0                  # 조각 사이 휴지 구간(전류 0)에서 버린 행 수
    n_clean: int = 0                # 과도구간 제외 후 남은 행 수 (솎아내기 전)
    n_transient: int = 0            # 제외된 과도구간 행 수
    sec_per_row: float = np.nan     # 원본 1행이 실제 몇 초인지 (추정)
    stride: int = 1                 # 5초 격자로 맞추기 위한 솎아내기 간격
    n_final: int = 0                # 솎아낸 뒤 최종 행 수 (= 이후 단계가 보는 길이)
    n_steps: int = 0                # 전류 급변 지점 수
    soc_min: float = np.nan
    soc_max: float = np.nan
    median_current: float = np.nan
    cell_swing: float = np.nan      # 셀별 (max-min)의 중앙값 [V]
    bms_swing: float = np.nan       # Vmax.max - Vmin.min [V]
    swing_ratio: float = np.nan     # cell_swing / bms_swing
    nunique_median: float = np.nan  # 셀별 고유값 수의 중앙값
    nunique_ratio: float = np.nan   # nunique_median / n_clean
    seg_hash: str = ""              # 정제 구간 내용 해시 (중복 팩 식별용)
    dup_of: int = -1                # 같은 구간을 가진 최소 pack_id (자기 자신이면 -1)
    mod_dev_std_mV: float = np.nan  # 모듈 편차 산포. 검증셋 층화 기준
    valid: bool = True
    reject_reason: str = ""
    split: str = ""                 # train / holdout / excluded

    def as_row(self) -> dict:
        # dataclass -> dict. DataFrame 한 행으로 그대로 들어간다
        return asdict(self)


# ── 1-1. 충전 구간 추출 ──────────────────────────────────────────────────────
def longest_active_run(current: np.ndarray, threshold: float = CURRENT_ON) -> tuple[int, int]:
    """|current| > threshold 인 최장 연속 구간의 [start, end) 를 반환."""
    # 충전 전류는 음수라서 부호를 없애고 크기만 본다.
    # 파일 하나에 충·방전이 여러 번 들어있을 수 있는데, 가장 긴 통전 한 번만 쓴다.
    active = np.abs(current) > threshold
    if not active.any():
        return 0, 0                       # 통전 구간이 아예 없음 -> 빈 구간

    # 경계에 False를 덧대 상승/하강 엣지를 찾는다
    #   active  : F T T F T T T F
    #   padded  : F F T T F T T T F F      (앞뒤에 False 를 하나씩 덧댐)
    #   edges   : 값이 바뀌는 위치들. 덧댐 덕분에 개수가 항상 짝수라
    #             짝수번째 = 시작, 홀수번째 = 끝(미포함) 으로 그냥 잘라 쓸 수 있다.
    # 파이썬 for 루프 없이 구간을 찾는 벡터화 관용구다.
    padded = np.concatenate(([False], active, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    starts, ends = edges[0::2], edges[1::2]
    best = int(np.argmax(ends - starts))   # 길이가 가장 긴 구간 선택
    return int(starts[best]), int(ends[best])


def active_runs(current: np.ndarray, threshold: float = CURRENT_ON) -> list[tuple[int, int]]:
    """|current| > threshold 인 모든 연속 구간 [(start, end), ...] 을 길이순이 아닌
    시간순으로 반환한다."""
    # longest_active_run 과 같은 엣지 탐색 관용구. 여기서는 최장 하나가 아니라 전부 준다.
    active = np.abs(current) > threshold
    if not active.any():
        return []
    padded = np.concatenate(([False], active, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    return [(int(a), int(b)) for a, b in zip(edges[0::2], edges[1::2])]


def transient_mask(current: np.ndarray,
                   step_delta: float = STEP_DELTA,
                   settle: int = SETTLE_SEC,
                   run_starts: tuple[int, ...] = (0,)) -> np.ndarray:
    """과도구간(True) 마스크. 구간 진입 직후와 전류 급변 직후 settle초를 제외한다.

    run_starts 는 이어붙인 배열에서 각 통전 조각이 시작하는 인덱스다.
    같은 전류로 재개된 조각(|dI| 가 작아 급변 판정에 안 걸리는 경우)도
    통전 재개 직후는 과도구간이므로 여기서 명시적으로 잘라낸다.
    """
    # 전류가 확 바뀐 직후 셀 전압은 내부저항 때문에 계단처럼 튄다.
    # 이건 셀 불량이 아니라 물리적 과도현상이므로 남겨두면 전 팩에서 오탐이 난다.
    n = len(current)
    mask = np.zeros(n, dtype=bool)
    if n == 0:
        return mask

    for s in run_starts:      # 통전 시작(및 재개) 자체가 하나의 급변이다
        mask[s:s + settle] = True

    # diff 로 초당 변화량을 구하고 임계를 넘은 지점 뒤 settle 초를 잘라낸다.
    # +1 은 diff 인덱스(i = current[i+1]-current[i])를 '변화가 나타난 시점'으로
    # 옮기기 위한 보정이다.
    steps = np.flatnonzero(np.abs(np.diff(current)) > step_delta) + 1
    for i in steps:
        mask[i:i + settle] = True          # 구간이 겹쳐도 True 로 덮어쓰면 그만이다
    return mask


def extract_charge_segment(df: pd.DataFrame, res: PackResult,
                           join: bool = True) -> pd.DataFrame:
    """통전 구간을 뽑고 과도구간을 제거한 DataFrame 을 반환.

    join=True 면 충전 도중 전류가 끊겼다 재개된 조각들을 휴지 구간만 버리고
    이어붙인다 (1004/1005 의 2단 급속충전, 1023 의 짧은 선행 충전).
    join=False 면 예전 동작대로 최장 조각 하나만 쓴다.
    """
    current = df["Current"].to_numpy(dtype=float)
    soc = df["RSOCavg"].to_numpy(dtype=float)
    runs = active_runs(current)
    if not runs:
        res.seg_start, res.seg_end, res.n_seg, res.n_runs = 0, 0, 0, 0
        return df.iloc[0:0]                # 컬럼 구조만 같은 빈 DataFrame

    # 행당 초는 '최장 단일 조각'에서만 추정한다.
    #   이어붙인 구간 전체로 재면 안 되는 이유: 1004/1005 는 -108 A 급속 구간과
    #   -54 A 구간이 섞여 있는데, BMS SOC 추정기의 dSOC/(A·s) 가 C-rate 에 따라
    #   16% 달라진다(실측 2.60e-4 vs 2.23e-4). 그러면 행당 초가 1.34 로 부풀어
    #   stride 가 5 대신 4 로 잡히고 5초 격자가 깨진다.
    a, b = longest = max(runs, key=lambda r: r[1] - r[0])
    res.sec_per_row = estimate_sec_per_row(soc[a:b], current[a:b])
    if join:
        # 조각 길이 기준은 '초'다. 행당 초가 팩마다 달라서(1초 vs 5초) 행으로 자르면
        # 5초 팩의 짧은 조각이 과하게 잘려나간다.
        min_rows = max(1, int(round(MIN_RUN_SEC / res.sec_per_row)))
        keep = [r for r in runs if r[1] - r[0] >= min_rows] or [longest]
    else:
        keep = [longest]

    res.n_runs = len(keep)
    res.seg_start, res.seg_end = keep[0][0], keep[-1][1]
    res.n_seg = sum(b - a for a, b in keep)
    # 버린 휴지 구간 행 수 = 전체 span 에서 실제 쓰는 행을 뺀 것
    res.n_gap = (res.seg_end - res.seg_start) - res.n_seg

    # 조각들을 이어붙인다. 인덱스는 리셋해서 이후 마스크 연산이 위치 기반이 되게 한다
    seg = pd.concat([df.iloc[a:b] for a, b in keep]).reset_index(drop=True)
    seg_current = seg["Current"].to_numpy(dtype=float)
    # 이어붙인 배열에서 각 조각이 시작하는 위치 (0, len0, len0+len1, ...)
    starts = tuple(int(x) for x in np.cumsum([0] + [b - a for a, b in keep[:-1]]))
    tmask = transient_mask(seg_current, run_starts=starts)

    res.n_steps = int((np.abs(np.diff(seg_current)) > STEP_DELTA).sum())
    res.n_transient = int(tmask.sum())
    res.median_current = float(np.median(seg_current))

    # ~tmask = 과도구간이 아닌 행만 남긴다. 시간축에 구멍이 생기지만
    # 이후 피처는 전부 '같은 시점의 셀들끼리 비교'라서 문제되지 않는다.
    clean = seg.loc[~tmask].reset_index(drop=True)
    res.n_clean = len(clean)
    if res.n_clean:
        # 이 팩이 어느 SOC 범위를 도는지. STEP 4 기준표 커버리지의 근거가 된다
        res.soc_min = float(clean["RSOCavg"].min())
        res.soc_max = float(clean["RSOCavg"].max())
    return clean


# ── 1-2. 무효 파일 제외 ──────────────────────────────────────────────────────
def check_validity(seg: pd.DataFrame, res: PackResult) -> None:
    """셀 전압 결함(frozen / stale) 판정. res 를 갱신한다."""
    # 여기서 거르는 건 '배터리 불량'이 아니라 '기록이 고장난 파일'이다.
    # 정상 데이터만으로 학습하는 One-class 모델이라 이런 파일이 섞이면
    # 기준표(STEP 4)의 산포가 통째로 왜곡된다.
    if res.n_clean < MIN_SEG_SEC:
        res.valid = False
        res.reject_reason = "too_short"
        return

    cells = seg[CELL_COLS]

    # 조건 1: 완전 고정 — 셀 컬럼이 BMS Vmin/Vmax 만큼 움직이지 않음
    #   충전 중이면 셀 전압은 반드시 오른다. BMS 대표값은 제대로 오르는데
    #   셀 컬럼만 제자리라면 셀 전압 기록 경로가 죽은 것이다.
    cell_swing = float((cells.max() - cells.min()).median())
    bms_swing = float(seg["Vmax"].max() - seg["Vmin"].min())
    res.cell_swing = cell_swing
    res.bms_swing = bms_swing
    res.swing_ratio = cell_swing / bms_swing if bms_swing else np.nan

    # 조건 2: 계단형 갱신 — 고유값 수가 길이 대비 극단적으로 적음
    #   값이 몇 초에 한 번씩만 갱신되면(홀드) 고유값 개수가 길이에 비해 훨씬 적다.
    #   실제 시간 해상도가 1초가 아니어서 잔차 통계가 어긋난다.
    nunique_median = float(cells.nunique().median())
    res.nunique_median = nunique_median
    res.nunique_ratio = nunique_median / res.n_clean

    if cell_swing < FROZEN_RATIO * bms_swing:
        res.valid = False
        res.reject_reason = "frozen"
    elif nunique_median < STALE_RATIO * res.n_clean:
        res.valid = False
        res.reject_reason = "stale"

    # 살아남은 팩만 층화 기준값을 계산한다 (제외된 팩 값은 어차피 쓰지 않는다)
    if res.valid:
        res.mod_dev_std_mV = module_deviation_std(cells.to_numpy(dtype=float)) * 1e3


def module_deviation_std(cells: np.ndarray) -> float:
    """모듈 편차(STEP 2 의 mod_dev)의 산포 [V]. 층화 추출 기준으로만 쓴다.

    팩마다 이 값이 1~6 mV 로 크게 다르고, 그 차이가 이상점수 분포를 좌우한다.
    무작위로 뽑으면 홀드아웃이 한쪽으로 쏠려 임계값 검증이 의미를 잃는다.
    """
    # STEP 2 분해를 여기서만 축약해 계산한다(필요한 건 산포 하나뿐이라 의존성을 안 만든다).
    grid = cells.reshape(-1, 16, 11)          # (T, 모듈16, 셀11)
    v_mod = np.median(grid, axis=2)           # 모듈별 중앙값 (T, 16)
    # 시점마다 모듈 중앙값들의 중앙값(= 팩 중심)을 빼면 모듈 편차가 된다
    return float(np.std(v_mod - np.median(v_mod, axis=1)[:, None]))


def estimate_sec_per_row(soc: np.ndarray, current: np.ndarray) -> float:
    """원본 1행이 실제 몇 초인지 추정한다.

    RSOCavg 는 BMS 의 쿨롱카운트라 dSOC/dt 가 전류에 비례한다 (실측 상관 0.999999).
    따라서  행당 dSOC / (|I| * SOC_PER_A_SEC)  가 곧 행당 초가 된다.
    전류로 나누므로 1004/1005 처럼 C-rate 가 다른 팩도 같은 식으로 처리된다.
    """
    d = np.diff(soc)
    d = d[d > 0]                      # SOC 가 실제로 오른 스텝만 (양자화 0 스텝 제외)
    i = float(np.median(np.abs(current)))
    if not d.size or i <= 0:
        return 1.0                    # 판정 불가 -> 솎아내지 않는다
    return float(d.mean() / (i * SOC_PER_A_SEC))


def downsample(seg: pd.DataFrame, res: PackResult,
               target: float = TARGET_SEC_PER_ROW) -> pd.DataFrame:
    """모든 팩을 target 초/행 격자로 통일한다. res 를 갱신한다."""
    if seg.empty:
        res.n_final = 0
        return seg
    # sec_per_row 는 extract_charge_segment 가 최장 단일 조각에서 이미 재놨다.
    # 여기서 다시 재면 이어붙인 다중 C-rate 구간이 섞여 값이 틀어진다.
    if not np.isfinite(res.sec_per_row):
        res.sec_per_row = estimate_sec_per_row(seg["RSOCavg"].to_numpy(dtype=float),
                                               seg["Current"].to_numpy(dtype=float))
    res.stride = max(1, int(round(target / res.sec_per_row)))
    # iloc[::stride] = 솎아내기. 각 행은 원본 순간값 그대로라 스파이크가 보존된다
    out = seg.iloc[::res.stride].reset_index(drop=True)
    res.n_final = len(out)
    return out


def segment_hash(seg: pd.DataFrame) -> str:
    """정제 구간의 내용 해시. 파일명이 달라도 같은 충전 세션인지 판별한다."""
    # 데이터셋에 같은 충전 세션이 다른 pack_id 로 중복 수록된 경우가 있다.
    # 모르고 train/holdout 을 나누면 홀드아웃에 학습 데이터의 사본이 들어가
    # 오탐률·일반화 수치가 전부 낙관적으로 나온다(데이터 누수).
    if seg.empty:
        return ""
    arr = seg[CELL_COLS + ["RSOCavg", "Current"]].to_numpy(dtype=float)
    return hashlib.md5(arr.tobytes()).hexdigest()   # 보안이 아니라 동일성 비교 용도


# ── 1-3. 검증셋 분리 ─────────────────────────────────────────────────────────
def split_holdout(valid_ids: list[int], hashes: dict[int, str] | None = None,
                  n_holdout: int = 6, seed: int = 0,
                  strata: dict[int, float] | None = None) -> list[int]:
    """학습 가용 팩에서 홀드아웃을 결정적으로 선택한다.

    두 가지를 지킨다.
    1. 구간 내용이 같은 팩(파일명만 다른 동일 충전 세션)은 한 덩어리로 묶어
       train/holdout 어느 한쪽에만 넣는다. 사본이 갈리면 오탐률이 무의미해진다.
    2. strata 가 주어지면 그 값 기준으로 층화 추출한다. 무작위로 뽑으면 모듈 편차가
       큰 팩만 홀드아웃에 몰려(실측 중앙값 4.0 vs 2.4 mV) 임계값이 과대 설정된다.
    """
    if not valid_ids:
        return []

    hashes = hashes or {}
    # 같은 해시끼리 한 그룹. 해시가 없으면 자기 자신만 있는 그룹으로 만든다
    # (키를 "__1000" 처럼 만들어 실제 해시와 절대 충돌하지 않게 한다)
    groups: dict[str, list[int]] = {}
    for pid in valid_ids:
        groups.setdefault(hashes.get(pid) or f"__{pid}", []).append(pid)

    keys = sorted(groups)                       # 정렬 -> 매 실행 같은 순서 보장
    rng = np.random.default_rng(seed)           # seed 고정 -> 결정적 선택

    if strata:
        # 층화: 그룹 대표값으로 정렬한 뒤 등간격 구간마다 1개씩 뽑는다
        #   정렬 후 균등 분할해서 각 칸에서 하나씩 고르면
        #   '편차 작은 팩 ~ 큰 팩'이 골고루 홀드아웃에 들어간다.
        keys.sort(key=lambda k: float(np.mean([strata.get(p, np.nan) for p in groups[k]])))
        avg = np.mean([len(groups[k]) for k in keys])       # 그룹당 평균 팩 수
        n_pick = max(1, int(round(n_holdout / avg)))        # 몇 개 그룹을 뽑아야 정원이 차나
        chunks = np.array_split(np.arange(len(keys)), n_pick)
        order = [keys[int(rng.choice(ch))] for ch in chunks if len(ch)]
        order += [k for k in keys if k not in order]        # 나머지는 뒤에 붙여 예비로 둔다
    else:
        order = list(keys)
        rng.shuffle(order)                                  # 단순 무작위 (--random-split)

    picked: list[int] = []
    for key in order:
        if len(picked) >= n_holdout:
            break
        members = groups[key]
        # 그룹은 통째로만 넣는다. 정원을 넘기면 건너뛰고 다음 후보로 간다
        # (쪼개면 사본이 train/holdout 으로 갈려 누수가 생기므로).
        if len(picked) + len(members) <= n_holdout:
            picked.extend(members)
    return sorted(picked)


# ── 파이프라인 ───────────────────────────────────────────────────────────────
def pack_ids(mode: str = "chg") -> list[int]:
    # data/1000_chg.csv -> 1000. 파일명 앞부분이 팩 ID 다
    return sorted(int(p.name.split("_")[0]) for p in DATA_DIR.glob(f"*_{mode}.csv"))


def load_pack(pack_id: int, mode: str = "chg") -> pd.DataFrame:
    # usecols 로 필요한 219개 컬럼만 읽는다 (원본의 나머지 컬럼은 쓰지 않는다)
    return pd.read_csv(DATA_DIR / f"{pack_id}_{mode}.csv",
                       usecols=META_COLS + CELL_COLS + TEMP_COLS)


def save_segment(seg: pd.DataFrame, seg_dir: Path, pack_id: int) -> Path:
    """정제 구간 저장. parquet 엔진이 없으면 csv.gz 로 떨어뜨린다."""
    # find_spec 은 실제 import 없이 설치 여부만 확인한다(불필요한 로딩 회피)
    if importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"):
        path = seg_dir / f"{pack_id}.parquet"
        seg.to_parquet(path, index=False)
    else:
        path = seg_dir / f"{pack_id}.csv.gz"
        seg.to_csv(path, index=False, compression="gzip")
    return path


def clean_pack(pack_id: int, mode: str = "chg",
               join: bool = True) -> tuple[pd.DataFrame, PackResult]:
    """팩 1개에 STEP 1-1, 1-2 를 적용한다."""
    # STEP 2·3 도 이 함수를 그대로 호출한다. "정제 규칙은 여기 한 곳"이라는 원칙.
    res = PackResult(pack_id=pack_id)
    df = load_pack(pack_id, mode)
    res.n_rows = len(df)
    seg = extract_charge_segment(df, res, join=join)   # 1-1  통전 구간 + 과도구간 제거
    # 1-2 는 반드시 '솎기 전' 원본 해상도에서 판정한다.
    #   frozen/stale 기준(swing 비율, nunique 비율)이 행 수에 의존하므로,
    #   솎은 뒤에 재면 가이드가 지정한 12팩 목록을 재현하지 못한다.
    check_validity(seg, res)                # 1-2  결함 파일 판정
    seg = downsample(seg, res)              # 1-3  5초 격자로 통일
    res.seg_hash = segment_hash(seg)
    return seg, res


def run_step1(mode: str = "chg", n_holdout: int = 6, seed: int = 0,
              save_segments: bool = False, verbose: bool = True,
              stratify: bool = True,
              candidates: set[int] | None = None,
              join: bool = True) -> pd.DataFrame:
    # candidates: 학습에 쓸 팩 목록. None 이면 frozen/stale 규칙만으로 거른다.
    #   chg 기본값은 TRAIN_CANDIDATES(30팩). 호출부에서 --all-valid 로 끌 수 있다.
    if candidates is None and mode == "chg":
        candidates = TRAIN_CANDIDATES
    ids = pack_ids(mode)
    results: list[PackResult] = []

    # 1) 팩을 하나씩 정제·판정한다 (한 번에 파일 하나만 메모리에 올린다)
    for pid in ids:
        seg, res = clean_pack(pid, mode, join=join)
        results.append(res)
        if verbose:
            flag = "OK" if res.valid else f"REJECT({res.reject_reason})"
            print(f"  {pid}  rows={res.n_rows:>6}  seg={res.n_seg:>5}  "
                  f"clean={res.n_clean:>5}  steps={res.n_steps:>2}  "
                  f"SOC={res.soc_min:5.1f}~{res.soc_max:5.1f}  "
                  f"swing={res.swing_ratio:5.2f}  nuniq={res.nunique_ratio:6.4f}  {flag}",
                  flush=True)
        if save_segments and res.valid:
            seg_dir = OUT_DIR / f"segments_{mode}"
            seg_dir.mkdir(parents=True, exist_ok=True)
            save_segment(seg, seg_dir, pid)

    table = pd.DataFrame([r.as_row() for r in results])

    # 같은 충전 세션이 다른 파일명으로 중복 수록된 경우를 표시한다
    #   해시가 같은 그룹의 최소 pack_id 를 원본으로 보고, 나머지 행에 그 번호를 적는다
    first = table.groupby("seg_hash")["pack_id"].transform("min")
    table["dup_of"] = np.where((table["seg_hash"] != "") & (table["pack_id"] != first),
                               first, -1)

    # 1-4) 최종 학습 후보만 남긴다.
    #   frozen/stale 판정(1-2)은 그대로 두고 그 위에 덮어쓴다. 그래야 verify() 가
    #   "우리 규칙이 가이드의 12팩을 재현하는가"를 계속 대조할 수 있다.
    #   제외 사유는 구분해서 적는다: 중복 사본이면 duplicate, 그 외는 not_selected.
    if candidates is not None:
        drop = table["valid"] & ~table["pack_id"].isin(candidates)
        table.loc[drop, "reject_reason"] = np.where(
            table.loc[drop, "dup_of"] > 0, "duplicate", "not_selected")
        table.loc[drop, "valid"] = False

    # 2) train / holdout 분리 (제외된 팩은 excluded)
    valid_ids = table.loc[table["valid"], "pack_id"].tolist()
    hashes = dict(zip(table["pack_id"], table["seg_hash"]))
    strata = dict(zip(table["pack_id"], table["mod_dev_std_mV"])) if stratify else None
    holdout = split_holdout(valid_ids, hashes, n_holdout=n_holdout, seed=seed, strata=strata)
    table["split"] = np.where(~table["valid"], "excluded",
                              np.where(table["pack_id"].isin(holdout), "holdout", "train"))

    # 3) 산출물 두 개를 남긴다
    #    - summary.csv  : 사람이 보는 팩별 상세 표
    #    - manifest.json: 이후 모든 STEP 이 읽는 계약서 (valid/train/holdout/구간 위치)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUT_DIR / f"step1_{mode}_summary.csv", index=False)
    manifest = {
        "mode": mode,
        # 어떤 파라미터로 만든 결과인지 함께 적어 재현성을 남긴다
        "params": {"current_on": CURRENT_ON, "step_delta": STEP_DELTA,
                   "settle_sec": SETTLE_SEC, "frozen_ratio": FROZEN_RATIO,
                   "stale_ratio": STALE_RATIO, "seed": seed,
                   "target_sec_per_row": TARGET_SEC_PER_ROW,
                   "join_runs": join, "min_run_sec": MIN_RUN_SEC,
                   "split": "stratified" if stratify else "random"},
        "n_files": len(ids),
        # 명시적 학습 후보 목록. null 이면 frozen/stale 규칙만으로 걸렀다는 뜻이다
        "candidates": sorted(candidates) if candidates is not None else None,
        "rejected": table.loc[~table["valid"], "pack_id"].tolist(),
        "reject_reasons": dict(zip(table.loc[~table["valid"], "pack_id"].astype(str),
                                   table.loc[~table["valid"], "reject_reason"])),
        "valid": valid_ids,
        # 고유 충전 세션 수 = 중복을 걷어낸 실질 데이터 양
        "n_unique_segments": int(table.loc[table["valid"], "seg_hash"].nunique()),
        "duplicate_groups": [sorted(int(p) for p in g)
                             for _, g in table.loc[table["valid"]].groupby("seg_hash")["pack_id"]
                             if len(g) > 1],
        "holdout": holdout,
        "train": [p for p in valid_ids if p not in holdout],
        # 팩별 구간 위치. 원본 CSV 의 몇 번째 행부터 몇 번째 행까지를 썼는지 추적용
        "segments": {str(r.pack_id): {"start": r.seg_start, "end": r.seg_end,
                                      "n_clean": r.n_clean, "n_final": r.n_final,
                                      "sec_per_row": r.sec_per_row, "stride": r.stride,
                                      "seg_hash": r.seg_hash}
                     for r in results},
    }
    (OUT_DIR / f"step1_{mode}_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    return table


def verify(table: pd.DataFrame, mode: str) -> bool:
    """가이드 STEP 1-2 기대값과 대조."""
    # 이 프로젝트의 모든 STEP 은 마지막에 verify() 로 "가이드가 말한 수치가 실제로
    # 나오는가"를 스스로 검사하고 PASS/FAIL 을 찍는다. 반환값이 종료 코드가 된다.
    rejected = sorted(table.loc[~table["valid"], "pack_id"].tolist())
    valid_n = int(table["valid"].sum())
    reasons = table.loc[~table["valid"], "reject_reason"].value_counts().to_dict()

    print("\n" + "=" * 78)
    print(f"STEP 1 검증 ({mode})")
    print("=" * 78)
    valid = table.loc[table["valid"]]
    dup_groups = [sorted(int(p) for p in g)
                  for _, g in valid.groupby("seg_hash")["pack_id"] if len(g) > 1]
    # 누수 검사: 같은 해시 그룹의 팩들이 서로 다른 split 에 들어갔으면 실패다
    leaked = [g for g in dup_groups
              if len({table.set_index("pack_id").loc[p, "split"] for p in g}) > 1]

    print(f"  파일 수         : {len(table)}")
    print(f"  제외 팩 ({len(rejected):2d}개)  : {rejected}")
    print(f"  사유별          : {reasons}")
    print(f"  학습 가용       : {valid_n}개 (고유 충전 세션 {valid['seg_hash'].nunique()}개)")
    print(f"  중복 팩 쌍      : {dup_groups}")
    print(f"  split 누수      : {'없음' if not leaked else leaked}")
    print(f"  SOC 시작점      : {valid['soc_min'].min():.1f}% ~ {valid['soc_min'].max():.1f}%")

    if mode != "chg":
        # 가이드의 기대값 목록은 충전(chg)에 대해서만 주어져 있다
        print("  (기대값은 chg 기준으로만 정의되어 있어 대조 생략)")
        return not leaked

    # [A] 결함 판정 규칙 검증 — 최종 선정과 별개로, frozen/stale 규칙 자체가
    #     가이드의 12팩을 그대로 재현하는지 본다. 선정 목록을 바꿔도 이 검사는 유지된다.
    defect = sorted(table.loc[table["reject_reason"].isin(["frozen", "stale"]),
                              "pack_id"].tolist())
    ok_set = defect == EXPECTED_REJECT
    ok_reason = reasons.get("frozen") == 10 and reasons.get("stale") == 2
    ok_cnt = len(table) - len(defect) == EXPECTED_VALID_COUNT

    print("\n  [A. 결함 판정 규칙 — 가이드 1-2 대조]")
    print(f"  frozen/stale 팩 == {EXPECTED_REJECT}")
    print(f"    -> {'PASS' if ok_set else 'FAIL'}")
    if not ok_set:
        # 어긋났을 때 '무엇이' 어긋났는지까지 찍어야 규칙을 고칠 수 있다
        print(f"      누락(기대O 실제X): {sorted(set(EXPECTED_REJECT) - set(defect))}")
        print(f"      과잉(기대X 실제O): {sorted(set(defect) - set(EXPECTED_REJECT))}")
    print(f"  frozen 10 / stale 2       -> {'PASS' if ok_reason else 'FAIL'} "
          f"(frozen {reasons.get('frozen', 0)} / stale {reasons.get('stale', 0)})")
    print(f"  결함 제외 후 {EXPECTED_VALID_COUNT}팩       -> {'PASS' if ok_cnt else 'FAIL'} "
          f"(실제 {len(table) - len(defect)})")

    # [B] 최종 선정 검증 — TRAIN_CANDIDATES 와 정확히 일치해야 한다
    ok_sel = set(valid["pack_id"]) == TRAIN_CANDIDATES
    ok_uniq = valid["seg_hash"].nunique() == valid_n      # 사본이 하나도 없어야 한다
    # 가이드 4-3: SOC 시작점 최소 24.2% / 최대 60.9% (0.15%p 허용).
    #   상한 60.9 는 '최장 통전 조각 하나만 쓴' 1004 의 잘린 시작점이다. 끊긴 조각을
    #   이어붙이면 1004 가 SOC 29.9 부터 시작하므로 이 상한은 성립하지 않는다.
    #   그래서 하한은 항상 검사하고, 상한은 이어붙이기를 끈 경우에만 대조한다.
    joined = bool((valid["n_runs"] > 1).any())
    ok_soc = abs(valid["soc_min"].min() - 24.2) <= 0.15
    if not joined:
        ok_soc = ok_soc and abs(valid["soc_min"].max() - 60.9) <= 0.15

    print(f"\n  [B. 최종 학습 후보 {len(TRAIN_CANDIDATES)}팩]")
    print(f"  선정 == TRAIN_CANDIDATES  -> {'PASS' if ok_sel else 'FAIL'} (실제 {valid_n}팩)")
    if not ok_sel:
        print(f"      누락: {sorted(TRAIN_CANDIDATES - set(valid['pack_id']))}")
        print(f"      과잉: {sorted(set(valid['pack_id']) - TRAIN_CANDIDATES)}")
    print(f"  사본 0개 (전부 고유 세션)  -> {'PASS' if ok_uniq else 'FAIL'} "
          f"({valid_n}팩 / 고유 {valid['seg_hash'].nunique()}개)")
    joined_n = int((valid["n_runs"] > 1).sum())
    print(f"  이어붙인 팩              : {joined_n}개 "
          f"{sorted(valid.loc[valid['n_runs'] > 1, 'pack_id'].tolist())}")
    print(f"  SOC 시작점 하한 24.2%     -> {'PASS' if ok_soc else 'FAIL'} "
          f"({valid['soc_min'].min():.1f} ~ {valid['soc_min'].max():.1f})"
          + ("   (상한 60.9 는 이어붙이기 시 미적용)" if joined else ""))
    print(f"  train/holdout 누수 없음   -> {'PASS' if not leaked else 'FAIL'}")
    print("=" * 78)
    return ok_set and ok_cnt and ok_reason and ok_sel and ok_uniq and ok_soc and not leaked


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 1. 데이터 정제")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--holdout", type=int, default=6)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--save-segments", action="store_true",
                    help="정제된 구간을 outputs/segments_<mode>/ 에 parquet 으로 저장")
    ap.add_argument("--random-split", action="store_true",
                    help="층화 추출 대신 단순 무작위로 홀드아웃을 뽑는다")
    ap.add_argument("--all-valid", action="store_true",
                    help="TRAIN_CANDIDATES 를 무시하고 frozen/stale 규칙만으로 거른다")
    ap.add_argument("--no-join", action="store_true",
                    help="끊긴 통전 조각을 이어붙이지 않고 최장 조각 하나만 쓴다")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    cand = None if args.all_valid else (TRAIN_CANDIDATES if args.mode == "chg" else None)
    print(f"STEP 1 데이터 정제 — mode={args.mode}, "
          f"학습 후보 {'전체(frozen/stale 만 제외)' if cand is None else f'{len(cand)}팩 고정'}")
    table = run_step1(mode=args.mode, n_holdout=args.holdout, seed=args.seed,
                      save_segments=args.save_segments, verbose=not args.quiet,
                      stratify=not args.random_split, candidates=cand,
                      join=not args.no_join)
    ok = verify(table, args.mode)

    tr = table.loc[table["split"] == "train", "pack_id"].tolist()
    ho = table.loc[table["split"] == "holdout", "pack_id"].tolist()
    print(f"\n  train  ({len(tr)}개): {tr}")
    print(f"  holdout({len(ho)}개): {ho}")
    print(f"\n  -> outputs/step1_{args.mode}_summary.csv")
    print(f"  -> outputs/step1_{args.mode}_manifest.json")
    return 0 if ok else 1     # 기대값 불일치는 종료 코드 1 (run_all.py 가 집계한다)


if __name__ == "__main__":
    raise SystemExit(main())
