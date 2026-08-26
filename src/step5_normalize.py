"""STEP 5. 정규화 — docs/battery_guide.md 구간 5 구현.

    z = (feature - med[soc_bin]) / sigma[soc_bin]      (SOC 선형보간 조회)

모든 피처가 동일 척도가 되어 하나의 모델에 넣을 수 있다.

실행:
    python src/step5_normalize.py
"""

# 이 단계가 하는 일은 두 가지다.
#   1. 단위 통일: V1 은 볼트, V9 는 무차원 비율, T2 는 섭씨다. 그대로 이어 붙이면
#      PCA 가 '값이 큰 열'만 본다. 기준표로 나눠 전부 "정상 대비 몇 배 벗어났나"로 바꾼다.
#   2. 열 배치 고정: 784차원 행렬의 어느 열이 어느 셀인지 못 박는다. 이 배치가 있어야
#      STEP 6 이 SPE 기여도를 "M08CV01" 같은 이름으로 되돌릴 수 있다.

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import step1_clean as s1
import step3_features as s3
import step4_reference as s4

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR

# 모델 입력 열 배치 (합계 784)
# V4(팩당 스칼라)와 T1(보정값)은 시간축이 없어 행렬에 넣지 않는다.
# 대신 STEP 8 이 유형 분류에서 직접 참조한다.
FEATURE_ORDER = ["V1", "V2", "V5", "V6", "V8", "V9", "T2", "T3", "T5"]
FEATURE_WIDTH = {"V1": 176, "V2": 176, "V5": 16, "V6": 16, "V8": 160,
                 "V9": 176, "T2": 32, "T3": 16, "T5": 16}
N_DIM = sum(FEATURE_WIDTH.values())
Z_CLIP = 50.0          # 수치 폭주 방지
EXP_MAD_TOL = 0.35     # 홀드아웃 MAD 허용 오차


def column_slices() -> dict[str, slice]:
    # 피처 이름 -> 행렬에서 그 피처가 차지하는 열 범위.
    # STEP 6 의 룰 점수, STEP 7 의 온도 열 마스킹이 전부 이 slice 를 쓴다
    out, start = {}, 0
    for f in FEATURE_ORDER:
        out[f] = slice(start, start + FEATURE_WIDTH[f])
        start += FEATURE_WIDTH[f]
    return out


COL_SLICE = column_slices()     # 모듈 로드 시 한 번만 계산


def column_labels() -> list[str]:
    """열 -> '피처:대상' 라벨. SPE 기여도 분해에서 원인 특정에 쓴다."""
    # 폭(개수)으로 대상 종류를 판별한다: 176=셀, 160=모듈내 인접쌍, 32=온도센서, 16=모듈
    labels: list[str] = []
    for f in FEATURE_ORDER:
        n = FEATURE_WIDTH[f]
        if n == 176:
            labels += [f"{f}:{c}" for c in s1.CELL_COLS]
        elif n == 160:
            # V8 은 모듈 k 의 j번째와 j+1번째 셀 쌍이다
            labels += [f"{f}:M{k + 1:02d}CV{j + 1:02d}-CV{j + 2:02d}"
                       for k in range(16) for j in range(10)]
        elif n == 32:
            labels += [f"{f}:{c}" for c in s1.TEMP_COLS]
        else:
            labels += [f"{f}:M{k + 1:02d}" for k in range(16)]
    return labels


COL_LABELS = column_labels()


def normalize(feats: dict[str, np.ndarray], ref: s4.ReferenceTable,
              soc: np.ndarray) -> dict[str, np.ndarray]:
    """피처 묶음 -> robust z 묶음."""
    # clip 은 sigma 가 극히 작은 구간에서 z 가 수천까지 튀는 것을 막는다.
    # 어차피 임계값은 6 근처라 50 이상은 "확실히 이상"으로 같은 취급이면 충분하다.
    return {f: np.clip(ref.robust_z(f, feats[f], soc), -Z_CLIP, Z_CLIP)
            for f in FEATURE_ORDER}


def feature_matrix(z: dict[str, np.ndarray]) -> np.ndarray:
    """z 묶음 -> (T, 784) 모델 입력 행렬. NaN 은 0(정보 없음)으로 둔다."""
    # FEATURE_ORDER 순서로 이어 붙이므로 COL_SLICE/COL_LABELS 와 항상 일치한다.
    # NaN -> 0 은 "정상값(중앙값)과 같다"는 뜻이라 모델을 자극하지 않는 안전한 대체다.
    mat = np.concatenate([z[f] for f in FEATURE_ORDER], axis=1)
    return np.nan_to_num(mat, nan=0.0, posinf=Z_CLIP, neginf=-Z_CLIP)


def pack_matrix(pack_id: int, ref: s4.ReferenceTable,
                mode: str = "chg") -> tuple[np.ndarray, np.ndarray]:
    """팩 1개 -> (Z 행렬, SOC)."""
    # 캐시 로드 -> 피처 -> 정규화 -> 행렬. 이후 단계들이 가장 자주 부르는 진입점이다
    c = s3.load_cache(pack_id, mode)
    feats = s3.build_features(c)
    z = normalize(feats, ref, c["soc"])
    return feature_matrix(z), c["soc"]


def _mad(x: np.ndarray) -> float:
    # 검증용 로버스트 산포. 정규화가 잘 됐다면 이 값이 1 근처여야 한다
    x = x[np.isfinite(x)]
    return float(np.median(np.abs(x - np.median(x))) * s4.MAD_TO_SIGMA)


def verify(mode: str = "chg") -> bool:
    man = json.loads((OUT_DIR / f"step1_{mode}_manifest.json").read_text(encoding="utf-8"))
    # 반드시 train 기준표로 평가한다. all(39팩) 표를 쓰면 홀드아웃이 자기 기준으로
    # 자기를 평가하게 되어 일반화 검증이 무의미해진다.
    ref = s4.ReferenceTable.load(OUT_DIR / f"step4_{mode}_reference_train.csv")

    print("\n" + "=" * 78)
    print(f"STEP 5 검증 — 기준표는 train {len(man['train'])}팩, "
          f"평가는 홀드아웃 {len(man['holdout'])}팩 분리")
    print("=" * 78)

    stats = {}
    for split in ("train", "holdout"):
        acc = {f: [] for f in FEATURE_ORDER}
        for pid in man[split]:
            c = s3.load_cache(pid, mode)
            z = normalize(s3.build_features(c), ref, c["soc"])
            for f in FEATURE_ORDER:
                v = z[f][::7].ravel()          # 표본 추출로 메모리 억제
                acc[f].append(v[np.isfinite(v)].astype(np.float32))
        stats[split] = {f: np.concatenate(acc[f]) for f in FEATURE_ORDER}

    # [1] 척도 통일 — 모든 피처의 z 중앙값 0 / MAD 1
    #     train 은 기준표를 자기 데이터로 만들었으니 당연히 맞아야 하고(정의상),
    #     의미 있는 검사는 아래 [2] 의 홀드아웃이다.
    print(f"\n  [1] 척도 통일 — 피처별 robust z 분포 (train 은 기준표 자기 자신이라 정의상 0/1)")
    print(f"      {'피처':<5}{'train med':>11}{'train MAD':>11}{'hold med':>11}"
          f"{'hold MAD':>11}{'hold |z|>6 %':>14}")
    ok_scale = True
    for f in FEATURE_ORDER:
        tr, ho = stats["train"][f], stats["holdout"][f]
        t_med, t_mad = float(np.median(tr)), _mad(tr)
        h_med, h_mad = float(np.median(ho)), _mad(ho)
        rate = 100.0 * float((np.abs(ho) > 6).mean())   # 룰 임계 6 을 넘는 정상 데이터 비율
        ok = abs(t_med) < 0.2 and abs(t_mad - 1.0) < 0.15
        ok_scale &= ok
        print(f"      {f:<5}{t_med:>11.4f}{t_mad:>11.4f}{h_med:>11.4f}{h_mad:>11.4f}"
              f"{rate:>13.3f}%  {'PASS' if ok else 'FAIL'}")

    # 홀드아웃 일반화. 전압은 연속값이라 타이트하게, 온도는 0.1 °C 양자화 때문에 느슨하게 본다
    volt = [f for f in FEATURE_ORDER if f.startswith("V")]
    temp = [f for f in FEATURE_ORDER if f.startswith("T")]
    v_mad = np.array([_mad(stats["holdout"][f]) for f in volt])
    t_mad = np.array([_mad(stats["holdout"][f]) for f in temp])
    ok_v = bool(np.all(np.abs(v_mad - 1.0) < 0.20))
    ok_t = bool(np.all(np.abs(t_mad - 1.0) < 0.60))
    ok_gen = ok_v and ok_t
    print(f"\n  [2] 홀드아웃 일반화 (기준표를 만들 때 쓰지 않은 6팩)")
    print(f"      전압 {volt}: MAD {v_mad.min():.2f}~{v_mad.max():.2f} "
          f"(허용 1±0.20)  -> {'PASS' if ok_v else 'FAIL'}")
    print(f"      온도 {temp}: MAD {t_mad.min():.2f}~{t_mad.max():.2f} "
          f"(허용 1±0.60)  -> {'PASS' if ok_t else 'FAIL'}")
    if t_mad.min() < 0.8:
        print("      [주의] 온도는 분해능 0.1 °C 양자화라 구간 MAD 가 1 LSB 단위로 튄다. "
              "T3(|T01-T02|)는 대부분 0 이라 z 척도가 거칠다")

    # SOC 의존 제거 확인: 저SOC/고SOC 구간의 z 분산이 비슷해야 한다
    #   원신호(V1)는 고SOC 에서 5배 넓어진다. 정규화 후에도 그 비율이 남아 있으면
    #   기준표가 SOC 의존성을 못 걷어낸 것이다.
    lo_hi = []
    for pid in man["holdout"]:
        c = s3.load_cache(pid, mode)
        z = normalize(s3.build_features(c), ref, c["soc"])["V1"]
        soc = c["soc"]
        m_lo, m_hi = soc < 45, soc > 80
        if m_lo.any() and m_hi.any():
            lo_hi.append((_mad(z[m_lo].ravel()), _mad(z[m_hi].ravel())))
    lo_hi = np.array(lo_hi)
    ratio = float(np.median(lo_hi[:, 1] / lo_hi[:, 0]))    # 1 에 가까울수록 잘 제거됨
    ok_soc = 0.6 <= ratio <= 1.6
    print(f"\n  [3] SOC 의존성 제거 (V1): MAD(고SOC)/MAD(저SOC) = {ratio:.2f} "
          f"(정규화 전 원신호는 5배)  -> {'PASS' if ok_soc else 'FAIL'}")

    # [4] 차원 내역을 찍어 STEP 6 의 입력 크기(784)를 눈으로 확인할 수 있게 한다
    print(f"\n  [4] 모델 입력 차원: {N_DIM} = " +
          " + ".join(f"{f} {FEATURE_WIDTH[f]}" for f in FEATURE_ORDER))
    print("=" * 78)
    return ok_scale and ok_gen and ok_soc


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 5. 정규화")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    args = ap.parse_args()
    print("STEP 5 정규화 (SOC 구간별 로버스트 Z-score)")
    # STEP 5 는 저장하는 산출물이 없다(정규화는 함수로만 제공). 검증만 수행한다
    return 0 if verify(args.mode) else 1


if __name__ == "__main__":
    raise SystemExit(main())
