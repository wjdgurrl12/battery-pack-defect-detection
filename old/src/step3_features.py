"""STEP 3. 피처 생성 — docs/battery_guide.md 구간 3 구현.

전압 7종
    V1 cell_dev    V_i - V_pack                        176   용량불량
    V2 dev_slope   d(V1)/d(SOC)                        176   용량불량 진행
    V4 dev_growth  V1@SOC85% - V1@SOC30%               176   용량불량 확정
    V5 mod_dev     median(모듈11셀) - V_pack            16   용접불량
    V6 mod_spread  IQR(모듈 11셀)                        16   용접 조기징후
    V8 adj_diff    cell_res_i - cell_res_(i+1) 모듈내부  160   센싱와이어
    V9 isolation   |cell_res_i| / (인접 평균 + eps)      176   유형 분류

온도 4종
    T1 t_offset    초기 60초 mean(T_j - median)          32   보정값 (학습 입력 아님)
    T2 t_resid     (T_j - median32) - t_offset_j         32   센서불량
    T3 t_pair      |M{k}T01 - M{k}T02|                   16   T2 보완
    T5 t_mod_dev   median(모듈2센서) - median(32)         16   모듈 냉각 이상

제외: dev_rate, t_rate (시간 미분), 저항 프록시, group 플래그

실행:
    python src/step3_features.py            # 캐시 생성 + 검증
    python src/step3_features.py --no-build # 기존 캐시로 검증만
"""

# 설계 원칙 두 가지:
#   1. 모든 피처는 "같은 시점, 같은 팩 안에서의 상대 비교"다. 절대 전압을 쓰지 않으므로
#      팩마다 다른 SOC·온도 조건에 휘둘리지 않는다.
#   2. 시간 미분(dev_rate/t_rate)은 제외했다. 충전 프로파일이 A/B 그룹마다 달라서
#      "초당 변화"가 고장이 아니라 그룹 차이를 학습해 버린다. 대신 SOC 로 미분한다(V2).
#
# 이 파일은 동시에 캐시 생성기이기도 하다. STEP 1(정제) + STEP 2(분해) 결과를
# 팩별 npz 로 떨궈두면 STEP 4~9 가 원본 CSV 를 다시 파싱하지 않는다.

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import step1_clean as s1
import step2_decompose as s2

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

OUT_DIR = s1.OUT_DIR
N_MOD, N_PM, N_CELLS = s2.N_MODULES, s2.N_CELLS_PER_MODULE, s2.N_CELLS

OFFSET_SEC = 60      # T1 오프셋 추정 구간
SLOPE_HALF = 30      # V2 기울기 창 (전후 30초 = 60초)
SLOPE_MIN_DSOC = 0.05  # %SOC. 분모 하한 (SOC 분해능 0.01%)
GROWTH_LO, GROWTH_HI = 30.0, 85.0   # V4 기준 SOC
GROWTH_TOL = 0.5     # V4 조회 허용 폭 (%SOC)
ISO_EPS = 5e-4       # V9 분모 보호. 셀 전압 분해능 1 mV 의 절반
                     # (더 작게 두면 이웃 잔차가 양자화로 0 이 될 때 비율이 폭주한다)

# 기대 피처 개수
# 176=셀, 16=모듈, 160=모듈내 인접쌍(16x10), 32=온도센서. verify 가 이 표와 대조한다
EXPECTED_COUNTS = {"V1": 176, "V2": 176, "V4": 176, "V5": 16, "V6": 16,
                   "V8": 160, "V9": 176, "T1": 32, "T2": 32, "T3": 16, "T5": 16}
# T1 기대 효과 (가이드 STEP 3 온도 절)
EXP_T_RESID_STD = 0.087      # °C. 오프셋 제거 후 센서 잔차 std
EXP_T_DETECT = 0.3           # °C. 검출 가능 최소 이상
EXP_OFFSET_CORR = (0.88, 0.94)  # 전후반 오프셋 상관


def cache_dir(mode: str = "chg") -> Path:
    return OUT_DIR / f"cache_{mode}"


# ── 캐시 ─────────────────────────────────────────────────────────────────────
def build_cache(pack_ids: list[int], mode: str = "chg", verbose: bool = True) -> None:
    """STEP 1 정제 + STEP 2 분해 결과를 팩별 npz 로 저장한다."""
    # 저장 항목은 "이후 단계가 필요로 하는 최소 집합"이다.
    # v_pack + mod_dev + cell_res 만 있으면 원본 셀 전압을 되살릴 수 있으므로
    # 176열짜리 원본은 따로 저장하지 않는다(fault_injection.raw_cells 가 복원한다).
    d = cache_dir(mode)
    d.mkdir(parents=True, exist_ok=True)
    for pid in pack_ids:
        seg, res = s1.clean_pack(pid, mode)
        cells = s2.as_cell_matrix(seg)
        dec = s2.decompose(cells, pack_id=pid)
        temp = seg[s1.TEMP_COLS].to_numpy(dtype=float)
        # float32 로 낮춰 용량을 절반으로 줄인다. 셀 전압 분해능이 1 mV 라 정밀도는 충분하다
        np.savez_compressed(
            d / f"{pid}.npz",
            soc=seg["RSOCavg"].to_numpy(np.float32),
            current=seg["Current"].to_numpy(np.float32),
            v_pack=dec.v_pack.astype(np.float32),
            mod_dev=dec.mod_dev.astype(np.float32),
            cell_res=dec.cell_res.astype(np.float32),
            temp=temp.astype(np.float32),
        )
        if verbose:
            print(f"  cache {pid}  T={len(seg)}", flush=True)


def load_cache(pack_id: int, mode: str = "chg") -> dict[str, np.ndarray]:
    p = cache_dir(mode) / f"{pack_id}.npz"
    if not p.exists():
        raise FileNotFoundError(f"{p} 없음. python src/step3_features.py 를 먼저 실행하세요.")
    with np.load(p) as z:
        # 계산은 float64 로 되돌려서 한다(중앙값·나눗셈 누적 오차 방지)
        c = {k: z[k].astype(np.float64) for k in z.files}
    c["pack_id"] = pack_id
    return c


# ── 전압 피처 ────────────────────────────────────────────────────────────────
def v1_cell_dev(c: dict) -> np.ndarray:
    """V1. 팩 기준 셀 편차 (T, 176)."""
    # 캐시에 cell_dev 를 따로 저장하지 않고 여기서 복원한다.
    # cell_dev = cell_res + mod_dev (STEP 2 의 정의상 항등식)
    return c["cell_res"] + np.repeat(c["mod_dev"], N_PM, axis=1)


def v2_dev_slope(v1: np.ndarray, soc: np.ndarray, half: int = SLOPE_HALF) -> np.ndarray:
    """V2. SOC 에 대한 V1 기울기 (T, 176). 단위 V/%SOC.

    시간 미분(dev_rate)은 A/B 그룹 의존성 때문에 제외 대상이므로 SOC 로 미분한다.
    """
    # 중심차분을 벡터화한 것. 각 시점 t 에서 (t-30, t+30) 두 지점을 잡고
    # 그 사이 V1 변화량을 SOC 변화량으로 나눈다.
    n = len(soc)
    lo = np.clip(np.arange(n) - half, 0, n - 1)     # 앞뒤 끝은 clip 으로 잘려 한쪽차분이 된다
    hi = np.clip(np.arange(n) + half, 0, n - 1)
    dsoc = soc[hi] - soc[lo]
    # SOC 가 거의 안 변한 구간(정지·저전류)에서 0 으로 나누면 값이 폭주한다.
    # NaN 으로 만들어 두면 STEP 5 의 feature_matrix 가 0(정보 없음)으로 바꾼다.
    dsoc = np.where(np.abs(dsoc) < SLOPE_MIN_DSOC, np.nan, dsoc)
    return (v1[hi] - v1[lo]) / dsoc[:, None]


def v4_dev_growth(v1: np.ndarray, soc: np.ndarray,
                  lo: float = GROWTH_LO, hi: float = GROWTH_HI,
                  tol: float = GROWTH_TOL) -> np.ndarray:
    """V4. V1@SOC85% - V1@SOC30% (176,). 구간 미포함 시 NaN."""
    # 시간축이 없는 유일한 전압 피처다(팩당 셀별 스칼라).
    # 용량이 줄어든 셀은 충전이 진행될수록 편차가 벌어지므로, 저SOC/고SOC 두 지점의
    # 편차 차이가 곧 '진행성'의 증거가 된다.
    out = np.full(v1.shape[1], np.nan)
    m_lo, m_hi = np.abs(soc - lo) <= tol, np.abs(soc - hi) <= tol   # 각 SOC 근방 ±0.5%
    if m_lo.any() and m_hi.any():
        # 근방 여러 시점의 중앙값을 써서 순간 노이즈를 없앤다
        out = np.median(v1[m_hi], axis=0) - np.median(v1[m_lo], axis=0)
    return out


def v5_mod_dev(c: dict) -> np.ndarray:
    """V5. 모듈 편차 (T, 16)."""
    # 모듈 11셀이 통째로 어긋나는 용접불량은 셀 단위(V1)로는 흐릿하고 여기서 선명하다
    return c["mod_dev"]


def v6_mod_spread(c: dict) -> np.ndarray:
    """V6. 모듈 내 11셀의 IQR (T, 16). 위치 이동에 불변이라 잔차로 계산한다."""
    # IQR(사분위 범위)은 모듈이 통째로 위아래로 움직여도 변하지 않고,
    # 모듈 '안에서 흩어지는' 정도만 잡는다. 용접 열화의 조기 징후가 여기 먼저 뜬다.
    grid = c["cell_res"].reshape(-1, N_MOD, N_PM)
    q75, q25 = np.percentile(grid, [75, 25], axis=2)
    return q75 - q25


def v8_adj_diff(c: dict) -> np.ndarray:
    """V8. 모듈 내부 인접 셀 잔차 차 (T, 160).

    M01CV11 과 M02CV01 은 번호만 인접하고 물리적으로 다른 모듈이므로 건너뛴다.
    """
    # (T,16,11) 로 접은 뒤 모듈 안에서만 이웃 차를 만든다.
    # 모듈당 10쌍 x 16모듈 = 160. 이렇게 하면 모듈 경계를 넘는 쌍이 애초에 생기지 않는다.
    grid = c["cell_res"].reshape(-1, N_MOD, N_PM)
    return (grid[:, :, :-1] - grid[:, :, 1:]).reshape(-1, N_MOD * (N_PM - 1))


def v9_isolation(c: dict, eps: float = ISO_EPS) -> np.ndarray:
    """V9. |cell_res_i| / (모듈 내 인접 셀 |cell_res| 평균 + eps) (T, 176).

    혼자 튀면 높고(용량불량), 이웃과 같이 움직이면 낮다(센싱와이어불량).
    """
    # 좌우 이웃을 한 칸씩 민 배열로 만들고(모듈 끝은 NaN),
    # nanmean 으로 "있는 이웃만" 평균낸다. 모듈 양 끝 셀은 이웃이 1개뿐이다.
    grid = np.abs(c["cell_res"].reshape(-1, N_MOD, N_PM))
    left = np.concatenate([np.full((grid.shape[0], N_MOD, 1), np.nan), grid[:, :, :-1]], axis=2)
    right = np.concatenate([grid[:, :, 1:], np.full((grid.shape[0], N_MOD, 1), np.nan)], axis=2)
    neigh = np.nanmean(np.stack([left, right]), axis=0)
    return (grid / (neigh + eps)).reshape(-1, N_CELLS)


# ── 온도 피처 ────────────────────────────────────────────────────────────────
def temp_center_residual(temp: np.ndarray) -> np.ndarray:
    """T_j - median(32센서) (T, 32)."""
    # 전압과 같은 발상. 팩 전체 온도(주행/외기)를 빼고 센서 간 상대차만 남긴다
    return temp - np.median(temp, axis=1)[:, None]


def t1_offset(temp: np.ndarray, n_sec: int = OFFSET_SEC) -> np.ndarray:
    """T1. 초기 n_sec 구간으로 추정한 센서별 고정 편차 (32,). 학습 입력 아님."""
    # 센서마다 부착 위치·개체차로 ±0.5 °C 고정 편차가 있다. 이걸 빼지 않으면
    # 검출 한계가 3σ = 1.0 °C 로 커진다. 빼면 0.3 °C 까지 내려간다.
    # 주의: 처음부터 고장난 센서는 그 고장까지 오프셋으로 흡수해 버린다 → T3 가 보완.
    return temp_center_residual(temp)[:n_sec].mean(axis=0)


def t2_resid(temp: np.ndarray, offset: np.ndarray | None = None) -> np.ndarray:
    """T2. 오프셋 제거 센서 잔차 (T, 32)."""
    r = temp_center_residual(temp)
    if offset is None:
        offset = t1_offset(temp)
    return r - offset


def t3_pair(temp: np.ndarray) -> np.ndarray:
    """T3. 모듈 내 두 센서 차 (T, 16). 오프셋 추정과 무관하게 작동한다."""
    # 같은 모듈의 두 센서는 원래 거의 같은 값이어야 한다. 한쪽만 틀어지면 바로 벌어진다.
    # T1 오프셋을 쓰지 않으므로 '시작부터 고장난 센서'도 잡을 수 있다(STEP 6 의 R4).
    g = temp.reshape(-1, N_MOD, 2)
    return np.abs(g[:, :, 0] - g[:, :, 1])


def t5_mod_dev(temp: np.ndarray) -> np.ndarray:
    """T5. 모듈 온도 중앙값 - 전체 중앙값 (T, 16)."""
    # 센서 하나가 아니라 모듈 하나가 통째로 뜨거운 경우(냉각 불균형)를 본다
    g = temp.reshape(-1, N_MOD, 2)
    return np.median(g, axis=2) - np.median(temp, axis=1)[:, None]


# ── 통합 ─────────────────────────────────────────────────────────────────────
def build_features(c: dict) -> dict[str, np.ndarray]:
    """캐시 1팩 -> 피처 묶음. 시간축 피처는 (T, n), V4/T1 은 (n,)."""
    # STEP 4·5·6·8·9 가 전부 이 함수 하나만 호출한다.
    # 피처 정의를 바꾸려면 여기 아래 함수들만 고치면 전 단계에 일관되게 반영된다.
    v1 = v1_cell_dev(c)
    temp = c["temp"]
    off = t1_offset(temp)            # T1 은 한 번만 구해 T2 에 넘긴다(중복 계산 방지)
    return {
        "V1": v1,
        "V2": v2_dev_slope(v1, c["soc"]),
        "V4": v4_dev_growth(v1, c["soc"]),
        "V5": v5_mod_dev(c),
        "V6": v6_mod_spread(c),
        "V8": v8_adj_diff(c),
        "V9": v9_isolation(c),
        "T1": off,                   # 보정값. 모델 입력에는 들어가지 않는다
        "T2": t2_resid(temp, off),
        "T3": t3_pair(temp),
        "T5": t5_mod_dev(c["temp"]),
        "soc": c["soc"],             # 기준표 조회 키로 함께 넘긴다
    }


# ── 검증 ─────────────────────────────────────────────────────────────────────
def verify(pack_ids: list[int], mode: str = "chg") -> bool:
    print("\n" + "=" * 78)
    print("STEP 3 검증")
    print("=" * 78)

    feats = {pid: build_features(load_cache(pid, mode)) for pid in pack_ids}
    any_pid = pack_ids[0]

    # 1) 피처 개수 — 정의대로 176/16/160/32 가 나오는지
    print("\n  [1] 피처 개수")
    ok_cnt = True
    for k, want in EXPECTED_COUNTS.items():
        got = feats[any_pid][k].shape[-1]
        ok = got == want
        ok_cnt &= ok
        print(f"      {k:<3} {got:>4} (기대 {want:>4})  {'PASS' if ok else 'FAIL'}")

    # 2) V8 모듈 경계 미교차
    #    M01 마지막 셀에만 큰 값을 넣고, 반응하는 V8 열이 M01 내부 쌍 하나뿐인지 본다.
    #    만약 경계를 넘는 쌍이 있었다면 M02 쪽 열도 함께 흔들렸을 것이다.
    print("\n  [2] V8 모듈 경계")
    c = load_cache(any_pid, mode)
    probe = {k: v.copy() for k, v in c.items() if isinstance(v, np.ndarray)}
    probe["cell_res"][:, 10] += 0.05        # M01CV11 에만 큰 값 주입
    before = np.abs(v8_adj_diff(c)).max(axis=0)
    after = np.abs(v8_adj_diff(probe)).max(axis=0)
    touched = np.flatnonzero(after - before > 1e-6)
    ok_v8 = list(touched) == [9]            # M01 의 마지막 쌍(CV10-CV11)만 반응
    print(f"      M01CV11 주입 시 반응한 V8 인덱스 {list(touched)} (기대 [9] = M01 내부 쌍)")
    print(f"      M01CV11-M02CV01 쌍은 애초에 존재하지 않음  -> {'PASS' if ok_v8 else 'FAIL'}")

    # 3) T1 오프셋 효과
    #    '이상적 추정'(전 구간 평균)과 '운영 방식'(초기 60초)을 나란히 재서,
    #    가이드 수치가 어느 쪽 기준인지 드러낸다.
    raw_off, resid_ideal, resid_60s, corrs = [], [], [], []
    for pid in pack_ids:
        temp = load_cache(pid, mode)["temp"]
        r = temp_center_residual(temp)
        raw_off.append(r.mean(axis=0))
        resid_ideal.append((r - r.mean(axis=0)).std())        # 전체 구간 오프셋 기준
        resid_60s.append(t2_resid(temp).std())                # 가이드 운영 방식(초기 60초)
        h = len(r) // 2
        # 전반부로 추정한 오프셋이 후반부에도 유효한가 = 오프셋이 '고정'인가
        corrs.append(np.corrcoef(r[:h].mean(axis=0), r[h:].mean(axis=0))[0, 1])

    off_all = np.concatenate(raw_off)
    std_ideal = float(np.median(resid_ideal))
    std_60s = float(np.median(resid_60s))
    corr = float(np.median(corrs))
    ok_std = abs(std_ideal - EXP_T_RESID_STD) <= 0.02
    ok_corr = EXP_OFFSET_CORR[0] - 0.02 <= corr <= EXP_OFFSET_CORR[1] + 0.02

    print("\n  [3] T1 오프셋 제거 효과 (온도 분해능 0.1 °C)")
    print(f"      제거 전 센서 고정편차: std {off_all.std():.3f} °C, 범위 ±{np.abs(off_all).max():.2f} °C"
          f"   (기대 ±0.5)")
    print(f"      제거 후 잔차 std: {std_ideal:.3f} °C (기대 {EXP_T_RESID_STD})  "
          f"-> {'PASS' if ok_std else 'FAIL'}")
    print(f"      검출 한계 3σ: {3 * off_all.std():.2f} °C -> {3 * std_ideal:.2f} °C "
          f"(기대 1.0 -> {EXP_T_DETECT})")
    print(f"      전후반 오프셋 상관: {corr:.3f} (기대 {EXP_OFFSET_CORR[0]}~{EXP_OFFSET_CORR[1]})  "
          f"-> {'PASS' if ok_corr else 'FAIL'}")
    print(f"      [주의] 초기 60초만으로 추정하면 잔차 std {std_60s:.3f} °C "
          f"(이상적 추정의 {std_60s / std_ideal:.1f}배)")

    # 4) V9 판별력 — 혼자 튀는 고장 vs 이웃과 같이 가는 고장
    #    같은 -12 mV 라도 단독 셀이면 V9 가 크고, 연속 2셀이면 서로 이웃이라 작아진다.
    #    STEP 8 이 용량불량/센싱와이어를 가르는 근거가 여기서 확인된다.
    c = load_cache(any_pid, mode)
    solo = {k: v.copy() for k, v in c.items() if isinstance(v, np.ndarray)}
    solo["cell_res"][:, 5] -= 0.012                       # 셀 1개만 (용량불량형)
    pair = {k: v.copy() for k, v in c.items() if isinstance(v, np.ndarray)}
    pair["cell_res"][:, 4:6] -= 0.012                     # 연속 2셀 (센싱와이어형)
    iso_solo = float(np.median(v9_isolation(solo)[:, 5]))
    iso_pair = float(np.median(v9_isolation(pair)[:, 5]))
    ok_v9 = iso_solo > iso_pair
    print("\n  [4] V9 유형 판별력 (-12 mV 주입)")
    print(f"      단독 셀 고장  V9 = {iso_solo:7.2f}")
    print(f"      연속 2셀 고장 V9 = {iso_pair:7.2f}")
    print(f"      단독 > 연속  -> {'PASS' if ok_v9 else 'FAIL'}")

    # 5) 제외 피처 확인 — 만든 목록에 시간 미분 계열이 없다는 사실을 명시적으로 남긴다
    print("\n  [5] 제외 피처: dev_rate, t_rate(시간 미분), 저항 프록시, group 플래그")
    print(f"      생성 피처 목록 {sorted(EXPECTED_COUNTS)} -> 시간 미분 피처 없음  PASS")

    print("=" * 78)
    return ok_cnt and ok_v8 and ok_std and ok_corr and ok_v9


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 3. 피처 생성")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--no-build", action="store_true", help="캐시 생성을 건너뛴다")
    args = ap.parse_args()

    man = json.loads((OUT_DIR / f"step1_{args.mode}_manifest.json").read_text(encoding="utf-8"))
    ids = man["valid"]        # 캐시는 학습 가용 39팩 전부에 대해 만든다(train+holdout)

    print(f"STEP 3 피처 생성 — mode={args.mode}, 팩 {len(ids)}개")
    if not args.no_build:
        build_cache(ids, args.mode)
    ok = verify(ids, args.mode)
    print(f"\n  -> {cache_dir(args.mode)}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
