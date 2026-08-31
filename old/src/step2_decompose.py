"""STEP 2. 계층 분해 — docs/battery_guide.md 구간 2 구현.

    V_mod_k  = median(모듈 k의 11개 셀)      # 16개
    V_pack   = median(16개 모듈 중앙값)      # 1개

    cell_dev_i = V_cell_i - V_pack           # 팩 기준 (용량불량용)
    cell_res_i = V_cell_i - V_mod_k(i)       # 모듈 기준 (센싱와이어용)
    mod_dev_k  = V_mod_k  - V_pack           # 모듈 기준 (용접불량용)

검증 항목
    1) 가역성      : V_pack + mod_dev + cell_res == V_cell (176 -> 176)
    2) 분산 비율   : 팩 99.97% / 모듈 0.009% / 셀 0.022%
    3) 주입 실험   : -12 mV 고장 1개에 대해 평균 -13.7/+1.4 mV, 중앙값 -16.0/-0.9 mV

실행:
    python src/step2_decompose.py              # STEP 1 학습 가용 팩 전체
    python src/step2_decompose.py --packs 1000 1001
    python src/step2_decompose.py --save       # 팩별 분해 결과 npz 저장
"""

# 왜 분해하는가:
#   셀 전압의 99.97% 는 "지금 팩이 얼마나 충전됐나"(팩 성분)라서, 원신호를 그대로
#   모델에 넣으면 SOC 만 학습한다. 팩 성분을 빼고 남은 0.03% 안에 고장 신호가 있다.
# 왜 중앙값(median)인가:
#   평균은 고장 셀 자신이 기준값을 끌고 내려가 신호를 1/11 만큼 깎고(희석),
#   나머지 10셀을 반대 부호로 오염시킨다. 중앙값은 고장 1개에 꿈쩍하지 않는다.
#   이 파일의 injection_sweep() 이 그 차이를 수치로 증명한다.

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# src/ 를 import 경로에 넣어 형제 모듈을 패키지 없이 부른다
# (각 STEP 을 단독 스크립트로도 실행할 수 있게 하려는 구조다)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import step1_clean as s1

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

ROOT = s1.ROOT          # 경로 상수는 STEP 1 것을 그대로 재사용한다
OUT_DIR = s1.OUT_DIR

N_MODULES = 16
N_CELLS_PER_MODULE = 11
N_CELLS = N_MODULES * N_CELLS_PER_MODULE  # 176

INJECT_V = -0.012  # V. 가이드 주입 실험 크기 (-12 mV)

# 가이드 기대값
EXP_VAR_RATIO = {"pack": 99.97, "module": 0.009, "cell": 0.022}
EXP_INJECT = {"mean": (-13.7, +1.4), "median": (-16.0, -0.9)}  # (대상 셀, 이웃 10셀) mV


@dataclass
class Decomposition:
    """팩 1개의 계층 분해 결과. 모든 배열의 0축은 시간(초)."""

    # cell_dev 와 cell_res 를 둘 다 들고 있는 이유:
    #   같은 고장이라도 '팩 대비'(cell_dev)와 '모듈 대비'(cell_res) 크기가 다르고,
    #   그 차이가 STEP 8 에서 용량불량과 센싱와이어불량을 가르는 근거가 된다.
    pack_id: int
    v_pack: np.ndarray    # (T,)      팩 중심 전압
    v_mod: np.ndarray     # (T, 16)   모듈 중심 전압
    cell_dev: np.ndarray  # (T, 176)  V1 재료. 팩 기준 편차
    cell_res: np.ndarray  # (T, 176)  모듈 기준 잔차
    mod_dev: np.ndarray   # (T, 16)   V5 재료. 모듈 편차
    center: str = "median"

    @property
    def n_samples(self) -> int:
        return self.v_pack.shape[0]


# ── 중심값 ───────────────────────────────────────────────────────────────────
def _center(a: np.ndarray, axis: int, how: str) -> np.ndarray:
    """중심값 계산. 가이드 기본은 median, 비교 실험용으로 mean 을 허용한다."""
    # how 를 인자로 뺀 덕분에 아래 injection_sweep 이 같은 코드로 두 방식을 비교할 수 있다
    if how == "median":
        return np.median(a, axis=axis)
    if how == "mean":
        return np.mean(a, axis=axis)
    raise ValueError(f"unknown center: {how}")


def as_cell_matrix(seg: pd.DataFrame) -> np.ndarray:
    """정제 구간 DataFrame -> (T, 176) 셀 전압 행렬."""
    # 열 순서는 s1.CELL_COLS(M01CV01...M16CV11) 고정. reshape 이 이 순서에 의존한다
    return seg[s1.CELL_COLS].to_numpy(dtype=float)


# ── 계층 분해 ────────────────────────────────────────────────────────────────
def decompose(cells: np.ndarray, pack_id: int = -1, center: str = "median") -> Decomposition:
    """(T, 176) 셀 전압 -> 팩/모듈/셀 3계층 분해."""
    if cells.ndim != 2 or cells.shape[1] != N_CELLS:
        raise ValueError(f"expected (T, {N_CELLS}) array, got {cells.shape}")

    # (T,176) -> (T,16,11). 열 순서가 모듈 단위로 묶여 있어 단순 reshape 으로 충분하다
    grid = cells.reshape(-1, N_MODULES, N_CELLS_PER_MODULE)
    v_mod = _center(grid, 2, center)                      # (T, 16)  모듈 안 11셀의 중심
    v_pack = _center(v_mod, 1, center)                    # (T,)     모듈 16개의 중심

    # repeat 은 각 모듈 값을 그 모듈의 11개 셀 자리로 펼친다
    # (tile 이 아니라 repeat 이어야 M01 값이 앞 11칸에 붙는다)
    cell_dev = cells - v_pack[:, None]
    cell_res = cells - np.repeat(v_mod, N_CELLS_PER_MODULE, axis=1)
    mod_dev = v_mod - v_pack[:, None]
    return Decomposition(pack_id, v_pack, v_mod, cell_dev, cell_res, mod_dev, center)


def reconstruct(dec: Decomposition) -> np.ndarray:
    """V_pack + mod_dev + cell_res 로 원본 176셀을 되돌린다."""
    # 정의상 항등식이라 오차는 부동소수 반올림 수준(1e-15)이어야 한다.
    # 이 값이 커지면 분해 어딘가에서 정보를 흘린 것이므로 verify 가 이걸 먼저 본다.
    return (dec.v_pack[:, None]
            + np.repeat(dec.mod_dev, N_CELLS_PER_MODULE, axis=1)
            + dec.cell_res)


def variance_shares(dec: Decomposition) -> dict[str, float]:
    """팩/모듈/셀 성분의 분산 기여.

    각 셀 값은 V_pack + mod_dev + cell_res 이므로, 세 성분을 셀 단위 관측으로
    펼쳐 분산을 비교한다 (팩 성분은 176셀 공통, 모듈 성분은 모듈당 11셀 공통).
    """
    var_pack = float(np.var(dec.v_pack))
    # 모듈 성분은 repeat 으로 펼쳐야 '셀 하나당 기여'로 팩 성분과 같은 저울에 올라간다
    var_mod = float(np.var(np.repeat(dec.mod_dev, N_CELLS_PER_MODULE, axis=1)))
    var_cell = float(np.var(dec.cell_res))
    total = var_pack + var_mod + var_cell     # 세 성분이 직교에 가까워 합으로 근사한다
    return {"pack": var_pack, "module": var_mod, "cell": var_cell, "total": total}


# ── 주입 실험 ────────────────────────────────────────────────────────────────
def injection_sweep(cells: np.ndarray, delta: float = INJECT_V,
                    center: str = "median") -> tuple[np.ndarray, np.ndarray]:
    """셀 176개에 각각 delta 를 주입했을 때의 (대상 셀 잔차, 이웃 10셀 잔차 평균).

    잔차는 모듈 기준(cell_res)이며, 시간 평균 후 셀 단위로 반환한다. 단위 V.
    """
    # 176개 셀을 하나씩 고장 내보는 전수 실험이다. 목적은 "평균 대신 중앙값을
    # 써야 하는 이유"를 말이 아니라 수치로 남기는 것.
    grid = cells.reshape(-1, N_MODULES, N_CELLS_PER_MODULE)
    target = np.empty(N_CELLS)      # 주입한 셀 자신의 잔차
    neighbor = np.empty(N_CELLS)    # 같은 모듈 나머지 10셀의 잔차 평균(= 오염 정도)

    for k in range(N_MODULES):
        block = grid[:, k, :]                      # (T, 11)  모듈 하나만 떼어 본다
        for j in range(N_CELLS_PER_MODULE):
            faulty = block.copy()
            faulty[:, j] += delta                  # 셀 j 에만 고장 주입
            ctr = _center(faulty, 1, center)       # (T,)  고장이 섞인 채로 중심 재계산
            res = faulty - ctr[:, None]            # (T, 11)  모듈 기준 잔차
            idx = k * N_CELLS_PER_MODULE + j       # (모듈, 모듈내 셀) -> 전역 셀 번호
            target[idx] = res[:, j].mean()
            neighbor[idx] = np.delete(res, j, axis=1).mean()   # 자기 자신 제외 평균
    return target, neighbor


def baseline_residual(cells: np.ndarray, center: str = "median") -> np.ndarray:
    """주입 전 셀별 모듈 기준 잔차의 시간 평균 (V)."""
    # 주입 결과에서 이 값을 빼면 순수한 주입 효과만 남는다(비교용 기준선)
    dec = decompose(cells, center=center)
    return dec.cell_res.mean(axis=0)


def closest_trial(trials: dict[str, dict[str, np.ndarray]], pack_of: np.ndarray) -> dict:
    """가이드 주입 실험 표(4개 수치)에 가장 가까운 (팩, 셀) 시행을 찾는다.

    가이드 표는 특정 셀 1개를 주입한 단일 시행이므로, 전수 시행 중 어떤 셀이
    그 표를 재현하는지 확인해 수치의 출처를 검증한다.
    """
    # want: 가이드가 적어둔 4개 수치 (평균 대상/이웃, 중앙값 대상/이웃)
    want = np.array([EXP_INJECT["mean"][0], EXP_INJECT["mean"][1],
                     EXP_INJECT["median"][0], EXP_INJECT["median"][1]])
    # got: 전 팩 x 176셀 시행을 같은 4열로 세운 것
    got = np.stack([trials["mean"]["target"], trials["mean"]["neighbor"],
                    trials["median"]["target"], trials["median"]["neighbor"]], axis=1)
    # 4개 수치가 "동시에" 맞아야 하므로 최대 절대오차(무한대 노름)로 거리를 잰다
    dist = np.abs(got - want).max(axis=1)
    i = int(np.argmin(dist))
    module, cell = divmod(i % N_CELLS, N_CELLS_PER_MODULE)   # 전역 셀 번호 -> (모듈, 셀)
    return {
        "pack_id": int(pack_of[i]),
        "cell": s1.CELL_COLS[i % N_CELLS],
        "module": module + 1, "cell_in_module": cell + 1,
        "max_abs_diff_mV": float(dist[i]),
        "mean_target_mV": float(got[i, 0]), "mean_neighbor_mV": float(got[i, 1]),
        "median_target_mV": float(got[i, 2]), "median_neighbor_mV": float(got[i, 3]),
    }


# ── 파이프라인 ───────────────────────────────────────────────────────────────
def load_valid_packs(mode: str = "chg") -> list[int]:
    # STEP 1 이 만든 계약서를 읽는다. 여기서 정한 목록 밖의 팩은 절대 쓰지 않는다
    path = OUT_DIR / f"step1_{mode}_manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"{path} 없음. 먼저 python src/step1_clean.py 를 실행하세요.")
    return json.loads(path.read_text(encoding="utf-8"))["valid"]


def run_step2(packs: list[int] | None = None, mode: str = "chg",
              save: bool = False, verbose: bool = True) -> tuple[pd.DataFrame, dict]:
    ids = packs if packs else load_valid_packs(mode)
    rows: list[dict] = []
    # 주입 실험 결과를 중심값 방식별로 모은다
    inj = {"mean": {"target": [], "neighbor": []},
           "median": {"target": [], "neighbor": []}}

    # 전 팩을 한 번에 메모리에 올리지 않고 분산을 구하기 위한 누적합(개수/합/제곱합).
    # 나중에 E[x^2] - E[x]^2 로 풀링 분산을 복원한다.
    pooled = {k: {"n": 0, "s": 0.0, "ss": 0.0} for k in ("pack", "module", "cell")}
    trial_packs: list[np.ndarray] = []      # 시행마다 어느 팩이었는지 기록

    for pid in ids:
        seg, _ = s1.clean_pack(pid, mode)       # STEP 1 규칙으로 정제한 구간
        cells = as_cell_matrix(seg)
        dec = decompose(cells, pack_id=pid)

        recon_err = float(np.abs(reconstruct(dec) - cells).max())   # 가역성 확인
        share = variance_shares(dec)
        pct = {k: 100.0 * share[k] / share["total"] for k in ("pack", "module", "cell")}

        # 전 팩 풀링 분산 (가이드는 단일 전역 수치를 제시한다)
        for key, arr in (("pack", dec.v_pack),
                         ("module", np.repeat(dec.mod_dev, N_CELLS_PER_MODULE, axis=1)),
                         ("cell", dec.cell_res)):
            pooled[key]["n"] += arr.size
            pooled[key]["s"] += float(arr.sum())
            pooled[key]["ss"] += float(np.square(arr).sum())

        # 같은 팩에 대해 평균/중앙값 두 방식으로 176셀 전수 주입을 돌린다
        for how in ("mean", "median"):
            t, n = injection_sweep(cells, INJECT_V, center=how)
            inj[how]["target"].append(t)
            inj[how]["neighbor"].append(n)
        trial_packs.append(np.full(N_CELLS, pid))

        row = {
            "pack_id": pid, "n_samples": dec.n_samples,
            "recon_err_V": recon_err,
            "var_pack_pct": pct["pack"], "var_mod_pct": pct["module"],
            "var_cell_pct": pct["cell"],
            "v_pack_mean": float(dec.v_pack.mean()),
            # 단위를 mV 로 바꿔 적는다. V 로 두면 표에서 0.000x 만 보인다
            "cell_dev_std_mV": float(dec.cell_dev.std() * 1e3),
            "cell_res_std_mV": float(dec.cell_res.std() * 1e3),
            "mod_dev_std_mV": float(dec.mod_dev.std() * 1e3),
        }
        rows.append(row)
        if verbose:
            print(f"  {pid}  T={dec.n_samples:>5}  recon_err={recon_err:.2e}  "
                  f"var%={pct['pack']:.4f}/{pct['module']:.4f}/{pct['cell']:.4f}  "
                  f"res_std={row['cell_res_std_mV']:.2f}mV", flush=True)

        if save:
            # --save 는 디버깅용 덤프다. 파이프라인 본류는 STEP 3 이 만드는 cache_* 를 쓴다
            d = OUT_DIR / f"decomp_{mode}"
            d.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(d / f"{pid}.npz", v_pack=dec.v_pack.astype(np.float32),
                                v_mod=dec.v_mod.astype(np.float32),
                                cell_dev=dec.cell_dev.astype(np.float32),
                                cell_res=dec.cell_res.astype(np.float32),
                                mod_dev=dec.mod_dev.astype(np.float32),
                                soc=seg["RSOCavg"].to_numpy(dtype=np.float32))

    table = pd.DataFrame(rows)

    # 누적합에서 풀링 분산 복원: Var = E[x^2] - (E[x])^2
    pooled_var = {k: pooled[k]["ss"] / pooled[k]["n"] - (pooled[k]["s"] / pooled[k]["n"]) ** 2
                  for k in pooled}
    tot = sum(pooled_var.values())
    pooled_pct = {k: 100.0 * pooled_var[k] / tot for k in pooled_var}

    # 전 팩 시행을 한 배열로 잇고 mV 로 환산한다
    trials = {how: {kind: np.concatenate(inj[how][kind]) * 1e3 for kind in ("target", "neighbor")}
              for how in ("mean", "median")}
    best = closest_trial(trials, np.concatenate(trial_packs))

    summary = {
        "mode": mode, "n_packs": len(ids), "inject_mV": INJECT_V * 1e3,
        "recon_err_max_V": float(table["recon_err_V"].max()),
        "var_pct_pooled": pooled_pct,
        # 풀링과 팩별 중앙값 둘 다 남긴다. 가이드 수치가 어느 쪽인지 알 수 없어서다
        "var_pct_pack_median": {"pack": float(table["var_pack_pct"].median()),
                                "module": float(table["var_mod_pct"].median()),
                                "cell": float(table["var_cell_pct"].median())},
        "inject": {how: {"target_mV": float(np.median(trials[how]["target"])),
                         "neighbor_mV": float(np.median(trials[how]["neighbor"])),
                         "target_p05_mV": float(np.percentile(trials[how]["target"], 5)),
                         "target_p95_mV": float(np.percentile(trials[how]["target"], 95)),
                         "neighbor_abs_mean_mV": float(np.abs(trials[how]["neighbor"]).mean())}
                   for how in ("mean", "median")},
        "guide_table_match": best,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUT_DIR / f"step2_{mode}_summary.csv", index=False)
    (OUT_DIR / f"step2_{mode}_report.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    return table, summary


def verify(table: pd.DataFrame, summary: dict) -> bool:
    print("\n" + "=" * 78)
    print("STEP 2 검증")
    print("=" * 78)

    # 1) 가역성 — 분해가 정보를 잃지 않았는지. 부동소수 오차 수준이어야 한다
    err = summary["recon_err_max_V"]
    ok_recon = err < 1e-9
    print(f"\n  [1] 가역성 (176 -> 176)")
    print(f"      재구성 최대 오차 {err:.3e} V  -> {'PASS' if ok_recon else 'FAIL'}")

    # 2) 분산 비율 — 팩 성분이 거의 전부라는 사실을 수치로 확인
    pooled, permed = summary["var_pct_pooled"], summary["var_pct_pack_median"]
    tol = {"pack": 0.03, "module": 0.010, "cell": 0.010}
    col = {"pack": "var_pack_pct", "module": "var_mod_pct", "cell": "var_cell_pct"}
    print(f"\n  [2] 분산 비율 (기대: 팩 {EXP_VAR_RATIO['pack']}% / 모듈 {EXP_VAR_RATIO['module']}% "
          f"/ 셀 {EXP_VAR_RATIO['cell']}%)")
    print(f"      {'성분':<7}{'전 팩 풀링':>11}{'팩별 중앙값':>13}{'기대':>9}"
          f"{'허용':>8}{'팩별 최소~최대':>20}  판정")
    ok_each = {}
    for k in ("pack", "module", "cell"):
        # 풀링과 팩별 중앙값 중 하나만 맞아도 통과로 본다(가이드 산출 방식이 불명확해서)
        ok_each[k] = (abs(pooled[k] - EXP_VAR_RATIO[k]) <= tol[k]
                      or abs(permed[k] - EXP_VAR_RATIO[k]) <= tol[k])
        lo, hi = table[col[k]].min(), table[col[k]].max()
        print(f"      {k:<7}{pooled[k]:>10.4f}%{permed[k]:>12.4f}%{EXP_VAR_RATIO[k]:>8.3f}%"
              f"{tol[k]:>7.3f}%p{lo:>10.4f}~{hi:<8.4f}  {'PASS' if ok_each[k] else 'FAIL'}")
    ok_var = all(ok_each.values())

    if not ok_var:
        # 실패했을 때 "어떤 팩을 골라도 기대표에 도달 불가"인지 알려준다.
        # 모듈/셀 분산비의 최소값이 이미 기대치보다 크면 조합 문제가 아니라 표 자체가 다르다.
        ratio = table["var_mod_pct"] / table["var_cell_pct"]
        want = EXP_VAR_RATIO["module"] / EXP_VAR_RATIO["cell"]
        print(f"      모듈/셀 분산비: 팩별 최소 {ratio.min():.2f}, 기대표 {want:.2f}")
        if ratio.min() > want:
            print("      -> 모든 팩이 기대표보다 모듈 성분이 크다. "
                  "어떤 팩 조합을 풀링해도 기대 비율에 도달하지 못한다")

    # 3) 주입 실험 — 중앙값을 쓰는 근거
    m, md = summary["inject"]["mean"], summary["inject"]["median"]
    inject = abs(summary["inject_mV"])
    print(f"\n  [3] 주입 실험 ({summary['inject_mV']:.0f} mV 고장 1개)")
    print(f"      전 팩·전 셀 {summary['n_packs'] * N_CELLS}회 시행의 중앙값")
    print(f"      {'중심값':<9}{'대상 셀 잔차':>14}{'이웃 10셀':>12}{'주입량 회수율':>14}")
    for how, g in (("평균", m), ("중앙값", md)):
        # 회수율 = 잔차에 나타난 크기 / 실제 주입 크기. 100% 에 가까울수록 신호 보존
        print(f"      {how:<8}{g['target_mV']:>11.1f} mV{g['neighbor_mV']:>9.1f} mV"
              f"{100 * abs(g['target_mV']) / inject:>12.1f}%")

    # 아래 5개는 "평균 대신 중앙값" 주장을 쪼갠 검사다. 하나라도 깨지면 전제가 틀린 것
    checks = [
        ("평균은 고장 신호를 약화시킨다 (회수율 < 95%)",
         abs(m["target_mV"]) / inject < 0.95),
        ("중앙값은 주입량을 거의 그대로 회수한다 (>= 95%)",
         abs(md["target_mV"]) / inject >= 0.95),
        ("중앙값 신호가 평균보다 강하다", abs(md["target_mV"]) > abs(m["target_mV"])),
        # 평균 기준 잔차는 합이 0 이라 이웃 10셀이 대상 셀의 -1/10 로 정확히 오염된다.
        # 그 구조적 항등식(neighbor*10 + target ~ 0)까지 확인한다.
        ("평균은 이웃 10셀을 반대 부호로 오염시킨다",
         m["neighbor_mV"] > 0.5 and abs(m["neighbor_mV"] * 10 + m["target_mV"]) < 0.2),
        ("중앙값은 이웃 오염이 평균의 절반 이하다",
         abs(md["neighbor_mV"]) <= 0.5 * abs(m["neighbor_mV"])),
    ]
    print()
    ok_inj = True
    for label, ok in checks:
        ok_inj &= ok
        print(f"      {'PASS' if ok else 'FAIL'}  {label}")

    # 가이드 표의 4개 수치가 어느 시행에서 나온 것인지 역추적한 결과
    b = summary["guide_table_match"]
    ok_match = b["max_abs_diff_mV"] <= 0.5
    print(f"\n      [가이드 원표 재현 시행 탐색] 기대 "
          f"평균({EXP_INJECT['mean'][0]}, {EXP_INJECT['mean'][1]:+}) / "
          f"중앙값({EXP_INJECT['median'][0]}, {EXP_INJECT['median'][1]:+}) mV")
    print(f"      최근접: 팩 {b['pack_id']} {b['cell']}  "
          f"평균({b['mean_target_mV']:.1f}, {b['mean_neighbor_mV']:+.1f}) / "
          f"중앙값({b['median_target_mV']:.1f}, {b['median_neighbor_mV']:+.1f})")
    print(f"      최대 오차 {b['max_abs_diff_mV']:.2f} mV -> "
          f"{'원표와 일치' if ok_match else '원표를 재현하는 셀 없음 (단일 시행 수치로 추정)'}")
    print("=" * 78)
    # ok_match 는 참고용이라 반환값에 넣지 않는다 (가이드 표는 단일 시행 추정치라서)
    return ok_recon and ok_var and ok_inj


def main() -> int:
    ap = argparse.ArgumentParser(description="STEP 2. 계층 분해")
    ap.add_argument("--mode", default="chg", choices=["chg", "dchg"])
    ap.add_argument("--packs", type=int, nargs="*", default=None)
    ap.add_argument("--save", action="store_true", help="팩별 분해 결과를 npz 로 저장")
    ap.add_argument("--report-only", action="store_true",
                    help="이전 실행 결과(outputs/step2_*)를 다시 검증만 한다")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    print(f"STEP 2 계층 분해 — mode={args.mode}")
    if args.report_only:
        # 주입 실험(팩당 176x2회)이 오래 걸려서, 검증 문구만 고칠 때 쓰는 우회로
        table = pd.read_csv(OUT_DIR / f"step2_{args.mode}_summary.csv")
        summary = json.loads(
            (OUT_DIR / f"step2_{args.mode}_report.json").read_text(encoding="utf-8"))
    else:
        table, summary = run_step2(packs=args.packs, mode=args.mode,
                                   save=args.save, verbose=not args.quiet)
    ok = verify(table, summary)
    print(f"\n  -> outputs/step2_{args.mode}_summary.csv")
    print(f"  -> outputs/step2_{args.mode}_report.json")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
