"""정상 팩에 인위적 고장을 주입한다 (가이드 '검증 계획'의 검출력 측정용).

불량 라벨 데이터가 없으므로, 정상 팩에 알려진 크기의 고장을 심어 검출 여부를
측정한다. 주입은 원본 셀 전압/온도 수준에서 하고 계층 분해를 다시 수행하므로,
실제 고장처럼 중심값(모듈 중앙값)도 함께 움직인다.
"""

# 여기서 가장 중요한 설계 결정: "분해된 잔차(cell_res)에 더하지 않고, 원본 전압에
# 더한 뒤 다시 분해한다"는 것.
#   - 잔차에 직접 더하면 중심값(모듈 중앙값)이 그대로라 고장이 100% 잔차로 남는다.
#     실제보다 검출이 쉬워져서 검출력이 과대평가된다.
#   - 원본에 더하고 재분해하면 중심값도 조금 끌려가고, 이웃 셀 잔차도 반응한다.
#     실제 BMS 가 보게 될 신호와 같은 형태다.

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import step2_decompose as s2

N_MOD, N_PM, N_CELLS = s2.N_MODULES, s2.N_CELLS_PER_MODULE, s2.N_CELLS

FAULT_TYPES = (
    "capacity",       # 셀 1개 지속 편차 (음수=저하 / 양수=상승)
    "welding",        # 모듈 11셀 통째
    "sensing_wire",   # 같은 모듈의 연속 셀 n개
    "sensor",         # 온도센서 1개 오프셋
    "spike",          # 셀 1개 순간 스파이크 (duration 행만)
    "multi_cell",     # 서로 떨어진 셀 여러 개 동시
    "temp_gradient",  # 같은 모듈 두 센서를 반대 부호로 (T3 를 직접 벌린다)
)

# 전압 고장인지 온도 고장인지. 전압이면 원본 셀 전압에 더한 뒤 재분해한다
VOLTAGE_KINDS = ("capacity", "welding", "sensing_wire", "spike", "multi_cell")
TEMP_KINDS = ("sensor", "temp_gradient")


def raw_cells(c: dict) -> np.ndarray:
    """캐시 -> 원본 셀 전압 (T, 176)."""
    # STEP 3 캐시는 용량을 줄이려고 원본 전압을 저장하지 않는다.
    # 분해가 가역이므로 세 성분을 다시 더하면 정확히 원본이 복원된다.
    return (c["v_pack"][:, None]
            + np.repeat(c["mod_dev"], N_PM, axis=1)
            + c["cell_res"])


def _redecompose(c: dict, cells: np.ndarray) -> dict:
    # 고장이 섞인 전압으로 STEP 2 분해를 다시 돌려 캐시 사본을 만든다.
    # dict(c) 얕은 복사라 soc/temp 등 손대지 않은 항목은 원본과 공유한다(메모리 절약).
    dec = s2.decompose(cells, pack_id=c.get("pack_id", -1))
    out = dict(c)
    out["v_pack"], out["mod_dev"], out["cell_res"] = dec.v_pack, dec.mod_dev, dec.cell_res
    return out


def fault_profile(n: int, magnitude: float, *, start_frac: float = 0.0,
                  duration: int | None = None, ramp: bool = False) -> tuple[np.ndarray, int]:
    """시간에 따른 고장 크기 곡선 (n,) 과 발생 행 인덱스를 반환한다.

        ramp=False, duration=None  -> 발생 시점부터 끝까지 계단   (급성·지속)
        ramp=True                  -> 0 에서 magnitude 까지 선형  (점진적 열화)
        duration=k                 -> 발생 시점부터 k 행만        (순간 스파이크)

    발생 행을 함께 돌려주는 이유: Detection Delay 는 '고장이 시작된 행'을 알아야
    계산할 수 있는데, 호출부에서 start_frac 을 다시 환산하면 반올림이 어긋난다.
    """
    prof = np.zeros(n)
    a = int(n * start_frac)
    b = n if duration is None else min(n, a + int(duration))
    k = b - a
    if k > 0:
        # linspace 는 마지막 점이 정확히 magnitude 가 되도록 k 개를 채운다
        prof[a:b] = np.linspace(0, magnitude, k) if ramp else magnitude
    return prof, a


def inject(c: dict, kind: str, magnitude: float, *, cell: int = 0,
           module: int = 0, sensor: int = 0, n_cells: int = 2,
           cells_idx: tuple[int, ...] | None = None,
           duration: int | None = None,
           ramp: bool = False, start_frac: float = 0.0) -> dict:
    """고장 1건을 주입한 캐시 사본을 반환한다.

    magnitude 는 전압 고장이면 V(음수=강하, 양수=상승), 온도 고장이면 °C.
    duration 은 고장이 지속되는 행 수 (None 이면 구간 끝까지). spike 용.
    cells_idx 는 multi_cell 에서 동시에 건드릴 셀 인덱스들.

    반환 dict 에 onset_row / fault_meta 를 심어두므로, 지표 계산 쪽에서
    고장 발생 시점을 다시 추측할 필요가 없다.
    """
    if kind not in FAULT_TYPES:
        raise ValueError(f"unknown fault: {kind}")

    n = len(c["soc"])
    prof, onset = fault_profile(n, magnitude, start_frac=start_frac,
                                duration=duration, ramp=ramp)
    meta = {"kind": kind, "magnitude": magnitude, "onset_row": onset,
            "duration": duration, "ramp": ramp,
            "target": target_columns(kind, cell=cell, module=module, sensor=sensor,
                                     n_cells=n_cells, cells_idx=cells_idx)}

    if kind in TEMP_KINDS:
        # 온도 고장은 전압과 무관하므로 재분해 없이 temp 열만 바꾼다
        temp = c["temp"].copy()
        if kind == "sensor":
            temp[:, sensor % 32] += prof     # % 로 감싸 범위 밖 인덱스도 안전하게 받는다
        else:                                # temp_gradient
            # 같은 모듈의 두 센서를 반대 부호로 민다. 모듈 평균은 그대로라
            # T5(모듈 온도)는 안 움직이고 T3(|T01-T02|)만 벌어진다.
            m = module % N_MOD
            temp[:, m * 2] += prof / 2.0
            temp[:, m * 2 + 1] -= prof / 2.0
        out = dict(c)
        out["temp"] = temp
        out["onset_row"], out["fault_meta"] = onset, meta
        return out

    cells = raw_cells(c)
    if kind in ("capacity", "spike"):            # 셀 1개 단독 (지속 / 순간)
        cells[:, cell % N_CELLS] += prof
    elif kind == "welding":                      # 모듈 11셀 통째
        m = module % N_MOD
        cells[:, m * N_PM:(m + 1) * N_PM] += prof[:, None]
    elif kind == "sensing_wire":                 # 같은 모듈의 연속 셀 여러 개
        m, j = divmod(cell % N_CELLS, N_PM)
        j = min(j, N_PM - n_cells)               # 모듈 경계를 넘지 않도록 시작 위치를 당긴다
        cells[:, m * N_PM + j: m * N_PM + j + n_cells] += prof[:, None]
    elif kind == "multi_cell":                   # 서로 떨어진 셀 여러 개 동시
        for idx in (cells_idx or _spread_cells(cell, n_cells)):
            cells[:, idx % N_CELLS] += prof
    out = _redecompose(c, cells)
    out["onset_row"], out["fault_meta"] = onset, meta
    return out


def _spread_cells(start: int, k: int) -> tuple[int, ...]:
    """서로 다른 모듈에 걸치도록 흩어진 셀 인덱스 k 개.

    인접 셀(센싱와이어형)과 구분되어야 하므로 모듈을 건너뛰며 고른다.
    간격 37 은 176 과 서로소라 k 가 커져도 같은 자리로 되돌아오지 않는다.
    """
    return tuple((start + 37 * i) % N_CELLS for i in range(max(1, k)))


def target_columns(kind: str, *, cell: int = 0, module: int = 0,
                   sensor: int = 0, n_cells: int = 2,
                   cells_idx: tuple[int, ...] | None = None) -> list[str]:
    """주입 지점에 해당하는 원본 컬럼 이름 (원인 특정 검증용)."""
    # 여기서 import 하는 이유: step1 -> step2 -> fault_injection 순환 참조를 피하려고
    # 실제로 필요한 시점에만 끌어온다.
    import step1_clean as s1
    if kind == "sensor":
        return [s1.TEMP_COLS[sensor % 32]]
    if kind == "temp_gradient":
        m = module % N_MOD
        return [s1.TEMP_COLS[m * 2], s1.TEMP_COLS[m * 2 + 1]]
    if kind == "welding":
        m = module % N_MOD
        return s1.CELL_COLS[m * N_PM:(m + 1) * N_PM]
    if kind == "sensing_wire":
        # inject() 와 같은 보정 규칙을 그대로 반복해야 실제 주입 위치와 일치한다
        m, j = divmod(cell % N_CELLS, N_PM)
        j = min(j, N_PM - n_cells)
        return s1.CELL_COLS[m * N_PM + j: m * N_PM + j + n_cells]
    if kind == "multi_cell":
        return [s1.CELL_COLS[i % N_CELLS]
                for i in (cells_idx or _spread_cells(cell, n_cells))]
    return [s1.CELL_COLS[cell % N_CELLS]]
