"""평가 지표 — DR / False Alarm Rate / Detection Delay.

라벨된 불량 데이터가 없으므로 지표를 두 부류로 엄격히 나눈다.

    실측       FAR  — 주입하지 않은 정상 팩에서 난 알람. 진짜 측정값이다.
    응답 특성  DR, Delay — 우리가 정의한 고장 모형에 대한 반응.
               "실제 불량을 몇 % 잡는다"로 읽으면 안 된다.

시간 단위 주의:
    STEP 1 이 전 팩을 5초/행 격자로 통일했으므로 1행 = 5초다.
    알람 확정에는 PERSIST 행 연속 초과가 필요한데, 고장 발생 행 자체가 첫 초과 행이
    될 수 있으므로 Delay 의 이론적 하한은 (PERSIST - 1) x 5초 = 5초다.
    이보다 작은 값이 나오면 확정 시점을 첫 초과 행으로 잘못 잡은 것이다.

실행:
    python src/metrics.py        # 자체 검증 (합성 신호로 경계 조건 확인)
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

SEC_PER_ROW = 5.0     # STEP 1 의 TARGET_SEC_PER_ROW 와 같아야 한다
PERSIST = 2           # STEP 7 의 PERSIST_SEC 와 같아야 한다 (2행 = 10초)


# ── 알람 구간 ────────────────────────────────────────────────────────────────
def alarm_events(score: np.ndarray, threshold: float,
                 persist: int = PERSIST) -> list[tuple[int, int]]:
    """persist 행 이상 연속 초과한 구간 [(start, end), ...]. end 는 미포함."""
    # STEP 1 의 longest_active_run, STEP 7 의 find_alarms 와 같은 엣지 탐색 관용구다.
    over = score > threshold
    if not over.any():
        return []
    padded = np.concatenate(([False], over, [False]))
    edges = np.flatnonzero(padded[1:] != padded[:-1])
    return [(int(a), int(b)) for a, b in zip(edges[0::2], edges[1::2])
            if b - a >= persist]


def first_confirm_row(score: np.ndarray, threshold: float,
                      persist: int = PERSIST, after: int = 0) -> int | None:
    """after 행 이후 알람이 '확정되는' 행. 없으면 None.

    확정 시점은 연속 초과가 persist 번째에 도달한 행이다. 첫 초과 행이 아니다 —
    그 시점에는 아직 알람이 아니기 때문이다. Delay 를 첫 초과 행으로 재면
    실제보다 (persist-1) 행만큼 짧게 나온다.
    """
    over = score > threshold
    run = 0
    for i in range(max(0, after), len(over)):
        run = run + 1 if over[i] else 0
        if run >= persist:
            return i
    return None


# ── 지표 1. False Alarm Rate ─────────────────────────────────────────────────
def false_alarm(score: np.ndarray, threshold: float, persist: int = PERSIST,
                sec_per_row: float = SEC_PER_ROW) -> dict:
    """주입하지 않은 정상 팩에서 잰다. 이 파일에서 유일한 '실측' 지표다."""
    ev = alarm_events(score, threshold, persist)
    n_rows = len(score)
    alarm_rows = sum(b - a for a, b in ev)
    hours = n_rows * sec_per_row / 3600.0
    return {
        "n_alarm": len(ev),                     # 알람 건수
        "alarm_rows": alarm_rows,               # 알람이 지속된 행 수
        "n_rows": n_rows,
        "hours": hours,
        "per_hour": len(ev) / hours if hours else float("nan"),
        "alarm_time_pct": 100.0 * alarm_rows / n_rows if n_rows else 0.0,
        "fired": bool(ev),                      # 팩 단위 발생 여부
    }


def far_summary(fold_rows: pd.DataFrame, prefix: str = "fa") -> dict:
    """폴드별 FAR 행을 모아 전체 오탐률을 낸다.

    '건/시간'과 '팩 단위 발생률'을 함께 낸다. 후자가 현장에서 더 중요하다 —
    한 팩에서 8건이 몰려 나는 것과 8개 팩에서 1건씩 나는 것은 전혀 다른 상황이다.
    """
    n_alarm = int(fold_rows[f"{prefix}_n_alarm"].sum())
    hours = float(fold_rows["hours"].sum())
    fired = int(fold_rows[f"{prefix}_fired"].sum())
    return {
        "n_alarm": n_alarm,
        "n_packs_fired": fired,
        "n_packs": len(fold_rows),
        "pack_fire_rate": fired / len(fold_rows) if len(fold_rows) else float("nan"),
        "hours": hours,
        "per_hour": n_alarm / hours if hours else float("nan"),
    }


# ── 지표 2·3. Detection Rate / Delay ─────────────────────────────────────────
def detection(score: np.ndarray, threshold: float, onset_row: int,
              persist: int = PERSIST, sec_per_row: float = SEC_PER_ROW,
              window_rows: int | None = None) -> dict:
    """주입 1건에 대한 검출 여부와 지연.

    onset_row  : 고장이 시작된 행 (fault_injection.inject 가 심어준 값)
    window_rows: 고장 이후 몇 행까지 볼 것인가. None 이면 구간 끝까지.
                 팩마다 길이가 687~963행으로 달라서, 고정하지 않으면 짧은 팩이
                 불리해진다(늦게 잡히는 고장을 '미검출'로 세게 된다).
    """
    n = len(score)
    end = n if window_rows is None else min(n, onset_row + int(window_rows))

    # 고장 이전에 이미 울고 있었는가. 그렇다면 이 건의 delay 는 해석 불가다
    pre = first_confirm_row(score[:onset_row], threshold, persist) is not None \
        if onset_row > 0 else False

    row = first_confirm_row(score[:end], threshold, persist, after=onset_row)
    detected = row is not None
    delay_rows = (row - onset_row) if detected else None
    return {
        "detected": detected,
        "confirm_row": row,
        "delay_rows": delay_rows,
        # 미검출이면 지연은 '정의되지 않음'이다. 0 이나 큰 값으로 메우면 안 된다
        "delay_sec": delay_rows * sec_per_row if detected else float("nan"),
        "pre_existing_alarm": pre,
        "onset_row": onset_row,
        "window_rows": end - onset_row,
    }


def dr_curve(trials: pd.DataFrame, group_cols=("scenario", "magnitude"),
             hit_col: str = "detected", split_col: str | None = "group") -> pd.DataFrame:
    """시나리오 x 크기 -> 검출률. split_col 이 있으면 그 값별로도 쪼갠다.

    그룹(A/BC)을 뭉치면 안 된다. z 척도가 최대 3배 달라서 "A 100% / BC 0%" 가
    "50%" 로 보인다.
    """
    rows = []
    for key, g in trials.groupby(list(group_cols), dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        row = dict(zip(group_cols, key))
        row["n"] = len(g)
        row["DR"] = float(g[hit_col].mean())
        # 검출된 건에 한해서만 지연 통계를 낸다
        det = g[g[hit_col]]
        row["delay_med_sec"] = float(det["delay_sec"].median()) if len(det) else float("nan")
        row["delay_p90_sec"] = float(det["delay_sec"].quantile(0.9)) if len(det) else float("nan")
        row["n_pre_alarm"] = int(g.get("pre_existing_alarm", pd.Series(dtype=bool)).sum())
        if split_col and split_col in g.columns:
            for v in sorted(g[split_col].dropna().unique()):
                sub = g[g[split_col] == v]
                row[f"DR_{v}"] = float(sub[hit_col].mean())
                row[f"n_{v}"] = len(sub)
        rows.append(row)
    out = pd.DataFrame(rows)
    sort = [c for c in group_cols]
    return out.sort_values(sort).reset_index(drop=True)


def lod(curve: pd.DataFrame, scenario: str, target: float = 0.5,
        col: str = "DR", mag_col: str = "magnitude") -> float:
    """검출률이 target 에 도달하는 최소 고장 크기 (선형보간). 없으면 NaN.

    LOD = Limit of Detection. 크기의 '절댓값' 오름차순으로 보간한다 —
    전압 고장은 음수(강하)와 양수(상승)가 섞여 있어 부호를 그대로 쓰면 어긋난다.
    """
    g = curve[(curve["scenario"] == scenario) & curve[col].notna()].copy()
    g = g[g[mag_col] != 0]
    if g.empty:
        return float("nan")
    g["absmag"] = g[mag_col].abs()
    g = g.sort_values("absmag")
    x, y = g["absmag"].to_numpy(float), g[col].to_numpy(float)
    hit = np.flatnonzero(y >= target)
    if not hit.size:
        return float("nan")           # 최대 크기에서도 target 미달
    j = hit[0]
    if j == 0 or y[j] == y[j - 1]:
        return float(x[j])
    return float(x[j - 1] + (target - y[j - 1]) * (x[j] - x[j - 1]) / (y[j] - y[j - 1]))


# ── 자체 검증 ────────────────────────────────────────────────────────────────
def verify() -> bool:
    """합성 신호로 경계 조건을 확인한다. 지표 구현의 단위 테스트다."""
    print("=" * 74)
    print("metrics 자체 검증")
    print("=" * 74)
    ok = True
    thr = 10.0

    print(f"\n  [1] 알람 확정 조건 (persist={PERSIST}행)")
    cases = [
        ("1행만 초과",        [0, 20, 0, 0, 0, 0], 0),
        ("2행 연속",          [0, 20, 20, 0, 0, 0], 1),
        ("3행 연속",          [0, 20, 20, 20, 0, 0], 1),
        ("떨어져서 2행",       [20, 0, 20, 0, 0, 0], 0),
        ("두 덩어리",         [20, 20, 0, 20, 20, 0], 2),
    ]
    for name, v, want in cases:
        got = len(alarm_events(np.array(v, float), thr))
        good = got == want
        ok &= good
        print(f"      {name:<14} 알람 {got}건 (기대 {want})  {'PASS' if good else 'FAIL'}")

    print("\n  [2] Delay 는 '확정 행 − 발생 행'  (첫 초과 행이 아니다)")
    #              행:  0  1  2   3   4   5
    s = np.array([0, 0, 0, 20, 20, 20], float)
    d = detection(s, thr, onset_row=2)
    #  발생 2행, 3행에서 첫 초과, 4행에서 확정 -> delay 2행 = 10초
    good = d["confirm_row"] == 4 and d["delay_rows"] == 2 and d["delay_sec"] == 10.0
    ok &= good
    print(f"      발생 2행 / 첫 초과 3행 / 확정 {d['confirm_row']}행"
          f" -> delay {d['delay_rows']}행 = {d['delay_sec']:.0f}초  "
          f"{'PASS' if good else 'FAIL'}")

    print("\n  [3] 이론적 최소 지연 = (persist-1) x 5초")
    #   발생 행이 곧 첫 초과 행이면 확정은 그 다음 행이다 -> 1행 = 5초
    s = np.array([0, 20, 20, 20], float)
    d = detection(s, thr, onset_row=1)
    lo = (PERSIST - 1) * SEC_PER_ROW
    good = d["delay_sec"] == lo
    ok &= good
    print(f"      발생 즉시 초과 -> delay {d['delay_sec']:.0f}초 "
          f"(하한 {lo:.0f}초)  {'PASS' if good else 'FAIL'}")

    print("\n  [4] 미검출이면 지연은 NaN (0 으로 메우지 않는다)")
    d = detection(np.zeros(10), thr, onset_row=3)
    good = (not d["detected"]) and np.isnan(d["delay_sec"])
    ok &= good
    print(f"      detected={d['detected']}  delay_sec={d['delay_sec']}  "
          f"{'PASS' if good else 'FAIL'}")

    print("\n  [5] 고장 이전 알람은 따로 표시한다")
    s = np.array([20, 20, 0, 0, 20, 20], float)
    d = detection(s, thr, onset_row=3)
    good = d["pre_existing_alarm"] is True and d["detected"]
    ok &= good
    print(f"      pre_existing_alarm={d['pre_existing_alarm']}  "
          f"delay={d['delay_sec']:.0f}초  {'PASS' if good else 'FAIL'}")

    print("\n  [6] 관측 창 제한")
    s = np.zeros(100); s[80:] = 20
    near = detection(s, thr, onset_row=10, window_rows=20)
    far = detection(s, thr, onset_row=10)
    good = (not near["detected"]) and far["detected"]
    ok &= good
    print(f"      창 20행 -> {near['detected']} / 창 없음 -> {far['detected']}  "
          f"{'PASS' if good else 'FAIL'}")

    print("\n  [7] FAR 단위 환산")
    s = np.zeros(720); s[100:104] = 20        # 720행 = 1시간, 알람 1건
    fa = false_alarm(s, thr)
    good = fa["n_alarm"] == 1 and abs(fa["hours"] - 1.0) < 1e-9 and fa["fired"]
    ok &= good
    print(f"      720행 x 5초 = {fa['hours']:.2f}시간, 알람 {fa['n_alarm']}건"
          f" -> {fa['per_hour']:.2f} 건/시간  {'PASS' if good else 'FAIL'}")

    print("\n  [8] LOD 선형보간")
    cur = pd.DataFrame({"scenario": ["x"] * 4,
                        "magnitude": [-0.004, -0.008, -0.012, -0.020],
                        "DR": [0.0, 0.25, 0.75, 1.0]})
    v = lod(cur, "x", 0.5)
    good = abs(v - 0.010) < 1e-9        # 0.008 과 0.012 사이 중간
    ok &= good
    print(f"      DR 0.25@8mV / 0.75@12mV -> LOD50 = {v * 1000:.1f} mV (기대 10.0)  "
          f"{'PASS' if good else 'FAIL'}")

    print("\n" + "=" * 74)
    print(f"  전체: {'PASS' if ok else 'FAIL'}")
    print("=" * 74)
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if verify() else 1)
