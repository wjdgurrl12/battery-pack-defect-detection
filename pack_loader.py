"""측정 이력 -> battery_anomaly.PackData 변환.

    CSV / DB 행  --이 파일-->  PackData  -->  battery_anomaly.BatteryAnomalyModel

모델은 팩(충전 세션) 하나를 통째로 받아 합/불을 낸다. 행 하나씩 판정하던
옛 모델(old/)과 달리, 세션 전체의 곡선을 SOC 16칸으로 접어서 본다.
그래서 이 파일이 하는 일은 "흩어진 측정 행들을 세션 하나로 모으는 것" 이다.

**학습과 추론이 같은 전처리를 거쳐야 한다.** 여기가 이 파일의 존재 이유다.
학습은 CSV 에서, 추론은 Kafka/DB 행에서 오지만 둘 다 아래 세 규칙을 똑같이
지나야 한다. 한쪽만 어긋나면 예외가 나지 않고 점수만 조용히 틀어진다.

    센티넬 배제   voltage/v_min/v_max == 0, t_min/t_max/t_avg == -40
    통전 구간만   |current| > 1.0 A
    5초 정규화    5초 구간마다 첫 행만

규칙의 근거와 실측치는 database.py 에 적혀 있다. 이 파일은 그 상수를
그대로 가져다 쓴다 - 숫자를 복제하면 한쪽만 바뀌었을 때 알 수 없다.
"""
from __future__ import annotations

import csv
from pathlib import Path

import numpy as np

import database
from battery_anomaly import N_CELL, N_CELL_PER_MODULE, N_MODULE, N_TEMP, PackData

# CSV 컬럼 이름. load_raw.py 가 배열로 접을 때 쓰는 순서와 같아야 한다 -
# 모델은 cell_voltages[m*11 + c] 를 M{m+1}CV{c+1} 로 읽는다.
CELL_COLUMNS = [f"M{m:02d}CV{c:02d}" for m in range(1, 17) for c in range(1, 12)]
TEMP_COLUMNS = [f"M{m:02d}T{s:02d}" for m in range(1, 17) for s in range(1, 3)]

# 판정에 필요한 최소 행 수. SOC 16칸을 의미 있게 채우려면 이보다는 있어야 한다.
# demo_loader 가 쓰던 100행을 그대로 가져왔다.
MIN_ROWS = 100


def build(pack_id: str, soc: np.ndarray, cells: np.ndarray,
          temps: np.ndarray) -> PackData:
    """세션 하나의 원시 배열을 PackData 로 분해한다.

    분해는 옛 모델의 step2_decompose.py 와 같은 항등식이다(old/src/).

        cell = v_pack + mod_dev + cell_res

    중앙값으로 나누는 이유: 고장 난 셀 하나가 평균을 끌고 가면 그 셀이
    기준 안으로 흡수되어 자기 자신을 못 잡는다. 중앙값은 소수의 이탈에
    끌려가지 않는다.
    """
    if not (len(soc) == len(cells) == len(temps)):
        raise ValueError(
            f"{pack_id}: 행 수가 어긋난다 - soc {len(soc)} / cells {len(cells)} / "
            f"temps {len(temps)}. 세 배열은 같은 행에서 나와야 한다")
    if cells.shape[1] != N_CELL or temps.shape[1] != N_TEMP:
        raise ValueError(
            f"{pack_id}: 셀 {cells.shape[1]}개 / 온도 {temps.shape[1]}개가 왔다 "
            f"(기대 {N_CELL} / {N_TEMP})")
    if len(cells) < MIN_ROWS:
        raise ValueError(f"{pack_id}: 통전 구간이 너무 짧다 ({len(cells)}행 < {MIN_ROWS})")

    g = cells.reshape(-1, N_MODULE, N_CELL_PER_MODULE)
    mod_median = np.median(g, axis=2)                       # (T, 16)
    v_pack = np.median(mod_median, axis=1)                  # (T,)

    return PackData(
        pack_id=pack_id,
        soc=soc,
        v_pack=v_pack,
        mod_dev=mod_median - v_pack[:, None],               # (T, 16)
        cell_res=(g - mod_median[:, :, None]).reshape(len(cells), -1),
        temp=temps,
    )


def from_rows(rows: list[dict], pack_id: str | None = None) -> PackData:
    """database.iter_measurements / Kafka 메시지 행들을 PackData 로 모은다.

    database.py 가 이미 센티넬·통전·5초를 걸러 보내므로 여기서 다시 거르지
    않는다. 두 번 거르면 규칙이 두 곳에 생겨 갈라진다.

    rows 는 측정 시각 순이어야 한다. SOC 격자로 접기 때문에 순서가 섞여도
    점수는 같지만, 순서를 지키는 편이 디버깅할 때 읽힌다.
    """
    if not rows:
        raise ValueError("빈 행 목록으로는 팩을 만들 수 없다")
    return build(
        pack_id or str(rows[0]["serial_number"]),
        np.asarray([r["rsoc_avg"] for r in rows], dtype=float),
        np.asarray([r["cell_voltages"] for r in rows], dtype=float),
        np.asarray([r["module_temps"] for r in rows], dtype=float),
    )


def from_csv(path: str | Path) -> PackData:
    """db/data 의 충전 CSV 한 개를 PackData 로 만든다. 학습이 쓰는 경로다.

    DB 를 거치지 않는 대신 database.py 의 규칙을 여기서 재현한다. 학습은
    컨테이너 밖에서도 돌아야 해서 Postgres 를 요구하지 않으려는 것이다.
    복제가 아니라 상수를 import 해서 쓰므로, 규칙이 바뀌면 같이 따라간다.
    """
    path = Path(path)
    soc, cells, temps, seen = [], [], [], set()

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        ix = {name: i for i, name in enumerate(header)}
        missing = [c for c in CELL_COLUMNS + TEMP_COLUMNS + ["RSOCavg", "Current"]
                   if c not in ix]
        if missing:
            raise ValueError(f"{path.name}: 컬럼 없음 {missing[:5]}")

        cell_ix = [ix[c] for c in CELL_COLUMNS]
        temp_ix = [ix[c] for c in TEMP_COLUMNS]

        for row in reader:
            if not any(v.strip() for v in row):
                continue
            try:
                get = lambda c: float(row[ix[c]])           # noqa: E731
                # 센티넬 - 센서 미응답 표시값이지 실측이 아니다
                if any(get(c) == 0 for c in ("Voltage", "Vmin", "Vmax")):
                    continue
                if any(get(c) == -40 for c in ("Tmin", "Tmax", "Tavg")):
                    continue
                # 통전 구간만. 충전이 멈춘 구간은 모델의 적용 범위 밖이다
                if abs(get("Current")) <= database.CURRENT_ON_AMPS:
                    continue
                # 5초 정규화. 구간 경계를 초 단위 epoch 로 잡아 database.py 의
                # extract(epoch)/5 와 같은 눈금을 쓴다
                h, m, s = (int(v) for v in row[ix["Time"]].split(":"))
                bucket = (row[ix["Date"]], (h * 3600 + m * 60 + s) // database.RESAMPLE_SECONDS)
                if bucket in seen:
                    continue
                seen.add(bucket)

                # 셋을 다 만든 뒤에 한꺼번에 붙인다. 하나씩 append 하면
                # 중간에서 ValueError 가 났을 때 앞의 것만 들어가 배열 길이가
                # 어긋난다(원본의 잘린 5건에서 실제로 그랬다).
                one_soc = get("RSOCavg")
                one_cells = [float(row[i]) for i in cell_ix]
                one_temps = [float(row[i]) for i in temp_ix]
            except ValueError:
                # 값이 빠진 행. 지어내지 않고 통째로 버린다
                seen.discard(bucket)
                continue

            soc.append(one_soc)
            cells.append(one_cells)
            temps.append(one_temps)

    return build(path.stem.replace("_chg", ""),
                 np.asarray(soc), np.asarray(cells), np.asarray(temps))


def training_packs(data_dir: str | Path = "db/data") -> list[PackData]:
    """학습용 정상 팩을 모은다.

    데모 팩(9001~9009)은 고장을 심은 데이터라 뺀다 - 정상만 학습하는 모델에
    섞이면 고장이 '정상 범위' 로 들어가 검출력이 떨어진다.
    1043 은 database.EXCLUDE_CHG 가 거르는 팩이라 여기서도 뺀다.
    """
    packs = []
    for path in sorted(Path(data_dir).glob("*_chg.csv")):
        stem = path.stem.replace("_chg", "")
        if not stem.isdigit():           # DEMO01 등
            continue
        if int(stem) in database.EXCLUDE_CHG:
            continue
        packs.append(from_csv(path))
    return packs
