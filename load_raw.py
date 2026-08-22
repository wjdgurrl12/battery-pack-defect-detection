"""db/data/*.csv 를 Postgres 의 pack_measurement 에 원시 그대로 적재한다.

    docker compose exec dev python load_raw.py              # 전체 적재
    docker compose exec dev python load_raw.py --sync       # 바뀐 파일만 맞춤
    docker compose exec dev python load_raw.py --truncate   # 비우고 다시
    docker compose exec dev python load_raw.py --pattern '10[0-2]*.csv' --replace

배제 규칙(sentinel, 1043_dchg, 중복 타임스탬프)은 여기서 적용하지 않는다.
DB 는 원본을 그대로 들고 있고, 정리는 읽는 쪽이 한다. 기준이 바뀌어도
600MB 를 다시 적재하지 않아도 되게 하려는 것이다.

값을 손대는 곳은 세 군데뿐이며, 전부 구조 변환이지 필터가 아니다.
  - Date + Time      -> measured_at (KST)
  - 파일명            -> mode (CSV 안에 없는 정보다)
  - 176 / 32개 컬럼   -> 배열 2개
"""

import argparse
import csv
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "db" / "data"
SCHEMA_SQL = PROJECT_ROOT / "db" / "init" / "01_schema.sql"

# 원본 Date/Time 에 시간대가 없다. KST 는 1988년 이후 서머타임이 없어
# 고정 오프셋으로 맞다(zoneinfo 없이도 동작한다).
KST = timezone(timedelta(hours=9))

# CSV 헤더명 -> 테이블 컬럼명. 나머지 208개는 배열로 접힌다.
SCALARS = {
    "Voltage": "voltage", "Current": "current", "Power": "power", "SOH": "soh",
    "RSOCmin": "rsoc_min", "RSOCmax": "rsoc_max", "RSOCavg": "rsoc_avg",
    "USOCmin": "usoc_min", "USOCmax": "usoc_max", "USOCavg": "usoc_avg",
    "ChgPmax": "chg_p_max", "DchgPmax": "dchg_p_max",
    "ChgImax": "chg_i_max", "DchgImax": "dchg_i_max",
    "Vmin": "v_min", "Vmax": "v_max", "DV": "dv",
    "Tmin": "t_min", "Tmax": "t_max", "Tavg": "t_avg",
}

COPY_COLUMNS = (["serial_number", "measured_at", "mode", "source_file"]
                + list(SCALARS.values()) + ["cell_voltages", "module_temps"])

CELL_COLUMNS = [f"M{m:02d}CV{c:02d}" for m in range(1, 17) for c in range(1, 12)]
TEMP_COLUMNS = [f"M{m:02d}T{s:02d}" for m in range(1, 17) for s in range(1, 3)]


def connect() -> psycopg.Connection:
    """docker-compose 가 넣어 주는 DATABASE_URL 로 붙는다."""
    import os
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL 이 없습니다. 컨테이너 안에서 실행하세요:\n"
                 "  docker compose exec dev python load_raw.py")
    return psycopg.connect(url)


def ensure_schema(conn: psycopg.Connection) -> None:
    """테이블이 없으면 01_schema.sql 을 적용한다.

    db/init 은 볼륨이 비어 있을 때만 도는데, 이미 Postgres 를 띄운 뒤라면
    그 기회가 지나갔다. 그때 down -v 를 강요하지 않으려고 여기서 한 번 더 본다.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('pack_measurement')")
        if cur.fetchone()[0] is None:
            print(f"pack_measurement 가 없어 {SCHEMA_SQL.name} 를 적용합니다.")
            cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
            conn.commit()


def parse_number(text: str) -> float | None:
    """빈 칸은 NULL 로 둔다. 원시 적재라 값을 지어내지 않는다."""
    text = text.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def read_rows(path: Path):
    """CSV 한 파일을 COPY 에 넣을 튜플로 바꿔 흘려보낸다."""
    mode = "chg" if path.stem.endswith("_chg") else "dchg"

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        ix = {name: i for i, name in enumerate(header)}
        cell_ix = [ix[c] for c in CELL_COLUMNS]
        temp_ix = [ix[c] for c in TEMP_COLUMNS]
        scalar_ix = [ix[c] for c in SCALARS]

        skipped = 0
        for row in reader:
            # 231칸이 전부 빈 행은 담을 내용이 없다(1050_chg.csv 뒤쪽 1,690행).
            if not any(v.strip() for v in row):
                skipped += 1
                continue

            date, clock = row[ix["Date"]].strip(), row[ix["Time"]].strip()
            measured_at = None
            if date and clock:
                measured_at = datetime.strptime(f"{date} {clock}",
                                                "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)

            serial = parse_number(row[ix["SerialNumber"]])

            yield (
                int(serial) if serial is not None else None,
                measured_at,
                mode,
                path.name,
                *[parse_number(row[i]) for i in scalar_ix],
                [parse_number(row[i]) for i in cell_ix],
                [parse_number(row[i]) for i in temp_ix],
            )

        if skipped:
            print(f"    빈 행 {skipped:,}건은 건너뜀")


def load_file(conn: psycopg.Connection, path: Path) -> int:
    """파일 하나를 COPY 로 밀어 넣고 적재된 행 수를 돌려준다."""
    columns = ", ".join(COPY_COLUMNS)
    rows = 0
    with conn.cursor() as cur:
        with cur.copy(f"COPY pack_measurement ({columns}) FROM STDIN") as copy:
            for record in read_rows(path):
                copy.write_row(record)
                rows += 1
    return rows


def csv_fingerprint(path: Path) -> tuple:
    """CSV 한 파일의 지문: (행 수, serial 목록, 최초 시각, 최종 시각).

    600MB 를 매번 해시하는 대신 이 네 가지만 본다. serial 수정이나 행
    추가/삭제는 여기서 걸린다. 값 하나만 슬쩍 바뀐 경우는 못 잡으므로,
    그럴 때는 --replace 로 해당 파일을 지정해 다시 넣는다.
    """
    rows = 0
    serials = set()
    first = last = None

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        ix = {name: i for i, name in enumerate(header)}
        for row in reader:
            if not any(v.strip() for v in row):
                continue
            rows += 1
            serial = row[ix["SerialNumber"]].strip()
            if serial:
                serials.add(int(float(serial)))
            date, clock = row[ix["Date"]].strip(), row[ix["Time"]].strip()
            if date and clock:
                at = datetime.strptime(f"{date} {clock}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                first = at if first is None else min(first, at)
                last = at if last is None else max(last, at)

    return rows, sorted(serials), first, last


def db_fingerprints(conn: psycopg.Connection) -> dict[str, tuple]:
    """적재된 파일들의 지문을 한 번에 가져온다."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_file,
                   count(*),
                   array_agg(DISTINCT serial_number ORDER BY serial_number)
                       FILTER (WHERE serial_number IS NOT NULL),
                   min(measured_at), max(measured_at)
            FROM pack_measurement
            GROUP BY source_file
        """)
        return {name: (rows, serials or [], first, last)
                for name, rows, serials, first, last in cur.fetchall()}


def delete_file(conn: psycopg.Connection, name: str) -> int:
    """한 파일에서 온 행을 전부 지운다. 다시 넣기 전 단계다."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pack_measurement WHERE source_file = %s", (name,))
        return cur.rowcount


def plan_sync(conn: psycopg.Connection, paths: list[Path]) -> tuple[list[Path], list[str]]:
    """무엇을 다시 넣고 무엇을 지울지 정한다.

    돌려주는 것: (다시 적재할 CSV 목록, DB 에만 남아 있어 지울 파일명 목록)
    """
    fingerprints = db_fingerprints(conn)
    stale, seen = [], set()

    for path in paths:
        seen.add(path.name)
        if path.name not in fingerprints:
            print(f"  + {path.name:18s} DB 에 없음")
            stale.append(path)
            continue

        csv_fp = csv_fingerprint(path)
        db_fp = fingerprints[path.name]
        if csv_fp != db_fp:
            reasons = []
            if csv_fp[0] != db_fp[0]:
                reasons.append(f"행 {db_fp[0]:,} -> {csv_fp[0]:,}")
            if csv_fp[1] != db_fp[1]:
                reasons.append(f"serial {db_fp[1]} -> {csv_fp[1]}")
            if csv_fp[2:] != db_fp[2:]:
                reasons.append("측정 기간")
            print(f"  ~ {path.name:18s} {', '.join(reasons)}")
            stale.append(path)

    orphans = sorted(set(fingerprints) - seen)
    for name in orphans:
        print(f"  - {name:18s} CSV 가 없어짐")

    return stale, orphans


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sync", action="store_true",
                        help="CSV 와 DB 를 비교해 달라진 파일만 다시 적재한다")
    parser.add_argument("--replace", action="store_true",
                        help="대상 파일의 기존 행을 지우고 다시 넣는다")
    parser.add_argument("--truncate", action="store_true",
                        help="적재 전에 pack_measurement 를 통째로 비운다")
    parser.add_argument("--pattern", default="*.csv",
                        help="적재할 파일 glob (기본: *.csv)")
    args = parser.parse_args()

    paths = sorted(DATA_DIR.glob(args.pattern))
    if not paths:
        sys.exit(f"{DATA_DIR} 에서 {args.pattern} 에 맞는 파일이 없습니다.")

    conn = connect()
    ensure_schema(conn)

    orphans: list[str] = []
    if args.sync:
        print(f"{len(paths)}개 파일을 DB 와 대조합니다.")
        print()
        paths, orphans = plan_sync(conn, paths)
        if not paths and not orphans:
            print()
            print("달라진 것이 없습니다.")
            conn.close()
            return
        print()

    if args.truncate:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE pack_measurement RESTART IDENTITY")
        conn.commit()
        print("pack_measurement 를 비웠습니다.")

    # CSV 가 사라진 파일은 DB 에서도 내린다
    for name in orphans:
        removed = delete_file(conn, name)
        conn.commit()
        print(f"  - {name:18s} {removed:7,d}행 삭제")

    if paths:
        print(f"{len(paths)}개 파일 적재 시작")
        print()
    total = 0
    started = time.time()

    for n, path in enumerate(paths, 1):
        t0 = time.time()
        # 같은 파일을 두 번 넣으면 행이 겹쳐 쌓인다. 먼저 지우고 넣는다.
        removed = 0
        if args.replace or args.sync:
            removed = delete_file(conn, path.name)

        rows = load_file(conn, path)
        conn.commit()          # 파일 단위로 커밋. 중간에 죽어도 여기까지는 남는다
        total += rows

        note = f"  (기존 {removed:,}행 교체)" if removed else ""
        print(f"  [{n:3d}/{len(paths)}] {path.name:18s} {rows:7,d}행  "
              f"{time.time() - t0:5.1f}s{note}")

    elapsed = time.time() - started
    if total:
        print()
        print(f"총 {total:,}행 / {elapsed:.1f}초 ({total / elapsed:,.0f} 행/초)")

    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT serial_number), "
                    "min(measured_at), max(measured_at) FROM pack_measurement")
        count, serials, first, last = cur.fetchone()
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('pack_measurement'))")
        size = cur.fetchone()[0]

    print(f"테이블: {count:,}행, 팩 {serials}종, {first} ~ {last}, {size}")
    conn.close()


if __name__ == "__main__":
    main()
