"""데이터 접근 계층.

sensor generator 가 Kafka 로 발행할 측정 이력을 읽어오는 것만 담당한다.

    Postgres  --이 파일-->  sensor generator  -->  Kafka  -->  api

pack_measurement 에는 원본 CSV 가 손대지 않은 상태로 들어 있다(480,949행).
배제 규칙과 5초 정규화는 적재가 아니라 여기, 읽는 시점에 적용한다. 걸러낸
결과는 125,488행이다. 기준이
바뀌어도 600MB 를 다시 적재하지 않아도 되게 하려는 것이다.

원본은 샘플링 주기가 두 가지로 섞여 있다(1초 파일 72개, 5초 파일 30개).
그대로 발행하면 팩마다 초당 메시지 수가 5배 차이 나므로, 읽어올 때 5초
간격으로 통일한다. 자세한 방식은 RESAMPLE_SECONDS 주석 참고.
"""
from collections.abc import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd
import os

# 이 프로젝트에 깔린 드라이버는 psycopg3 다. SQLAlchemy 는 postgresql:// 를
# 보면 psycopg2 를 찾으므로, 드라이버를 명시하지 않으면 ModuleNotFoundError 가
# 난다. docker-compose 가 넣어 주는 DATABASE_URL 도 이 형태라 여기서 바꿔 준다.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/appdb")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False,
)

def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def check_db_connection() -> int:
    with engine.connect() as conn:
        result = conn.scalar(text("select 1"))
    return int(result)

# 시간축이 뭉개진 파일. 고유 타임스탬프가 35개뿐이라 시계열로 쓸 수 없다.
EXCLUDED_FILES = frozenset({"1043_dchg.csv"})

# 발행에서 뺄 구간. (팩 번호, 구간) 짝으로 본다.
#
# 2026-08-20 회의: 충전과 방전을 나눠 보내는 방식으로 바꾸면서, 결측 의심
# 구간 34개의 제외는 일단 보류했다. 지금 빠지는 것은 1043 뿐이다 - 시간축이
# 뭉개진 데이터라(1,349행의 고유 타임스탬프가 35개) 시계열로 쓸 수 없다.
# 방전은 EXCLUDED_FILES 가 파일 단위로 이미 걸러내므로 충전만 여기 적는다.
#
# 보류한 34구간의 목록과 판단 근거는 docs/kafka-message-spec.md 8.5 절에
# 그대로 남아 있다. 다시 빼기로 하면 아래 두 집합에 되돌려 넣으면 된다.
#
# DB 에서 지우지 않고 여기서 거르는 이유: pack_measurement 는 원본을 그대로
# 들고 있어야 판단이 바뀌었을 때 목록만 고치면 된다. 이번처럼 되돌리는 일이
# 실제로 생겼고, 600MB 재적재 없이 끝났다.
#
# 이 목록을 고치면 스트림 크기가 바뀌므로, 명세서의 행 수도 함께 갱신한다.
EXCLUDE_CHG = frozenset({
    1043
})
EXCLUDE_DCHG = frozenset({
    
})

# 센서 미응답 표시값. 176셀 전압 배열의 실제 최솟값은 3.435V 이고 0 이 한 번도
# 없으므로, 요약 컬럼만 0 / -40 으로 떨어지는 것은 실측이 아니다.
SENTINEL_ZERO = ("voltage", "v_min", "v_max")
SENTINEL_MINUS40 = ("t_min", "t_max", "t_avg")

# 통일할 샘플링 주기(초).
#
# measured_at 을 5초 단위 구간으로 나누고 각 구간의 첫 행만 남긴다.
#   - 1초 파일: 구간마다 5행 -> 1행 (20% 로 줄어든다)
#   - 5초 파일: 구간마다 1행 -> 그대로 (100% 유지)
# 구간 경계를 epoch 기준으로 잡으므로 두 종류가 같은 눈금을 쓴다.
#
# 남는 행의 measured_at 은 실제 측정 시각 그대로다. 눈금에 맞춰 반올림하지
# 않으므로, 원본에 2초 건너뛴 자리가 있으면 간격이 4초나 6초로 나올 수 있다
# 시각을 조작하는 것보다 낫다고 보고 둔다.
#
# 평균이 아니라 실측 한 행을 고르는 이유: power = voltage * current / 1000,
# dv = (v_max - v_min) * 1000 같은 파생 관계가 한 행 안에서 성립해야 하는데,
# 컬럼별로 평균을 내면 이 관계가 깨진다.
RESAMPLE_SECONDS = 5

# 발행 대상 컬럼. Kafka 메시지의 필드와 1:1 로 대응한다.
COLUMNS = """
    measured_at, serial_number, mode,
    voltage, current, power,
    rsoc_min, rsoc_max, rsoc_avg,
    usoc_min, usoc_max, usoc_avg,
    chg_p_max, dchg_p_max, chg_i_max, dchg_i_max,
    v_min, v_max, dv, cell_voltages,
    t_min, t_max, t_avg, module_temps
"""

# 5초 구간 번호. DISTINCT ON 과 ORDER BY 에 같은 식이 들어가야 한다.
_BUCKET = f"(extract(epoch FROM measured_at)::bigint / {RESAMPLE_SECONDS})"


def _where(serial_number: int | None, mode: str | None) -> tuple[str, dict]:
    """배제 규칙 + 사용자 조건을 WHERE 절로 만든다.

    DB 는 원시 데이터라 배제 대상이 그대로 살아 있다. 걸러내는 것이 여기 일이다.
    NULL <> 0 은 참이 아니므로, 불완전 행의 NULL 스칼라도 이 조건에서 함께 빠진다.
    """
    if mode not in (None, "chg", "dchg"):
        raise ValueError(f"mode 는 'chg' 또는 'dchg' 여야 한다: {mode!r}")

    clauses = [
        "measured_at IS NOT NULL",
        "serial_number IS NOT NULL",
        "source_file <> ALL(:excluded)",
        # 배열 안에 값이 빠진 행(원본에서 중간부터 잘린 5건)
        "array_position(cell_voltages, NULL) IS NULL",
        "array_position(module_temps, NULL) IS NULL",
    ]
    clauses += [f"{col} <> 0" for col in SENTINEL_ZERO]
    clauses += [f"{col} <> -40" for col in SENTINEL_MINUS40]

    # 회의에서 정한 구간 제외. 팩과 구간을 짝으로 봐야 하므로 mode 별로 나눈다.
    clauses += [
        "NOT (mode = 'chg' AND serial_number = ANY(:exclude_chg))",
        "NOT (mode = 'dchg' AND serial_number = ANY(:exclude_dchg))",
    ]

    params: dict = {
        "excluded": list(EXCLUDED_FILES),
        "exclude_chg": sorted(EXCLUDE_CHG),
        "exclude_dchg": sorted(EXCLUDE_DCHG),
    }
    if serial_number is not None:
        clauses.append("serial_number = :serial_number")
        params["serial_number"] = serial_number
    if mode is not None:
        clauses.append("mode = :mode")
        params["mode"] = mode

    return "WHERE " + "\n      AND ".join(clauses), params


def _build_query(serial_number: int | None,
                 mode: str | None,
                 limit: int | None = None) -> tuple[str, dict]:
    """조회 SQL 과 바인드 파라미터를 만든다.

    DISTINCT ON 이 5초 구간마다 첫 행만 남기는 부분이다. ORDER BY 의 앞쪽이
    DISTINCT ON 과 같아야 하고, 마지막 measured_at 이 '구간 안에서 가장 이른
    행' 을 고르게 한다. 이 정렬이 곧 발행 순서이기도 하다 - 같은 팩의 시계열
    순서가 깨지면 결함 판정이 통째로 틀어진다.

    재생 순서는 mode 가 먼저다 - 충전 전량(1000~1050)을 보낸 뒤 방전 전량을
    보낸다. 문자열 정렬로 chg < dchg 라서 ORDER BY 만으로 그 순서가 나온다.
    실제 시험이 충전을 전부 끝낸 뒤 방전으로 넘어가는 방식이라 이를 재현한다.

    측정 시각은 이 순서에서 단조 증가하지 않는다. 충전 구간이 끝나고 방전
    1000번이 시작될 때 measured_at 이 2021-03 에서 2020-08 로 되돌아간다.
    팩 하나만 놓고 보면 여전히 충전 -> 방전 순이므로 시계열은 온전하다.
    """
    where, params = _where(serial_number, mode)
    sql = (
        f"SELECT DISTINCT ON (mode, serial_number, {_BUCKET})\n{COLUMNS}"
        f"FROM pack_measurement\n{where}\n"
        f"ORDER BY mode, serial_number, {_BUCKET}, measured_at"
    )
    if limit is not None:
        sql += "\nLIMIT :limit"
        params["limit"] = limit
    return sql, params


def load_measurements(serial_number: int | None = None,
                      mode: str | None = None,
                      limit: int | None = None) -> pd.DataFrame:
    """Kafka 로 발행할 측정 이력을 5초 간격으로 읽어온다.

    반환 컬럼:
    - measured_at: 측정 시각 (tz-aware)
    - serial_number: 팩 식별자. Kafka 메시지 키로 쓴다
    - mode: 'chg'(충전) | 'dchg'(방전)
    - voltage, current, power: 팩 상태. current 는 음수가 충전이다
    - rsoc_min/max/avg, usoc_min/max/avg: 충전 상태
    - chg_p_max, dchg_p_max, chg_i_max, dchg_i_max: BMS 허용 한계
    - v_min, v_max, dv: 셀 전압 요약. dv 단위만 mV 다
    - cell_voltages: 셀 전압 176개 배열
    - t_min, t_max, t_avg, module_temps: 온도 요약과 32개 배열

    인자를 주지 않으면 125,488행이 나온다. 발행 루프처럼 전량을 훑어야 할
    때는 이쪽 대신 iter_measurements 를 쓴다.
    """
    sql, params = _build_query(serial_number, mode, limit)
    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        raise RuntimeError(
            "조건에 맞는 행이 없습니다. pack_measurement 가 비어 있거나 "
            "(serial_number/mode) 조건이 맞지 않습니다. "
            "적재는 `python load_raw.py` 로 합니다."
        )
    return df


def iter_measurements(serial_number: int | None = None,
                      mode: str | None = None,
                      batch_size: int = 1000) -> Generator[dict, None, None]:
    """측정 이력을 5초 간격으로, 한 행씩 dict 로 흘려보낸다. 발행 루프가 쓴다.

    stream_results 라 서버 사이드 커서로 받는다. 125,488행을 그대로 돌려도
    메모리는 batch_size 만큼만 쓴다.

        for row in iter_measurements(serial_number=1000, mode="chg"):
            producer.produce(topic, key=str(row["serial_number"]), value=...)
    """
    sql, params = _build_query(serial_number, mode)
    options = {"stream_results": True, "max_row_buffer": batch_size}

    with engine.connect().execution_options(**options) as conn:
        for row in conn.execute(text(sql), params):
            yield dict(row._mapping)
