"""데이터 접근 계층.

sensor generator 가 Kafka 로 발행할 측정 이력을 읽어오는 것만 담당한다.

    Postgres  --이 파일-->  sensor generator  -->  Kafka  -->  api

pack_measurement 에는 원본 CSV 가 손대지 않은 상태로 들어 있다(480,949행).
배제 규칙·통전 필터·5초 정규화는 적재가 아니라 여기, 읽는 시점에 적용한다.
기준이 바뀌어도 600MB 를 다시 적재하지 않아도 되게 하려는 것이다.

    480,949행  원본
      -배제    센티넬·불완전 행·1043·방전 전량(EXCLUDE_DCHG)
      -정지    |current| <= 1.0 A 인 행 (CURRENT_ON_AMPS)
      ÷5       5초 구간마다 첫 행만 (RESAMPLE_SECONDS)
     38,058건  Kafka 로 나가는 측정 메시지 (충전만)

원본은 샘플링 주기가 두 가지로 섞여 있다(1초 파일 72개, 5초 파일 30개).
그대로 발행하면 팩마다 초당 메시지 수가 5배 차이 나므로, 읽어올 때 5초
간격으로 통일한다. 자세한 방식은 RESAMPLE_SECONDS 주석 참고.

원본 50팩과는 별개로 발표용 합성 팩 9개(DEMO01~09)가 같은 테이블에 들어
있다. 기본 스트림에서는 빠지고, demo=True 로 부를 때만 나온다. DEMO_SERIALS
주석 참고.
"""
from collections.abc import Generator
from sqlalchemy import create_engine, text
import pandas as pd
import os

# 이 프로젝트에 깔린 드라이버는 psycopg3 다. SQLAlchemy 는 postgresql:// 를
# 보면 psycopg2 를 찾으므로, 드라이버를 명시하지 않으면 ModuleNotFoundError 가
# 난다. docker-compose 가 넣어 주는 DATABASE_URL 도 이 형태라 여기서 바꿔 준다.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/appdb")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DATABASE_URL)

# 2026-08-30 정리: SessionLocal / get_db / check_db_connection 을 지웠다.
# FastAPI 에 ORM 을 붙이려던 초기 골격인데, 이 파일의 실제 소비자(generator 의
# 발행 루프, 분석용 load_measurements)는 전부 engine 을 직접 쓴다. 어디서도
# import 되지 않는 것을 확인하고 걷어냈다 - 되살릴 일이 있으면 git 이력에 있다.

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

# 2026-08-26 결정: 방전은 다루지 않기로 했다. 발행 대상을 충전 50구간으로 줄인다.
#
# 팩 번호를 전부 적는 대신 range 로 둔 이유: 방전 구간의 serial_number 는
# 1000~1050 연속 51개다(실측). 목록을 손으로 나열하면 원본에 팩이 늘었을 때
# 조용히 빠뜨리게 되는데, 범위는 그럴 일이 없다.
#
# 방전을 다시 살리려면 이 집합을 frozenset() 으로 비우면 된다. DB 에는 원본이
# 그대로 있으므로 재적재는 필요 없다 - 그러라고 여기서 거르는 것이다.
EXCLUDE_DCHG = frozenset(range(1000, 1051))

# 발표용 합성 팩. make_demo.py 가 만들어 db/data/DEMO01_chg.csv ~ 로 들어온다.
#
# 원본 50팩은 전부 정상이라 그대로 재생하면 이상 판정이 한 번도 안 나온다.
# 그래서 원본 1000_chg 의 타임라인에 실제 팩의 편차 패턴을 이식하고 그 위에
# 고장을 주입한 팩 9개를 따로 만들었다. 정상 2, 용접불량 2, 센싱와이어불량 2,
# 센서불량 2, 검출한계 미만 1 - 무엇을 심었는지 아는 데이터라 화면의 판정을
# 정답과 대조할 수 있다. 발행 직전에 값을 흔들던 예전 주입 도구와 다른
# 점은 데이터 자체가 고장 팩이라는 것이다 - 상관 구조가 살아 있어 모델이
# 실제로 판정할 수 있고, 그래서 그 도구들은 2026-08-30 에 걷어냈다(old/).
#
# serial 을 9001~9009 로 잡아 원본(1000~1050)과 겹치지 않게 했다. 덕분에
# pack_measurement 에 같이 들어 있어도 serial 만 보고 갈라낼 수 있고,
# EXCLUDE_CHG / EXCLUDE_DCHG 같은 구간 제외 규칙과도 부딪히지 않는다.
#
# 기본 스트림에서 빼는 이유: 합성 데이터가 원본 통계에 섞이면 안 되고,
# 명세서에 적힌 행 수(38,058)도 흔들린다. 데모를 재생할 때만 demo=True 다.
#
# 실측 - 배제 규칙과 통전 필터까지 전부 적용하고 5초로 맞추면 팩당 816건,
# 9팩 합계 7,344건이 나간다(원본 6,009행 -> 통전 4,077행 -> 816건).
# sensor_generator 의 3초 주기로는 팩 하나에 41분, 전체 6시간이다.
DEMO_SERIALS = frozenset(range(9001, 9010))

# 데모 팩의 정답표. make_demo.py 가 함께 뱉는 answer_key.csv 와 같은 내용을
# 코드 쪽에도 둔다 - CSV 는 적재되지 않아 DB 에 없고, 화면이 "이 팩은 무엇을
# 심었는가" 를 보여주려면 어딘가에서 읽어야 하기 때문이다.
#
#   donor    : 편차 패턴을 가져온 실제 팩 번호
#   fault    : 주입한 고장 유형 (None 이면 정상)
#   location : 고장을 심은 자리. 모듈 단위면 M07, 셀 단위면 M05CV06
#   expect   : 모델이 내야 하는 판정
#
# DEMO09 만 expect 가 '정상' 인데 fault 가 있다. 2 mV 는 검출 한계 아래라
# 안 걸리는 것이 맞다 - 임계값이 헐거워지면 여기가 먼저 깨진다는 뜻이라
# 일부러 남겨 둔 경계 사례다.
DEMO_PACKS = {
    9001: dict(pack_id="DEMO01", donor=1013, fault=None,
               location="",        magnitude="",       expect="정상"),
    9002: dict(pack_id="DEMO02", donor=1011, fault=None,
               location="",        magnitude="",       expect="정상"),
    9003: dict(pack_id="DEMO03", donor=1002, fault="weld",
               location="M07",     magnitude="8.0 mV",  expect="용접불량"),
    9004: dict(pack_id="DEMO04", donor=1028, fault="weld",
               location="M12",     magnitude="12.0 mV", expect="용접불량"),
    9005: dict(pack_id="DEMO05", donor=1046, fault="wire",
               location="M05CV06", magnitude="8.0 mV",  expect="센싱와이어불량"),
    9006: dict(pack_id="DEMO06", donor=1029, fault="capacity",
               location="M09CV03", magnitude="25.0 mV", expect="센싱와이어불량"),
    9007: dict(pack_id="DEMO07", donor=1020, fault="sensor_offset",
               location="M01T02",  magnitude="2.5 °C",  expect="센서불량"),
    9008: dict(pack_id="DEMO08", donor=1003, fault="sensor_stuck",
               location="M14T01",  magnitude="stuck",   expect="센서불량"),
    9009: dict(pack_id="DEMO09", donor=1010, fault="weld",
               location="M03",     magnitude="2.0 mV",  expect="정상 (검출한계 미만)"),
}

# 센서 미응답 표시값. 176셀 전압 배열의 실제 최솟값은 3.435V 이고 0 이 한 번도
# 없으므로, 요약 컬럼만 0 / -40 으로 떨어지는 것은 실측이 아니다.
SENTINEL_ZERO = ("voltage", "v_min", "v_max")
SENTINEL_MINUS40 = ("t_min", "t_max", "t_avg")

# 통전 판정 기준 [A]. |current| 가 이보다 커야 발행한다.
#
# 2026-08-26 결정: 비통전 행을 발행하지 않는다. 충전이 멈춘 구간은 모델의 적용
# 범위 밖이라 어차피 판정되지 않고, 발행해 봐야 판정 없는 측정만 쌓인다.
#
# 이 값은 모델이 학습 때 쓴 것과 같아야 한다. pack_loader 가 학습·추론 양쪽에서
# 이 상수를 그대로 import 해서 쓴다 - 숫자를 복제하면 한쪽만 바뀌어도 모른다.
# 실측으로는 0 과 1.0 A 사이의 행이 한 건도 없어서 `<> 0` 과 결과가 같지만,
# 기준을 모델 쪽에 맞춰 둔다 - 새 데이터에 0.5 A 같은 값이 들어와도 양쪽이
# 같은 판단을 하게 하려는 것이다.
#
# 줄어드는 양 (실측, 배제 규칙까지 전부 적용한 뒤):
#     80,313건 -> 38,058건   (42,255건 / 52.6% 감소)
# 3초/건이면 재생 시간이 67시간 -> 32시간이다.
#
# 원본에 0 과 1.0 A 사이인 행은 한 건도 없다. 즉 걸러지는 것은 전부 정확히
# 0 A 인 정지 행이고, 그중 대부분은 충전 완료(SOC 90) 후의 유지 구간이다
# (예: 1018_chg 는 10,454행 중 9,497행이 그 구간이다).
#
# **주의 - 이 필터는 세션 경계 신호를 지운다.** 원본에는 충전 완료 후 20~160분씩
# 정지 구간이 있어서, 그 정지가 이어지는 것을 보면 "충전 세션이 끝났다"고 알 수
# 있었다. 정지 행이 아예 오지 않으면 그 신호가 사라지므로, api 쪽에서
# measured_at 의 공백으로 같은 판단을 한다(detector.SESSION_GAP_SECONDS).
# 한쪽만 바꾸면 안 된다.
CURRENT_ON_AMPS = 1.0

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


def _where(serial_number: int | None, mode: str | None,
           demo: bool = False) -> tuple[str, dict]:
    """배제 규칙 + 통전 구간 + 사용자 조건을 WHERE 절로 만든다.

    DB 는 원시 데이터라 배제 대상이 그대로 살아 있다. 걸러내는 것이 여기 일이다.
    NULL <> 0 은 참이 아니므로, 불완전 행의 NULL 스칼라도 이 조건에서 함께 빠진다.

    demo 는 원본 50팩과 합성 팩 9개 중 어느 쪽을 볼지 고르는 스위치다
    (DEMO_SERIALS 주석 참고). 둘을 섞어 보내는 경우는 없다.
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

    # 통전 구간만 발행한다(CURRENT_ON_AMPS 주석 참고). current 가 NULL 인
    # 불완전 행도 NULL > 1.0 이 참이 아니라 여기서 함께 빠진다.
    clauses.append("abs(current) > :current_on")

    # 회의에서 정한 구간 제외. 팩과 구간을 짝으로 봐야 하므로 mode 별로 나눈다.
    clauses += [
        "NOT (mode = 'chg' AND serial_number = ANY(:exclude_chg))",
        "NOT (mode = 'dchg' AND serial_number = ANY(:exclude_dchg))",
    ]

    params: dict = {
        "excluded": list(EXCLUDED_FILES),
        "exclude_chg": sorted(EXCLUDE_CHG),
        "exclude_dchg": sorted(EXCLUDE_DCHG),
        "current_on": CURRENT_ON_AMPS,
    }
    # 데모 팩과 원본 팩 가르기. 팩을 콕 집어 부른 경우에는 이 구분을 따지지
    # 않는다 - serial 9003 을 달라고 했으면 그건 데모를 달라는 뜻이 분명한데,
    # demo=True 를 같이 안 줬다고 빈 결과를 주면 부르는 쪽만 헷갈린다.
    params["demo_serials"] = sorted(DEMO_SERIALS)
    if serial_number is not None:
        clauses.append("serial_number = :serial_number")
        params["serial_number"] = serial_number
    elif demo:
        clauses.append("serial_number = ANY(:demo_serials)")
    else:
        clauses.append("serial_number <> ALL(:demo_serials)")

    if mode is not None:
        clauses.append("mode = :mode")
        params["mode"] = mode

    return "WHERE " + "\n      AND ".join(clauses), params


def _build_query(serial_number: int | None,
                 mode: str | None,
                 limit: int | None = None,
                 demo: bool = False) -> tuple[str, dict]:
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

    데모 재생(demo=True)에서는 이 정렬이 곧 DEMO01 -> DEMO09 순이다. serial
    9001~9009 를 검사 순서대로 매겨 뒀기 때문에 따로 정렬할 것이 없다.
    """
    where, params = _where(serial_number, mode, demo)
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
                      limit: int | None = None,
                      demo: bool = False) -> pd.DataFrame:
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

    demo=True 면 합성 팩 9개(7,344행)만 나온다. 어느 팩에 무엇을 심었는지는
    DEMO_PACKS 에 있다.
    """
    sql, params = _build_query(serial_number, mode, limit, demo)
    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        raise RuntimeError(
            "조건에 맞는 행이 없습니다. pack_measurement 가 비어 있거나 "
            "(serial_number/mode/demo) 조건이 맞지 않습니다. "
            "적재는 `python load_raw.py` 로 합니다"
            + (" (데모 팩은 db/data/DEMO*_chg.csv 가 있어야 합니다)." if demo
               else ".")
        )
    return df


def iter_measurements(serial_number: int | None = None,
                      mode: str | None = None,
                      batch_size: int = 1000,
                      demo: bool = False) -> Generator[dict, None, None]:
    """측정 이력을 5초 간격으로, 한 행씩 dict 로 흘려보낸다. 발행 루프가 쓴다.

    stream_results 라 서버 사이드 커서로 받는다. 125,488행을 그대로 돌려도
    메모리는 batch_size 만큼만 쓴다.

        for row in iter_measurements(serial_number=1000, mode="chg"):
            producer.produce(topic, key=str(row["serial_number"]), value=...)

    demo=True 면 합성 팩 9개만 DEMO01 -> DEMO09 순으로 흘러나온다(7,344행).
    발표에서 재생하는 것이 이쪽이다.
    """
    sql, params = _build_query(serial_number, mode, demo=demo)
    options = {"stream_results": True, "max_row_buffer": batch_size}

    with engine.connect().execution_options(**options) as conn:
        for row in conn.execute(text(sql), params):
            yield dict(row._mapping)
