-- 배터리 팩 측정 원시 데이터.
--
-- db/data/*.csv 를 가공 없이 그대로 담는다. 배제 규칙(sentinel, 1043_dchg,
-- 빈 행)과 5초 정규화는 여기가 아니라 읽는 쪽에서 적용한다. 기준이 바뀌어도
-- 재적재가 필요 없게 하려는 것이다.
--
-- 이 파일은 Postgres 볼륨이 비어 있을 때 최초 1회만 실행된다. 이미 볼륨이
-- 있다면 `docker compose down -v` 로 지우거나, load_raw.py 가 알아서 적용한다.

CREATE TABLE IF NOT EXISTS pack_measurement (
    id             BIGSERIAL   PRIMARY KEY,

    -- 식별
    serial_number  INTEGER,                -- CSV 의 SerialNumber 컬럼 값 그대로
    measured_at    TIMESTAMPTZ,            -- Date + Time 결합, KST 로 간주
    mode           TEXT        NOT NULL CHECK (mode IN ('chg', 'dchg')),
    source_file    TEXT        NOT NULL,   -- 원본 파일명. 파일명의 팩 번호를 되찾을 때 쓴다

    -- 팩 상태. current 는 음수가 충전이다. soh 는 원본이 전부 0 이라 쓸 수 없다.
    voltage        REAL,                   -- V
    current        REAL,                   -- A
    power          REAL,                   -- kW
    soh            REAL,                   -- %

    -- 충전 상태. RSOC 는 실제 용량 기준, USOC 는 사용자 표시용.
    rsoc_min       REAL,
    rsoc_max       REAL,
    rsoc_avg       REAL,
    usoc_min       REAL,
    usoc_max       REAL,
    usoc_avg       REAL,

    -- BMS 가 허용하는 충방전 한계
    chg_p_max      REAL,                   -- kW
    dchg_p_max     REAL,                   -- kW
    chg_i_max      REAL,                   -- A
    dchg_i_max     REAL,                   -- A

    -- 셀 전압. dv 만 단위가 mV 다.
    v_min          REAL,                   -- V
    v_max          REAL,                   -- V
    dv             REAL,                   -- mV
    cell_voltages  REAL[],                 -- 176개. M01CV01 ~ M16CV11 순서

    -- 온도
    t_min          REAL,                   -- C
    t_max          REAL,                   -- C
    t_avg          REAL,                   -- C
    module_temps   REAL[]                  -- 32개. M01T01 ~ M16T02 순서
);

-- 셀 176개를 컬럼으로 펼치지 않고 배열로 둔 이유
--   1. 231컬럼 테이블은 쿼리도 모델도 감당이 안 된다
--   2. 모듈 단위 집계(결함 판정이 실제로 하는 일)가 배열이면 그냥 된다
--   3. 배열은 순서를 보존하므로 손실이 없다. cell_voltages[m*11 + c + 1] 로
--      M{m+1}CV{c+1} 을 되찾을 수 있다 (Postgres 배열은 1-based)

-- 타입을 REAL(4바이트)로 잡은 이유
--   원본 유효숫자가 최대 5자리(3.645, 641.3)라 REAL 의 7자리로 충분하다.
--   DOUBLE PRECISION 이면 저장 공간이 두 배가 되는데 얻는 정밀도는 없다.
--   셀 전압 오차는 0.0004mV 수준으로, 판정 기준인 mV 단위보다 3자리 작다.

-- generator 의 조회 순서와 같게 잡은 인덱스. 팩 하나의 시계열을 순서대로
-- 훑는 것이 이 테이블의 주 용도다.
CREATE INDEX IF NOT EXISTS pack_measurement_series_idx
    ON pack_measurement (serial_number, mode, measured_at);

-- 원시 데이터라 (serial_number, measured_at, mode) 에 UNIQUE 를 걸 수 없다.
-- 1043_dchg.csv 는 같은 타임스탬프가 60번씩 반복되고(고유 타임스탬프 35개에
-- 1,349행), 1050_chg.csv 에도 중복이 있다. 제약을 걸면 적재 자체가 실패한다.
-- 중복 배제는 읽는 쪽에서 한다.

COMMENT ON TABLE  pack_measurement IS
    '배터리 팩 측정 원시 데이터. 품질 규칙은 읽는 쪽에서 적용한다. docs/kafka-message-spec.md 참고';
COMMENT ON COLUMN pack_measurement.current IS '음수 = 충전, 양수 = 방전';
COMMENT ON COLUMN pack_measurement.dv IS '셀 전압 편차. 단위는 mV (v_max/v_min 은 V)';
COMMENT ON COLUMN pack_measurement.soh IS '원본이 전 행 0 이라 사용 불가';
COMMENT ON COLUMN pack_measurement.cell_voltages IS '176개. M01CV01~M16CV11 순서, 1-based';
COMMENT ON COLUMN pack_measurement.module_temps IS '32개. M01T01~M16T02 순서, 1-based';
