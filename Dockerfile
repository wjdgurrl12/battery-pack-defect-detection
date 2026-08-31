# syntax=docker/dockerfile:1

# 이미지 넷을 한 파일에서 굽는다. 바닥(의존성)이 같아서 따로 두면 두 벌이
# 어긋나기 때문이다. 어느 것을 구울지는 `--target` 또는 compose 의 build.target
# 으로 고른다.
#
#   deps            의존성만 설치한 공통 바닥 (직접 쓰지 않는다)
#   dev             개발용. 코드를 굽지 않고 /workspace 에 마운트해서 쓴다
#   runtime         배포용. api·streamlit 이 함께 쓴다. 코드와 모델을 굽는다
#   seedgen         데모 CSV -> COPY 덤프. postgres-demo 의 재료다
#   postgres-demo   데모 9팩이 들어 있는 Postgres
#
# 개발 이미지와 배포 이미지의 차이는 **코드를 굽는가** 하나뿐이다.
#   개발  이미지에 의존성만. 코드는 마운트 -> 고치면 즉시 반영
#   배포  이미지에 코드까지. 마운트 없음 -> clone 없이 pull 만으로 돈다


##############################################################################
# deps - 의존성만 설치한 공통 바닥
##############################################################################
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS deps

# 가상환경을 /workspace 밖에 둬서, /workspace 에 볼륨을 마운트해도 덮이지 않게 한다
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

# 의존성만 미리 설치해 이미지에 구워둔다. 받아서 띄우면 바로 import 가 되는 상태.
# (README.md 는 pyproject.toml 의 readme 항목이 참조하므로 함께 복사한다)
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project


##############################################################################
# dev - 지금까지 쓰던 개발 이미지
#
# 애플리케이션 코드는 굽지 않고 /workspace 에 마운트해서 쓴다.
# (코드를 이미지에 넣으면 컨테이너 안에서 고친 내용과 이미지가 어긋난다)
##############################################################################
FROM deps AS dev

# 개발 중에 흔히 필요한 것들만 최소로
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 실행 명령은 compose 의 command 로 지정한다 (dev 컨테이너는 sleep infinity)


##############################################################################
# runtime - api 와 streamlit 이 함께 쓰는 배포 이미지
#
# 이미지 하나를 둘이 나눠 쓴다. 같은 코드에서 진입점만 다르고(uvicorn /
# streamlit run), 둘로 나누면 같은 레이어를 두 번 저장하게 된다.
##############################################################################
FROM deps AS runtime

WORKDIR /app

# src 레이아웃이라 패키지가 /app/src 밑에 있는데 파이썬은 CWD 만 경로에 넣는다.
# 의존성만 굽고 프로젝트는 설치하지 않으므로(--no-install-project) editable
# 설치가 만들어 주는 .pth 도 없다. 그 자리를 이 한 줄이 대신한다.
ENV PYTHONPATH=/app/src \
    HOME=/home/app

# 굽는 것은 도는 데 필요한 것만이다. db/data 의 원본 CSV 600MB 는 넣지 않는다 -
# 측정은 Postgres 에서 나오고, 적재는 개발 쪽 일이다.
#
# detector.DEFAULT_MODEL 이 src 에서 두 단계 올라간 자리(=/app)의 models/ 를
# 보므로, 이 배치가 곧 모델 경로다.
COPY src/ /app/src/
COPY models/ /app/models/
COPY db/init/ /app/db/init/
# load_raw.py 는 배포 때 부를 일이 없지만 여기 둔다. 아래 seedgen 스테이지가
# 이 파일의 CSV -> 배열 변환을 그대로 빌려 쓴다.
COPY main.py app.py database.py sensor_generator.py battery_anomaly.py \
     pack_loader.py load_raw.py schemas.py /app/

# root 로 돌 이유가 없다. api 가 sensor_generator 를 자식 프로세스로 띄우는데,
# 그 프로세스까지 권한을 물려받기 때문에 여기서 한 번 낮춰 두는 값이 크다.
RUN useradd --create-home --uid 10001 app \
    && chown -R app:app /app /home/app
USER app

EXPOSE 3000 8501

# 기본은 api. streamlit 은 compose 에서 command 로 갈아 끼운다.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]


##############################################################################
# seedgen - 데모 CSV 9개를 Postgres COPY 덤프로 바꾼다
#
# 여기서 하는 이유: 231컬럼 CSV 를 배열 2개로 접는 변환이 load_raw.py 안에
# 있고, 그 규칙을 SQL 로 옮겨 적으면 두 벌이 갈라진다. 파이썬이 있는 이
# 스테이지에서 한 번 돌려 결과만 넘긴다.
##############################################################################
FROM runtime AS seedgen

USER root

# .dockerignore 가 db/data 에서 DEMO*_chg.csv 만 통과시킨다 (원본 600MB 제외)
COPY db/data/ /app/db/data/
COPY docker/export_demo_copy.py /app/docker/

RUN python /app/docker/export_demo_copy.py /seed/02_demo.sql.gz


##############################################################################
# postgres-demo - 데모 9팩이 이미 들어 있는 Postgres
#
# 스키마와 덤프를 초기화 스크립트 자리에 둔다. 공식 엔트리포인트가 **데이터
# 디렉터리가 비어 있을 때 1회** 알파벳 순으로 실행한다(.sql / .sql.gz / .sh).
# 그래서 볼륨이 이미 있으면 다시 돌지 않는다 - `down -v` 로 지워야 한다.
#
# PGDATA 를 이미지에 미리 만들어 두지 않는 이유: 공식 이미지가 그 경로를
# VOLUME 으로 선언해 두어서, 빌드 중에 쓴 것은 그대로 버려진다.
##############################################################################
FROM postgres:17-alpine AS postgres-demo

COPY db/init/01_schema.sql /docker-entrypoint-initdb.d/01_schema.sql
COPY --from=seedgen /seed/02_demo.sql.gz /docker-entrypoint-initdb.d/02_demo.sql.gz
