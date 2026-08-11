# syntax=docker/dockerfile:1

# 개발용 이미지. 애플리케이션 코드는 굽지 않고 /workspace 에 마운트해서 쓴다.
# (코드를 이미지에 넣으면 컨테이너 안에서 고친 내용과 이미지가 어긋난다)
# 배포용 런타임 이미지는 실제 앱 코드가 생긴 뒤에 별도 스테이지로 추가한다.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# 가상환경을 /workspace 밖에 둬서, /workspace 에 볼륨을 마운트해도 덮이지 않게 한다
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# 개발 중에 흔히 필요한 것들만 최소로
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# 의존성만 미리 설치해 이미지에 구워둔다. 받아서 띄우면 바로 import 가 되는 상태.
# (README.md 는 pyproject.toml 의 readme 항목이 참조하므로 함께 복사한다)
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project

# 실행 명령은 compose 의 command 로 지정한다 (dev 컨테이너는 sleep infinity)