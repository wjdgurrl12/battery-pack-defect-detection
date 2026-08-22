# 개발 규칙

## 의존성을 추가할 때

```bash
docker compose exec dev uv add polars
```

바뀐 **`pyproject.toml` 과 `uv.lock` 을 함께 커밋**하면 끝이다. 이미지를 다시 굽거나
push 할 필요는 없다.

각 컨테이너는 뜰 때마다 `uv sync --locked` 로 자기 `/opt/venv` 를 `uv.lock` 에 맞춘다
(`docker-compose.yml` 의 `command`). `uv.lock` 은 git 으로 전파되므로 받는 쪽은
`git pull` 후 `docker compose up -d` 만 하면 된다.

이미 떠 있는 컨테이너에 반영하려면 재시작한다. 1초면 된다.

```bash
docker compose restart api streamlit
```

> `uv pip install` 은 쓰지 않는다. `uv.lock` 에 기록되지 않아 아무에게도 전파되지 않는다.

## 이미지를 다시 구워야 하는 경우

`Dockerfile` 이 바뀔 때뿐이다 — 시스템 패키지(apt), 파이썬 버전, uv 설정 등.
이때는 **태그를 올리는 것까지가 한 세트**다.

```bash
# 1) docker-compose.yml 의 x-app.image 태그를 올린다
#    4dcookie/vibration-monitoring-dev:0.1.0  ->  :0.1.1
# 2) 굽고 올린다 (앱 이미지 하나만 push 된다)
docker login
docker compose build
docker compose push
# 3) 확인
docker compose up -d --wait
docker compose exec dev pytest
```

`Dockerfile` 과 `docker-compose.yml` 을 한 커밋에 담아 PR 을 올린다.

**태그를 그대로 두고 push 하면 안 된다.** Hub 의 이미지는 갱신되지만 `pull_policy: missing`
때문에 로컬에 `0.1.0` 이 이미 있는 팀원은 새로 받지 않는다. "나는 되는데 너만 안 되는"
상태가 되고 원인을 찾기 어렵다.

> `4dcookie/` 네임스페이스에 push 하려면 권한이 필요하다. 없으면 1번까지만 하고 PR 을
> 올린 뒤 push 는 저장소 관리자에게 맡긴다.

## 컨테이너가 안 뜰 때

`uv sync --locked` 는 `pyproject.toml` 과 `uv.lock` 이 어긋나면 **실패한다.** 의존성이
조용히 빠진 채로 뜨는 것보다 낫지만, 컨테이너가 아예 안 뜨는 것으로 나타난다.

```bash
docker compose logs api | tail -20
```

로그에 `The lockfile is not up-to-date` 같은 메시지가 보이면 `pyproject.toml` 을 손으로
고치고 lock 을 안 돌린 경우다. 아래로 고친다.

```bash
docker compose exec dev uv lock
docker compose restart api streamlit
```

현재 환경이 `uv.lock` 과 맞는지는 이걸로 확인한다 (어긋나면 종료 코드 1).

```bash
docker compose exec api uv sync --locked --check --no-install-project
```

## 환경 자체가 의심스러울 때

```bash
docker compose exec dev pytest
```

파이썬/라이브러리 import, Postgres 읽고 쓰기, Kafka 발행→수신 왕복을 확인한다.
`tests/test_smoke.py` 는 앱 로직이 아니라 **환경**을 검증하는 곳이므로, 앱 테스트는
`tests/` 에 별도 파일로 추가한다.
