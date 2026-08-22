# battery-pack-defect-detection

FastAPI + Streamlit + Kafka + Postgres 개발 환경. 클론해서 컨테이너를 띄우면
바로 개발을 시작할 수 있는 상태가 되는 것이 목표다.

## 시작하기

```bash
git clone https://github.com/wjdgurrl12/battery-pack-defect-detection.git
cd battery-pack-defect-detection
docker compose up -d
```

VS Code 라면 폴더를 연 뒤 **Reopen in Container** 를 누르면 `dev` 컨테이너에
붙는다. 나머지 서비스도 함께 뜬다.

## 환경이 제대로 떴는지 확인

```bash
docker compose exec dev pytest
```

파이썬/라이브러리, Postgres 읽고 쓰기, Kafka 발행→수신 왕복을 확인한다.

## 서비스

| 서비스 | 주소 | 설명 |
|---|---|---|
| `dev` | – | 개발 작업용 컨테이너 (`sleep infinity`) |
| `api` | http://localhost:3000 | FastAPI (`main.py`, `--reload`) |
| `streamlit` | http://localhost:8501 | Streamlit (`app.py`) |
| `kafka-ui` | http://localhost:8080 | 토픽/메시지 확인 |
| `postgres` | `localhost:5432` | `app` / `app` / `appdb` |
| `broker` | `localhost:9092` | 컨테이너 안에서는 `broker:19092` |

## 구조

- 앱 코드는 이미지에 굽지 않는다. `./` 를 `/workspace` 로 마운트하므로 호스트에서
  파일을 고치면 컨테이너에 바로 반영된다 (api 는 `--reload` 로 자동 재시작).
- 가상환경은 `/opt/venv` 에 있다. 마운트에 덮이지 않도록 `/workspace` 밖에 뒀다.
- 의존성은 `docker compose exec dev uv add <pkg>` 로 추가하고, 바뀐 `pyproject.toml` 과
  `uv.lock` 을 함께 커밋한다. 이미지를 다시 구울 필요는 없다 — 각 컨테이너가 뜰 때
  `uv sync --locked` 로 `uv.lock` 에 맞춘다. 받는 쪽은 `git pull` 후 `docker compose up -d`.
- 이미지를 다시 구워야 하는 경우는 `Dockerfile` 이 바뀔 때다(시스템 패키지, 파이썬 버전).
  이때는 `docker-compose.yml` 의 태그를 올리고(`0.1.0` → `0.1.1`) `build` 후 `push` 한다.
- 초기 스키마가 필요하면 `db/init/*.sql` 에 넣는다. Postgres 최초 기동 시 1회 실행되므로,
  이미 볼륨이 있으면 `docker compose down -v` 로 지워야 다시 적용된다.

## 개발 시작점

- `main.py` — `/health` 만 있는 FastAPI 껍데기
- `app.py` — API 상태만 확인하는 Streamlit 껍데기
- `src/battery_pack_defect_detection/` — 공용 모듈을 넣을 패키지
- `tests/test_smoke.py` — 환경 확인용. 앱 테스트는 `tests/` 에 따로 추가한다

의존성 추가, 이미지 갱신, 컨테이너가 안 뜰 때의 대처는 참고.
