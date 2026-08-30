# old — 쓰지 않는 옛 모델

2026-08-27 에 이상탐지 모델을 오토인코더([`battery_anomaly.py`](../battery_anomaly.py))로
갈아타면서, 그 전까지 쓰던 모델 일체를 여기로 옮겼다.

**지금 이 폴더의 파일은 어디에서도 import 되지 않는다.** 파이프라인이 도는 데
필요 없다. 지우지 않고 남긴 이유는 아래 '왜 지우지 않았나' 에 적었다.

## 무엇이 여기 있나

| 경로 | 무엇 |
|---|---|
| `battery_detector.py` | 모델팀 인수인계본. 설정·스키마·전처리 게이트(`StreamGate`)·검출기 파사드(`DetectorPool`) |
| `src/step1_clean.py` ~ `step9_realtime.py` | 학습 파이프라인 9단계. 전처리 → 분해 → 피처 → 기준표 → 정규화 → 모델 → 알람 → 유형분류 → 실시간재생 |
| `src/cross_validate.py` `evaluate.py` `validate.py` `verify_model.py` | 위 단계들의 검증 도구 |
| `src/fault_injection.py` `metrics.py` `snapshot_model.py` | 주입 실험·지표·번들 스냅샷 |
| `src/README.md` | 모델팀 인수인계 문서 |
| `src/.env.example` `src/requirements.txt` | 옛 모델의 환경변수·의존성 |
| `models/battery_model_20260825_165511_b_option.bundle` | 학습된 번들(PCA / IsolationForest 가 pickle 로 들어 있다) |

파일들끼리는 서로를 참조한다(`cross_validate.py` 가 `import step1_clean` 하는 식).
그래서 `src/` 하위 구조를 그대로 옮겼다 — 한 폴더에 모여 있어야 그대로 돌아간다.

## 2026-08-30 에 추가로 들어온 것

| 경로 | 무엇 | 왜 |
|---|---|---|
| `inject_anomalies.py` | 재생 중인 스트림의 셀 하나를 일회성으로 띄우던 개발 도구 | 데모 팩이 생기면서 고장이 데이터 자체에 들어갔다. 발행 직전에 또 흔들면 정답표(`database.DEMO_PACKS`)와 대조가 안 된다 |

이 파일은 `sensor_generator.make_anomalous` 를 공유해 썼는데, 그 함수와
`AnomalyPlan`·`--anomaly-every` 옵션(발행 루프 쪽 주입 일체)도 같은 날
sensor_generator 에서 지워졌다. **그래서 이 파일은 이대로는 돌지 않는다** -
되살리려면 git 이력에서 sensor_generator 의 '3단계: 이상치 주입' 을 함께 꺼낸다.

같은 날 파일 없이 지워진 것들(전부 어디서도 안 쓰이는 것을 확인했다):
`schemas.py`(pydantic import 한 줄짜리 빈 껍데기), `database.py` 의
`SessionLocal`/`get_db`/`check_db_connection`(초기 ORM 골격), `app.py` 의
`CHARGE_TONES`(사이드바 배지 시절의 색 짝).

## 무엇이 달라졌나

| | 옛 모델 (여기) | 새 모델 |
|---|---|---|
| 판정 단위 | 행 하나 | 팩(충전 세션) 하나 |
| 방식 | PCA + IsolationForest + 룰 점수 | 오토인코더 2개 + 로버스트 통계 |
| 상태 | `StreamGate` 가 링버퍼·온도 오프셋·지속 카운터를 들고 있다 | 상태 없음. 세션 누적은 `detector.py` 가 한다 |
| 전처리 | 모델이 스스로 통전/중복/솎기를 거른다 | `database.py` 가 거른 것을 그대로 믿는다 |
| 아티팩트 | `.bundle` (코드 해시 검증 포함) | `models/battery_anomaly.pkl` |
| 학습 | `src/step*.py` 9단계 | [`train_anomaly.py`](../train_anomaly.py) 한 개 |

## 왜 지우지 않았나

1. **되돌릴 수 있어야 한다.** 새 모델은 팩 단위라 세션이 끝나갈 때까지 확정
   판정이 안 나온다. 행 단위 즉시 알람이 다시 필요해지면 여기서 꺼내 쓴다.
2. **근거 문서가 이 코드를 가리킨다.** `docs/` 의 실험 기록(`ae_model.md`,
   `diagnostics (1).md`, `joint_anomaly.md`)이 baseline 으로 삼은 것이 이 모델이다.
   코드가 없으면 그 숫자들을 다시 낼 수 없다.
3. 크지 않다. 번들 3.2 MB 에 코드 약 4,000줄이다.

## 되살리려면

`src/battery_pack_defect_detection/detector.py` 를 갈아 끼우면 된다. 그 파일이
바깥과의 계약(`load` / `judge` / `info` / `reset_pack`)을 들고 있고, 모델을 바꾼다는
것은 그 안쪽만 바꾼다는 뜻이다. git 이력에 옛 구현이 그대로 남아 있다.

    git log --follow -- src/battery_pack_defect_detection/detector.py

환경변수(`BD_ARTIFACT_BUNDLE` / `BD_SRC_DIR` / `BD_SOURCE_HZ` / `BD_SCORE_KEY` /
`BD_VERIFY_HASH`)는 `docker-compose.yml` 에서 뺐다. 되살릴 때 `src/.env.example`
을 보고 다시 넣되, 경로가 `/workspace/old/` 아래로 바뀐 것에 주의한다.
