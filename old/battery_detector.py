"""배터리 이상탐지 — 단일 파일 배포 모듈.

Kafka(원시) -> FastAPI(추론) -> Kafka(결과) -> Streamlit 구성에 붙인다.

    uvicorn battery_detector:app --host 0.0.0.0 --port 8000 --workers 1

이 파일이 들고 있는 것 (원래 5개 모듈이었다)
    Settings ..................... 환경변수 설정
    RawReading / ScoreMessage / AlarmMessage .. Kafka 메시지 스키마
    StreamGate ................... 스트림 전처리 (STEP 1 대응)
    PackDetector / DetectorPool .. 검출기 파사드 (인프라 무관)
    app .......................... FastAPI + Kafka consumer

별도로 필요한 것
    src/        step*.py 전체. 이 파일은 모델 코드를 복제하지 않고 import 한다.
                복제하면 학습 코드와 배포 코드가 갈라지고, manifest 의
                source_sha8 검증이 무의미해진다.
    아티팩트    폴더(BD_ARTIFACT_DIR) 또는 번들 1개(BD_ARTIFACT_BUNDLE)

════════════════════════════════════════════════════════════════════════════
반드시 지킬 것 — 어겨도 예외가 안 나고 조용히 틀린다
════════════════════════════════════════════════════════════════════════════

1) 입력은 5초에 1행.  학습 데이터가 전부 5초/행이라 코드의 '초' 상수가 사실은
   '행'이다. 1 Hz 를 그대로 넣으면 모든 시간 창이 5배 줄어든다.

       상수            코드 표기   실제(5초/행)   1 Hz 로 넣으면
       SLOPE_HALF 30   "60초"      300초          60초
       persist    2    "10초"      10초           2초   <- 오탐 급증
       warmup     60   "300초"     300초          60초

   StreamGate 가 처리한다. BD_SOURCE_HZ 만 정확히 넣으면 된다.

2) 메시지 키 = pack_id, uvicorn --workers 1.
   모델이 상태를 들고 있어서(V2 링버퍼·T1 오프셋·지속 카운터) 같은 팩의 연속
   행이 같은 프로세스에 순서대로 들어가야 한다. 확장은 워커가 아니라 파티션 수로.

3) scikit-learn 버전 고정.
   pkl 안에 PCA / IsolationForest 객체가 그대로 들어 있다.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
import pickle
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from pydantic import BaseModel, Field, field_validator

log = logging.getLogger("battery.detector")

N_CELLS, N_TEMPS = 176, 32
BUNDLE_FORMAT = 1


# ════════════════════════════════════════════════════════════════════════════
# 설정
# ════════════════════════════════════════════════════════════════════════════
def _env(key: str, default: str | None = None) -> str:
    v = os.environ.get(key, default)
    if v is None:
        raise RuntimeError(f"환경변수 {key} 가 필요하다")
    return v


def _path_env(key: str) -> Path | None:
    v = os.environ.get(key)
    return Path(v) if v else None


@dataclass
class Settings:
    # ── 아티팩트 ─────────────────────────────────────────────────────────
    # 둘 중 하나만 있으면 된다. 번들이 있으면 번들이 우선한다.
    bundle: Path | None = field(default_factory=lambda: _path_env("BD_ARTIFACT_BUNDLE"))
    artifact_dir: Path | None = field(default_factory=lambda: _path_env("BD_ARTIFACT_DIR"))
    src_dir: Path = field(default_factory=lambda: Path(_env("BD_SRC_DIR")))

    # 어떤 점수를 쓸 것인가. step9_realtime.main() 의 기본값이 rule 이다.
    #   rule  : 룰 기반 연속 점수. validate.py 비교에서 검출 감도가 가장 높았다
    #   score : max(z_max, SPE, IF) 통합 점수
    # 바꾸면 임계값도 같이 바뀐다 (rule 11.47 / score 33.72).
    score_key: str = field(default_factory=lambda: os.environ.get("BD_SCORE_KEY", "rule"))

    # 폴더 모드에서 읽을 파일명. rule/score 둘 다 운영 모델(_op)을 쓴다.
    model_name: str = "model_chg_op.pkl"
    reference_name: str = "step4_chg_reference_train.csv"

    # ── 신호 전처리 (STEP 1 과 같은 상수. 임의로 바꾸지 말 것) ────────────
    target_sec_per_row: float = 5.0   # 학습 격자. manifest.json 에 기록돼 있다
    source_hz: float = field(default_factory=lambda: float(os.environ.get("BD_SOURCE_HZ", "1.0")))
    current_on: float = 1.0           # A. |I| 가 이보다 커야 통전 (충전 전류는 음수)
    step_delta: float = 5.0           # A. 행간 전류 변화가 이보다 크면 급변
    settle_rows: int = 5              # 급변 직후 제외할 원본 행 수
    idle_reset_rows: int = 60         # 통전이 이만큼 끊기면 세션 종료(상태 리셋)

    # ── Kafka ───────────────────────────────────────────────────────────
    bootstrap: str = field(default_factory=lambda: os.environ.get("BD_KAFKA_BOOTSTRAP", ""))
    topic_raw: str = field(default_factory=lambda: os.environ.get("BD_TOPIC_RAW", "battery.raw"))
    topic_score: str = field(default_factory=lambda: os.environ.get("BD_TOPIC_SCORE", "battery.score"))
    topic_alarm: str = field(default_factory=lambda: os.environ.get("BD_TOPIC_ALARM", "battery.alarm"))
    group_id: str = field(default_factory=lambda: os.environ.get("BD_GROUP_ID", "battery-detector"))

    # 점수 토픽 발행 간격(판정 행 단위). 1=5초마다, 12=1분마다. 알람은 항상 즉시.
    score_every: int = field(default_factory=lambda: int(os.environ.get("BD_SCORE_EVERY", "1")))

    # ── 안전장치 ────────────────────────────────────────────────────────
    # 기동 시 manifest 의 source_sha8 과 실제 src/ 를 대조한다.
    # 모델과 코드가 어긋나면 예외 없이 조용히 틀리므로 기본값을 켜둔다.
    verify_code_hash: bool = field(
        default_factory=lambda: os.environ.get("BD_VERIFY_HASH", "1") != "0")

    def __post_init__(self) -> None:
        if self.bundle is None and self.artifact_dir is None:
            raise RuntimeError("BD_ARTIFACT_BUNDLE 또는 BD_ARTIFACT_DIR 중 하나가 필요하다")

    @property
    def stride(self) -> int:
        """원본 몇 행마다 1행을 판정에 쓸 것인가. 1 Hz 입력이면 5."""
        return max(1, round(self.target_sec_per_row * self.source_hz))

    @property
    def alarm_config_name(self) -> str:
        return ("step7_chg_alarm_config_rule.json" if self.score_key == "rule"
                else "step7_chg_alarm_config_op.json")


def load_settings() -> Settings:
    return Settings()


# ════════════════════════════════════════════════════════════════════════════
# Kafka 메시지 스키마
#   토픽 3개 — battery.raw(입력) / battery.score / battery.alarm
#   메시지 키는 반드시 pack_id. Kafka 는 같은 파티션 안에서만 순서를 보장하는데,
#   이 모델은 상태를 들고 있어서 순서가 뒤바뀌면 조용히 망가진다.
# ════════════════════════════════════════════════════════════════════════════
class RawReading(BaseModel):
    """battery.raw — BMS 1행."""

    pack_id: int
    ts: float                     # epoch seconds. 중복·역순 판별에 쓴다
    seq: int | None = None        # BMS 시퀀스 번호. 있으면 ts 보다 우선한다
    cells: list[float] = Field(..., description="176 셀 전압 [V]")
    temps: list[float] = Field(..., description="32 온도 센서 [°C]")
    soc: float = Field(..., description="RSOCavg [%]")
    current: float = Field(..., description="팩 전류 [A]. 충전은 음수")

    @field_validator("cells")
    @classmethod
    def _cells_len(cls, v: list[float]) -> list[float]:
        if len(v) != N_CELLS:
            raise ValueError(f"cells 는 {N_CELLS}개여야 한다 (받은 값 {len(v)}개)")
        return v

    @field_validator("temps")
    @classmethod
    def _temps_len(cls, v: list[float]) -> list[float]:
        if len(v) != N_TEMPS:
            raise ValueError(f"temps 는 {N_TEMPS}개여야 한다 (받은 값 {len(v)}개)")
        return v


class ScoreMessage(BaseModel):
    """battery.score — 판정 행마다 (대시보드 추이용)."""

    pack_id: int
    ts: float
    soc: float                    # 필수. 검출률이 SOC 에 따라 2~6배 갈리므로
    score: float
    z_max: float
    threshold: float
    alarm: bool
    warmup: bool                  # True 면 온도 판정(T2/T3/T5) 보류 중
    session_row: int              # 이번 충전 세션에서 몇 번째 판정 행인가


class AlarmMessage(BaseModel):
    """battery.alarm — 알람 발생 시에만."""

    pack_id: int
    ts: float
    soc: float
    score: float
    threshold: float
    cause: str                    # SPE 기여도 1위 열. 예: "V1:M08CV01"
    top3: list[tuple[str, float]]
    fault_type: str
    diagnosis: str
    evidence: dict[str, Any] = Field(default_factory=dict)


# ════════════════════════════════════════════════════════════════════════════
# 스트림 전처리 — STEP 1(데이터 정제)의 실시간 대응물
#
#   학습 데이터는 STEP 1 이 아래 순서로 만들었다. 운영 입력도 같은 순서를
#   거쳐야 모델이 학습 때와 같은 분포를 본다.
#       1) 통전 구간만        |I| > 1.0 A
#       2) 과도구간 제외      전류 급변(|dI| > 5 A) 직후 5행, 통전 재개 직후 5행
#       3) 5초 격자로 솎기    iloc[::stride]  (평균 아님)
#
#   솎아내기를 평균으로 바꾸면 안 된다. 학습 데이터의 각 행은 5초 평균이 아니라
#   순간값 1개이고, 평균을 내면 전압 스파이크가 사라져 성질이 달라진다.
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class Decision:
    """1행에 대한 전처리 판정."""

    use: bool                     # 이 행을 모델에 넣을 것인가
    reason: str = ""              # 버린 이유 (관측용)
    session_reset: bool = False   # 이 행 직전에 세션이 끝났는가


class StreamGate:
    """팩 1대의 전처리 상태. 원본 주기(예: 1 Hz) 그대로 먹인다.

    솎아내기 카운터는 '필터를 통과한 행' 기준으로 센다. STEP 1 도 과도구간을
    떨어낸 뒤에 iloc[::stride] 를 적용하므로 같은 순서다.
    """

    def __init__(self, stride: int, current_on: float = 1.0,
                 step_delta: float = 5.0, settle_rows: int = 5,
                 idle_reset_rows: int = 60):
        self.stride = stride
        self.current_on = current_on
        self.step_delta = step_delta
        self.settle_rows = settle_rows
        self.idle_reset_rows = idle_reset_rows
        self.reset()

    def reset(self) -> None:
        self.prev_current: float | None = None
        self.settle_left = 0        # 남은 과도구간 행 수
        self.idle_rows = 0          # 연속 비통전 행 수
        self.kept = 0               # 필터 통과 행 수 (솎아내기 카운터)
        self.last_key: float | None = None   # 중복·역순 판별 키

    def is_stale(self, key: float) -> bool:
        """이미 본 행이거나 순서가 뒤집힌 행인가.

        Kafka 기본이 at-least-once 라 재전송 중복이 온다. 무상태 서비스면
        무해하지만 여기서는 링버퍼에 같은 행이 두 번 들어가 V2 가 왜곡된다.
        """
        if self.last_key is not None and key <= self.last_key:
            return True
        self.last_key = key
        return False

    def feed(self, current: float) -> Decision:
        # 1) 통전 판정. 충전이 끝나면 세션을 닫는다.
        #    학습 데이터는 팩당 충전 세션 1개라, 세션이 바뀌면 검출기 상태도
        #    새로 시작해야 한다(V2 링버퍼·T1 오프셋이 그 세션 것이므로).
        if abs(current) <= self.current_on:
            self.idle_rows += 1
            if self.idle_rows == self.idle_reset_rows:
                self.reset()
                return Decision(False, "session_end", session_reset=True)
            return Decision(False, "idle")

        resumed = self.idle_rows > 0
        self.idle_rows = 0

        # 2) 과도구간. 전류가 확 바뀐 직후 셀 전압은 내부저항 때문에 계단처럼
        #    튄다. 셀 불량이 아니라 물리 현상이라 남겨두면 전 팩에서 오탐이 난다.
        if resumed or self.prev_current is None:
            self.settle_left = self.settle_rows      # 통전 시작·재개 자체가 급변
        elif abs(current - self.prev_current) > self.step_delta:
            self.settle_left = self.settle_rows
        self.prev_current = current

        if self.settle_left > 0:
            self.settle_left -= 1
            return Decision(False, "transient")

        # 3) 솎아내기. stride 행마다 1행만 모델에 넣는다.
        self.kept += 1
        if (self.kept - 1) % self.stride:
            return Decision(False, "decimated")
        return Decision(True)


# ════════════════════════════════════════════════════════════════════════════
# 아티팩트 로딩
# ════════════════════════════════════════════════════════════════════════════
def _load_src(src_dir: Path):
    """프로젝트 코드는 패키지가 아니라 sys.path 에 src/ 를 얹는 구조다."""
    p = str(Path(src_dir).resolve())
    if p not in sys.path:
        sys.path.insert(0, p)
    import step4_reference as s4      # noqa: E402
    import step6_model as s6          # noqa: E402
    import step7_alarm as s7          # noqa: E402
    import step9_realtime as s9       # noqa: E402
    return s4, s6, s7, s9


def sha8(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:8]


def check_code_hash(src_dir: Path, manifest: dict) -> list[str]:
    """manifest 에 기록된 학습 시점 코드 해시와 지금 src/ 를 대조한다.

    이 시스템은 모델과 코드가 어긋나도 예외가 안 나고 조용히 틀린다
    (피처 순서·격자·상수가 전부 코드 쪽에 있다). 기동 시 반드시 확인한다.
    """
    bad = []
    for name, h in manifest.get("source_sha8", {}).items():
        f = Path(src_dir) / name
        if not f.exists():
            bad.append(f"{name}: 파일 없음")
        elif sha8(f) != h:
            bad.append(f"{name}: {sha8(f)} != {h} (학습 시점)")
    return bad


@dataclass
class Artifacts:
    ref: Any            # step4_reference.ReferenceTable
    model: Any          # step6_model.Model
    cfg: Any            # step7_alarm.AlarmConfig
    manifest: dict
    s9: Any             # step9_realtime 모듈 (Reading / RealtimeDetector)
    source: str = ""    # 어디서 읽었는지 (관측용)


def load_artifacts(st: Settings) -> Artifacts:
    import pandas as pd

    s4, s6, s7, s9 = _load_src(st.src_dir)

    if st.bundle is not None:
        # ── 번들 모드: 파일 1개 ─────────────────────────────────────────
        with open(st.bundle, "rb") as f:
            b = pickle.load(f)
        if b.get("format") != BUNDLE_FORMAT:
            raise RuntimeError(f"번들 형식 {b.get('format')} 을 읽을 수 없다 "
                               f"(이 코드는 {BUNDLE_FORMAT})")
        if st.score_key not in b["alarm"]:
            raise RuntimeError(f"번들에 {st.score_key} 임계값이 없다 "
                               f"(있는 것: {sorted(b['alarm'])})")
        manifest = b["manifest"]
        alarm = b["alarm"][st.score_key]

        def make():
            return (s4.ReferenceTable(pd.read_csv(io.StringIO(b["reference_csv"]))),
                    s6.Model(**b["model"]), s7.AlarmConfig(**alarm))

        source = f"bundle:{Path(st.bundle).name}"
    else:
        # ── 폴더 모드: 원래 파일 그대로 ─────────────────────────────────
        d = Path(st.artifact_dir)
        manifest = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
        alarm = json.loads((d / st.alarm_config_name).read_text(encoding="utf-8"))

        def make():
            return (s4.ReferenceTable.load(d / st.reference_name),
                    s6.load(d / st.model_name), s7.AlarmConfig(**alarm))

        source = f"dir:{d.name}"

    if st.verify_code_hash:
        bad = check_code_hash(st.src_dir, manifest)
        if bad:
            raise RuntimeError(
                "학습 시점 코드와 src/ 가 다르다. 모델을 그대로 쓰면 조용히 틀린다:\n  "
                + "\n  ".join(bad)
                + "\n(의도한 변경이라면 BD_VERIFY_HASH=0 으로 끌 수 있다)")

    trained = manifest.get("params", {}).get("target_sec_per_row")
    if trained is not None and abs(trained - st.target_sec_per_row) > 1e-9:
        raise RuntimeError(
            f"격자 불일치: 학습 {trained}초/행, 설정 {st.target_sec_per_row}초/행")

    ref, model, cfg = make()
    return Artifacts(ref=ref, model=model, cfg=cfg, manifest=manifest,
                     s9=s9, source=source)


# ════════════════════════════════════════════════════════════════════════════
# 검출기 — 인프라를 전혀 모른다. 전송 계층을 바꿔도 이 아래는 그대로다.
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class Result:
    pack_id: int
    ts: float
    soc: float
    score: float
    z_max: float
    alarm: bool
    warmup: bool
    session_row: int
    cause: str = ""
    top3: list[tuple[str, float]] = field(default_factory=list)
    fault_type: str = ""
    diagnosis: str = ""
    evidence: dict = field(default_factory=dict)


class PackDetector:
    """전처리 게이트 + RealtimeDetector 를 한 쌍으로 묶은 것."""

    def __init__(self, pack_id: int, art: Artifacts, st: Settings):
        self.pack_id = pack_id
        self.art, self.st = art, st
        self.gate = StreamGate(stride=st.stride, current_on=st.current_on,
                               step_delta=st.step_delta, settle_rows=st.settle_rows,
                               idle_reset_rows=st.idle_reset_rows)
        self.det = art.s9.RealtimeDetector(art.ref, art.model, art.cfg,
                                           score_key=st.score_key)
        self.session_row = 0
        self.n_seen = 0          # 받은 원본 행 (관측용)
        self.n_used = 0          # 판정에 쓴 행
        self.n_alarm = 0

    def reset_session(self) -> None:
        """충전 세션이 끝났을 때. 링버퍼·오프셋·지속 카운터를 전부 비운다."""
        self.det.reset()
        self.session_row = 0

    def feed(self, ts: float, cells, temps, soc: float, current: float,
             key: float | None = None) -> Result | None:
        self.n_seen += 1

        if self.gate.is_stale(key if key is not None else ts):
            return None                          # Kafka at-least-once 중복

        d = self.gate.feed(current)
        if d.session_reset:
            self.reset_session()
        if not d.use:
            return None

        v = self.det.step(self.art.s9.Reading(
            np.asarray(cells, dtype=float),
            np.asarray(temps, dtype=float),
            float(soc)))

        self.n_used += 1
        self.session_row += 1

        r = Result(pack_id=self.pack_id, ts=ts, soc=float(soc),
                   score=float(v.score), z_max=float(v.z_max),
                   alarm=bool(v.alarm), warmup=bool(v.warmup),
                   session_row=self.session_row)
        if v.alarm:
            self.n_alarm += 1
            r.cause = v.cause
            r.fault_type = v.fault_type
            r.top3 = [(str(lab), float(w)) for lab, w in v.detail.get("top", [])]
            r.diagnosis = str(v.detail.get("diag", ""))
        return r

    def stats(self) -> dict:
        return {"pack_id": self.pack_id, "seen": self.n_seen, "used": self.n_used,
                "alarms": self.n_alarm, "session_row": self.session_row,
                "warmup_left": max(0, self.art.cfg.warmup_sec - self.session_row)}


class DetectorPool:
    """pack_id -> PackDetector. 아티팩트(모델·기준표)는 전 팩이 공유한다.

    모델과 기준표는 읽기 전용이라 공유해도 안전하다. 팩마다 다른 것은
    RealtimeDetector 의 상태(링버퍼 61행 x 176셀, 오프셋 32개, 카운터)뿐이고
    팩당 약 90 KB 다. 100팩이면 9 MB 라 메모리는 문제가 되지 않는다.
    """

    def __init__(self, st: Settings | None = None):
        self.st = st or load_settings()
        self.art = load_artifacts(self.st)
        self.packs: dict[int, PackDetector] = {}

    @property
    def threshold(self) -> float:
        return float(self.art.cfg.threshold)

    def get(self, pack_id: int) -> PackDetector:
        d = self.packs.get(pack_id)
        if d is None:
            d = self.packs[pack_id] = PackDetector(pack_id, self.art, self.st)
        return d

    def feed(self, pack_id: int, ts: float, cells, temps,
             soc: float, current: float, key: float | None = None) -> Result | None:
        return self.get(pack_id).feed(ts, cells, temps, soc, current, key)

    def drop(self, pack_id: int) -> None:
        self.packs.pop(pack_id, None)

    def stats(self) -> dict:
        return {
            "source": self.art.source,
            "model": self.art.manifest.get("created"),
            "tag": self.art.manifest.get("tag"),
            "score_key": self.st.score_key,
            "threshold": self.threshold,
            "stride": self.st.stride,
            "warmup_rows": self.art.cfg.warmup_sec,
            "persist_rows": self.art.cfg.persist_sec,
            "n_packs": len(self.packs),
            "packs": [d.stats() for d in self.packs.values()],
        }


# ════════════════════════════════════════════════════════════════════════════
# 서비스 — FastAPI + Kafka consumer
#
#   HTTP 로 행을 받지 않는다. 상태가 있는 모델이라 같은 팩의 연속 행이 반드시
#   같은 프로세스에 순서대로 들어가야 한다. HTTP 는 관측·제어 평면으로만 쓴다.
#
#       battery.raw --> [consumer 루프 --> DetectorPool] --> battery.score
#                                                        --> battery.alarm
#
#   확장은 워커 수가 아니라 Kafka 파티션 수로 한다.
#     - 원시 토픽 파티션 N개, 이 서비스 인스턴스 N개, 전부 같은 group_id
#     - 프로듀서는 메시지 키를 반드시 pack_id 로 (같은 팩 -> 같은 파티션)
#
#   fastapi/aiokafka 가 없으면 이 아래는 정의되지 않는다. 검출기만 쓰는
#   경우(스모크 테스트·배치 재생)에는 설치 없이도 위쪽이 그대로 동작한다.
# ════════════════════════════════════════════════════════════════════════════
try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from fastapi import FastAPI, HTTPException
    from pydantic import ValidationError
    _SERVICE_DEPS = True
except ImportError:                                   # pragma: no cover
    _SERVICE_DEPS = False

STATE: dict = {"pool": None, "task": None, "errors": 0, "skipped": 0}


if _SERVICE_DEPS:

    async def consume_loop(st: Settings, pool: DetectorPool,
                           consumer, producer) -> None:
        """원시 토픽 -> 판정 -> 결과 토픽.

        추론을 스레드로 넘기지 않고 루프 안에서 그대로 돈다. 순서 보장이
        정확성 조건이라 동시 실행이 오히려 위험하고, 1행당 처리 시간이 예산
        대비 수백 배 여유라 그럴 이유도 없다.
        """
        async for msg in consumer:
            try:
                r = RawReading.model_validate_json(msg.value)
            except ValidationError as e:
                STATE["errors"] += 1
                log.warning("스키마 불일치 offset=%s: %s", msg.offset, e)
                continue

            try:
                res = pool.feed(pack_id=r.pack_id, ts=r.ts, cells=r.cells,
                                temps=r.temps, soc=r.soc, current=r.current,
                                key=float(r.seq) if r.seq is not None else r.ts)
            except Exception:
                STATE["errors"] += 1
                log.exception("판정 실패 pack=%s ts=%s", r.pack_id, r.ts)
                continue

            if res is None:                 # 솎임·과도구간·비통전·중복
                STATE["skipped"] += 1
                continue

            key = str(res.pack_id).encode()

            # 알람은 항상 즉시. 점수는 score_every 마다(대시보드 부하 조절).
            if res.alarm:
                m = AlarmMessage(pack_id=res.pack_id, ts=res.ts, soc=res.soc,
                                 score=res.score, threshold=pool.threshold,
                                 cause=res.cause, top3=res.top3,
                                 fault_type=res.fault_type,
                                 diagnosis=res.diagnosis, evidence=res.evidence)
                await producer.send_and_wait(
                    st.topic_alarm, m.model_dump_json().encode(), key=key)

            if res.alarm or res.session_row % st.score_every == 0:
                m = ScoreMessage(pack_id=res.pack_id, ts=res.ts, soc=res.soc,
                                 score=res.score, z_max=res.z_max,
                                 threshold=pool.threshold, alarm=res.alarm,
                                 warmup=res.warmup, session_row=res.session_row)
                await producer.send_and_wait(
                    st.topic_score, m.model_dump_json().encode(), key=key)

    @asynccontextmanager
    async def lifespan(app):
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(name)s %(message)s")
        st = load_settings()
        if not st.bootstrap:
            raise RuntimeError("BD_KAFKA_BOOTSTRAP 가 필요하다")

        # 아티팩트 로드 + 코드 해시 검증. 여기서 실패하면 기동하지 않는다.
        pool = DetectorPool(st)
        log.info("모델 로드 완료: %s", json.dumps(
            {k: v for k, v in pool.stats().items() if k != "packs"},
            ensure_ascii=False))

        consumer = AIOKafkaConsumer(
            st.topic_raw, bootstrap_servers=st.bootstrap, group_id=st.group_id,
            # 상태 있는 소비자라 자동 커밋으로 충분하다. 재시작 시 몇 행 다시
            # 받아도 StreamGate.is_stale 이 중복을 걸러낸다.
            enable_auto_commit=True, auto_offset_reset="latest",
            max_poll_records=200)
        producer = AIOKafkaProducer(bootstrap_servers=st.bootstrap,
                                    acks="all", enable_idempotence=True)

        await consumer.start()
        await producer.start()
        task = asyncio.create_task(consume_loop(st, pool, consumer, producer))
        STATE.update(pool=pool, task=task)
        log.info("consumer 기동: %s -> %s / %s",
                 st.topic_raw, st.topic_score, st.topic_alarm)
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await consumer.stop()
            await producer.stop()
            log.info("종료 완료")

    app = FastAPI(title="Battery Anomaly Detector", lifespan=lifespan)

    # ── 관측·제어 평면 (추론 경로가 아니다) ──────────────────────────────
    @app.get("/health")
    def health() -> dict:
        task = STATE.get("task")
        alive = task is not None and not task.done()
        return {"ok": alive and STATE["pool"] is not None,
                "consumer_alive": alive,
                "errors": STATE["errors"], "skipped": STATE["skipped"]}

    @app.get("/stats")
    def stats() -> dict:
        pool = STATE.get("pool")
        if pool is None:
            raise HTTPException(503, "아직 기동 중")
        return pool.stats()

    @app.get("/packs/{pack_id}")
    def pack_stats(pack_id: int) -> dict:
        pool = STATE.get("pool")
        if pool is None or pack_id not in pool.packs:
            raise HTTPException(404, f"팩 {pack_id} 상태 없음")
        return pool.packs[pack_id].stats()

    @app.post("/packs/{pack_id}/reset")
    def pack_reset(pack_id: int) -> dict:
        """팩 상태를 강제로 비운다. 다시 warmup 부터 시작한다."""
        pool = STATE.get("pool")
        if pool is None or pack_id not in pool.packs:
            raise HTTPException(404, f"팩 {pack_id} 상태 없음")
        pool.packs[pack_id].reset_session()
        return {"pack_id": pack_id, "reset": True}
