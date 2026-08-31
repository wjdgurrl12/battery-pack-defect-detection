"""학습된 모델을 models/ 아래에 스냅샷으로 보관한다.

outputs/ 는 실행할 때마다 덮어써지는 작업 공간이다. 배포에 필요한 파일만
골라 타임스탬프 폴더로 복사해 두면, 나중에 "그때 그 모델" 을 다시 꺼낼 수 있다.

배포에 필요한 것은 다섯 가지다.
    ① 모델 가중치        model_chg.pkl / model_chg_op.pkl
    ② SOC 기준표         step4_chg_reference_train.csv
    ③ 알람 설정          step7_chg_alarm_config*.json
    ④ 데이터 계약        step1_chg_manifest.json  (어느 팩을 썼는지)
    ⑤ 재현 정보          manifest.json            (파라미터·성능·코드 해시)

⑤ 가 핵심이다. 모델 파일만 남기면 "무슨 설정으로 학습했는지" 를 잃는다.

실행:
    python src/snapshot_model.py                  # 현재 outputs 를 스냅샷
    python src/snapshot_model.py --tag baseline   # 이름 붙여 보관
    python src/snapshot_model.py --list           # 보관된 스냅샷 목록
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"
MODELS = ROOT / "models"

# 배포에 실제로 필요한 파일. 없으면 스냅샷을 만들지 않는다
REQUIRED = [
    "model_chg.pkl",
    "model_chg_op.pkl",
    "step4_chg_reference_train.csv",
    "step7_chg_alarm_config.json",
    "step7_chg_alarm_config_op.json",
    "step7_chg_alarm_config_rule.json",
    "step1_chg_manifest.json",
]
# 있으면 같이 담고, 없어도 넘어가는 것
OPTIONAL = ["validation_chg.json", "eval_chg_summary.json", "eval_chg_folds.csv"]

# 재현에 필요한 소스 파일 (해시로 버전을 남긴다)
SOURCES = ["step1_clean.py", "step2_decompose.py", "step3_features.py",
           "step4_reference.py", "step5_normalize.py", "step6_model.py",
           "step7_alarm.py", "step8_classify.py", "step9_realtime.py",
           "fault_injection.py", "metrics.py", "evaluate.py", "cross_validate.py"]


def sha8(path: Path) -> str:
    """파일 내용 해시 앞 8자리. 코드가 바뀌었는지 한눈에 보려는 용도."""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:8]


def collect_meta(stamp: str, tag: str) -> dict:
    """스냅샷과 함께 남길 재현 정보."""
    import step1_clean as s1
    import step4_reference as s4
    import step5_normalize as s5
    import step6_model as s6
    import step7_alarm as s7

    meta: dict = {
        "created": stamp, "tag": tag,
        "params": {
            "fit_on_all": getattr(s4, "FIT_ON_ALL", None),
            "target_sec_per_row": s1.TARGET_SEC_PER_ROW,
            "min_run_sec": s1.MIN_RUN_SEC,
            "n_bins": s4.N_BINS, "soc_range": [s4.SOC_LO, s4.SOC_HI],
            "n_dim": s5.N_DIM,
            "fit_stride": s6.FIT_STRIDE,
            "if_kwargs": s6.IF_KWARGS,
            "threshold_q": s7.THRESHOLD_Q,
            "persist_rows": s7.PERSIST_SEC,
            "warmup_rows": s7.WARMUP_SEC,
        },
        "source_sha8": {f: sha8(ROOT / "src" / f)
                        for f in SOURCES if (ROOT / "src" / f).exists()},
    }

    # 학습에 쓴 팩과 모델 규모
    man_path = OUT / "step1_chg_manifest.json"
    if man_path.exists():
        man = json.loads(man_path.read_text(encoding="utf-8"))
        fit_all = meta["params"]["fit_on_all"]
        meta["data"] = {
            "n_valid": len(man["valid"]),
            "fit_packs": man["valid"] if fit_all else man["train"],
            "n_fit": len(man["valid"] if fit_all else man["train"]),
            "holdout": man["holdout"],
        }
    try:
        m = s6.load(OUT / "model_chg_op.pkl")
        meta["model_op"] = {"n_components": m.n_components, "dim": m.dim,
                            "spe_ref": m.spe_ref, "if_ref": m.if_ref, "if_med": m.if_med}
    except Exception:
        pass
    for name in ("", "_op", "_rule"):
        p = OUT / f"step7_chg_alarm_config{name}.json"
        if p.exists():
            meta.setdefault("thresholds", {})[name or "spec"] = json.loads(
                p.read_text(encoding="utf-8"))

    # 교차검증 성능 (있으면)
    ev = OUT / "eval_chg_summary.json"
    if ev.exists():
        s = json.loads(ev.read_text(encoding="utf-8"))
        meta["cv"] = {
            "n_folds": s.get("n_folds"), "n_train_per_fold": s.get("n_train_per_fold"),
            "n_trials": s.get("n_trials"),
            "false_alarm": s.get("false_alarm"),
            "headline": s.get("headline"),
        }
    return meta


def snapshot(tag: str = "", verbose: bool = True) -> Path | None:
    missing = [f for f in REQUIRED if not (OUT / f).exists()]
    if missing:
        print(f"  [중단] 필수 파일 없음: {missing}")
        print(f"         python run_all.py 를 먼저 실행하세요")
        return None

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"{stamp}_{tag}" if tag else stamp
    dst = MODELS / name
    dst.mkdir(parents=True, exist_ok=True)

    copied = []
    for f in REQUIRED + OPTIONAL:
        src = OUT / f
        if src.exists():
            shutil.copy2(src, dst / f)
            copied.append((f, src.stat().st_size))

    meta = collect_meta(stamp, tag)
    (dst / "manifest.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False, default=float), encoding="utf-8")

    # 최신 스냅샷을 가리키는 포인터 (심볼릭 링크는 Windows 권한 문제가 있어 텍스트로)
    (MODELS / "LATEST.txt").write_text(name + "\n", encoding="utf-8")

    if verbose:
        total = sum(s for _, s in copied)
        print(f"  스냅샷 → models/{name}/   파일 {len(copied)}개 · {total / 1e6:.1f} MB")
        for f, s in copied:
            print(f"    {f:<38}{s:>12,} B")
        print(f"    {'manifest.json':<38}{(dst / 'manifest.json').stat().st_size:>12,} B  ← 재현 정보")
        d = meta.get("data", {})
        print(f"\n  학습 팩 {d.get('n_fit')}개 / 가용 {d.get('n_valid')}개"
              f"  ·  FIT_ON_ALL={meta['params']['fit_on_all']}")
        if "model_op" in meta:
            print(f"  운영 모델 주성분 {meta['model_op']['n_components']}개, "
                  f"입력 {meta['model_op']['dim']}차원")
        if "cv" in meta and meta["cv"].get("false_alarm"):
            fa = meta["cv"]["false_alarm"]
            print(f"  교차검증 FAR {fa.get('per_hour', float('nan')):.3f} 건/시간, "
                  f"울린 팩 {fa.get('n_packs_fired')}/{fa.get('n_packs')}")
    return dst


def list_snapshots() -> None:
    if not MODELS.exists():
        print("  models/ 없음 — 아직 스냅샷이 없습니다")
        return
    latest = (MODELS / "LATEST.txt").read_text(encoding="utf-8").strip() \
        if (MODELS / "LATEST.txt").exists() else ""
    dirs = sorted([d for d in MODELS.iterdir() if d.is_dir()])
    print(f"  보관된 스냅샷 {len(dirs)}개\n")
    print(f"  {'이름':<28}{'학습팩':>7}{'주성분':>7}{'FAR':>9}{'크기':>9}   최신")
    for d in dirs:
        mp = d / "manifest.json"
        m = json.loads(mp.read_text(encoding="utf-8")) if mp.exists() else {}
        n_fit = m.get("data", {}).get("n_fit", "?")
        nc = m.get("model_op", {}).get("n_components", "?")
        fa = m.get("cv", {}).get("false_alarm", {}).get("per_hour")
        size = sum(f.stat().st_size for f in d.iterdir() if f.is_file())
        print(f"  {d.name:<28}{n_fit:>7}{nc:>7}"
              f"{(f'{fa:.3f}' if fa is not None else '—'):>9}{size / 1e6:>8.1f}M"
              f"   {'←' if d.name == latest else ''}")


def main() -> int:
    ap = argparse.ArgumentParser(description="학습된 모델 스냅샷 보관")
    ap.add_argument("--tag", default="", help="스냅샷 이름 뒤에 붙일 태그")
    ap.add_argument("--list", action="store_true", help="보관된 스냅샷 목록")
    args = ap.parse_args()
    if args.list:
        list_snapshots()
        return 0
    return 0 if snapshot(args.tag) else 1


if __name__ == "__main__":
    raise SystemExit(main())
