"""오토인코더 이상탐지 모델 학습.

    db/data/*_chg.csv  --pack_loader-->  PackData  --fit-->  models/battery_anomaly.pkl

    python train_anomaly.py                 # 학습 + 데모 9팩 검증
    python train_anomaly.py --no-validate   # 학습만
    python train_anomaly.py --out models/실험.pkl

정상 팩만 학습한다(데모 팩과 1043 은 pack_loader.training_packs 가 뺀다).
임계는 leave-one-pack-out 으로 정한다 - 자기 자신이 학습에 든 채로 채점하면
점수가 낮게 나와 임계가 과소 설정된다. 그래서 학습이 오래 걸린다(팩 수 x AE 2개).

검증은 데모 9팩을 판정해 database.DEMO_PACKS 의 정답과 대조한다. 정답표를
여기 복제하지 않는 이유: 답이 두 곳에 있으면 한쪽만 고쳤을 때 알 수 없다.
"""
import argparse
import time
from pathlib import Path

import database
import pack_loader
from battery_anomaly import STREAMS, BatteryAnomalyModel

DEFAULT_OUT = Path("models/battery_anomaly.pkl")

# 정답표의 '정상 (검출한계 미만)' 처럼 괄호로 단서를 단 표기를 판정 라벨과
# 맞추기 위한 정리. 모델이 내는 것은 괄호 없는 라벨뿐이다.
def _expected(text: str) -> str:
    return text.split(" (")[0]


def validate(model: BatteryAnomalyModel, data_dir: Path) -> int:
    """데모 팩을 판정해 정답과 대조한다. 맞힌 개수를 돌려준다."""
    by_pack_id = {m["pack_id"]: m for m in database.DEMO_PACKS.values()}
    paths = sorted(data_dir.glob("DEMO*_chg.csv"))
    if not paths:
        print(f"\n{data_dir} 에 데모 팩이 없어 검증을 건너뛴다.")
        return 0

    print(f"\n{'팩':<8}{'주입':<16}{'모델 판정':<16}{'지목':<22}{'결과'}")
    print("-" * 72)
    ok = 0
    for path in paths:
        pack = pack_loader.from_csv(path)
        answer = by_pack_id.get(pack.pack_id)
        if answer is None:
            print(f"{pack.pack_id:<8}정답표에 없는 팩이라 건너뜀")
            continue

        v = model.predict(pack)
        verdict = ", ".join(v.fault_types) if v.fault_types else "정상"
        hit = verdict == _expected(answer["expect"])
        ok += hit
        found = ", ".join(d["component"] for d in v.detail.values() if d["hit"])
        print(f"{pack.pack_id:<8}{answer['fault'] or 'none':<16}{verdict:<16}"
              f"{found or '-':<22}{'O' if hit else 'X'}")

    print(f"\n일치 {ok}/{len(paths)}")

    print(f"\n스트림별 점수 (임계 대비)\n{'팩':<8}" +
          "".join(f"{s:>18}" for s in STREAMS))
    for path in paths:
        v = model.predict(pack_loader.from_csv(path))
        cells = []
        for name in STREAMS:
            d = v.detail[name]
            cells.append(f"{d['score']:8.2f}/{d['threshold']:6.2f}{'*' if d['hit'] else ' '}")
        print(f"{v.pack_id:<8}" + "".join(f"{c:>18}" for c in cells))
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, default=Path("db/data"))
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--no-validate", action="store_true",
                        help="데모 팩 검증을 건너뛴다")
    args = parser.parse_args()

    t0 = time.time()
    packs = pack_loader.training_packs(args.data_dir)
    if not packs:
        raise SystemExit(f"{args.data_dir} 에 학습할 충전 CSV 가 없다.")
    print(f"정상 팩 {len(packs)}개 로드 ({time.time() - t0:.1f}초)")
    print(f"  행 수 {min(len(p.soc) for p in packs)}~{max(len(p.soc) for p in packs)}")

    t0 = time.time()
    model = BatteryAnomalyModel().fit(packs)
    print(f"학습·임계 보정 완료 ({time.time() - t0:.1f}초)\n")
    print(model.report())

    args.out.parent.mkdir(parents=True, exist_ok=True)
    model.save(args.out)
    print(f"\n저장: {args.out}")

    if not args.no_validate:
        validate(model, args.data_dir)


if __name__ == "__main__":
    main()
