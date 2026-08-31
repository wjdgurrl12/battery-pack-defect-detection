"""데모 CSV 9개를 Postgres 가 그대로 삼킬 수 있는 COPY 덤프로 바꾼다.

    python docker/export_demo_copy.py /seed/02_demo.sql.gz

이미지 빌드(Dockerfile 의 seedgen 스테이지)에서만 부른다. 결과 파일은
postgres-demo 이미지의 `/docker-entrypoint-initdb.d/` 에 놓이고, 컨테이너가
빈 데이터 디렉터리로 처음 뜰 때 공식 엔트리포인트가 한 번 실행한다.

**변환 규칙을 여기에 다시 적지 않는다.** 231컬럼 CSV 를 배열 둘로 접는 일은
load_raw.read_rows 가 이미 한다. 그 함수를 그대로 불러 쓰고, 이 파일은 결과를
COPY 텍스트 형식으로 받아 적기만 한다. 규칙을 SQL 이나 여기로 옮겨 적으면
적재 경로가 두 벌이 되어, 한쪽만 고쳤을 때 개발 DB 와 배포 DB 의 내용이
조용히 갈라진다.

왜 pg_dump 가 아니라 이 방식인가: 덤프를 뜨려면 이미 데이터가 든 Postgres 가
있어야 하고, 그러면 저장소에 수십 MB 짜리 산출물을 커밋해 두거나 빌드 중에
DB 를 띄워야 한다. CSV 는 이미 저장소에 있으므로 그것만으로 만든다.
"""

import gzip
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "/app")

import load_raw  # noqa: E402  (sys.path 를 먼저 세워야 한다)

# 데모 팩만 넣는다. 원본 50팩(600MB)은 이미지에 넣지 않는다 - 배포 이미지가
# 답해야 할 것은 '데모가 도는가' 지 '원본을 다 들고 있는가' 가 아니다.
PATTERN = "DEMO*_chg.csv"


def as_copy_value(value) -> str:
    """파이썬 값 하나를 COPY 텍스트 형식의 한 칸으로.

    형식은 Postgres 문서의 'Text Format' 그대로다.
      - NULL 은 `\\N`
      - 탭·개행·역슬래시는 이스케이프한다 (지금 데이터에는 없지만, 파일명이
        하나라도 이상해지면 열이 통째로 밀리는 종류의 사고라 막아 둔다)
      - 배열은 `{1.2,3.4,NULL}`. 배열 **안의** NULL 은 `\\N` 이 아니라
        대문자 NULL 이다. 두 자리에서 표기가 다르다.
    """
    if value is None:
        return r"\N"
    if isinstance(value, list):
        return "{" + ",".join(
            "NULL" if v is None else repr_number(v) for v in value) + "}"
    if isinstance(value, datetime):
        # timestamptz. 오프셋을 그대로 실어 보내면 서버 시간대와 무관하게
        # 같은 순간으로 들어간다.
        return value.isoformat(sep=" ")
    if isinstance(value, (int, float, Decimal)):
        return repr_number(value)
    return (str(value).replace("\\", "\\\\").replace("\t", "\\t")
            .replace("\n", "\\n").replace("\r", "\\r"))


def repr_number(value) -> str:
    """숫자를 왕복 가능한 최단 표기로. repr 이 그 일을 한다."""
    return repr(float(value)) if isinstance(value, float) else str(value)


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("사용법: python docker/export_demo_copy.py <출력 .sql.gz>")

    out = Path(sys.argv[1])
    out.parent.mkdir(parents=True, exist_ok=True)

    paths = sorted(load_raw.DATA_DIR.glob(PATTERN))
    if not paths:
        sys.exit(f"{load_raw.DATA_DIR} 에 {PATTERN} 가 없습니다. "
                 ".dockerignore 가 DEMO CSV 를 통과시키는지 확인하세요.")

    columns = ", ".join(load_raw.COPY_COLUMNS)
    total = 0

    # gzip 으로 쓴다. 공식 엔트리포인트가 *.sql.gz 를 알아서 풀어 실행하고,
    # 60MB 남짓한 텍스트가 이미지 레이어에서 그만큼 줄어든다.
    with gzip.open(out, "wt", encoding="utf-8", newline="\n") as fh:
        fh.write("-- Dockerfile 의 seedgen 스테이지가 생성한 파일이다. "
                 "직접 고치지 않는다.\n")
        fh.write(f"COPY pack_measurement ({columns}) FROM stdin;\n")
        for path in paths:
            rows = 0
            for record in load_raw.read_rows(path):
                fh.write("\t".join(as_copy_value(v) for v in record) + "\n")
                rows += 1
            print(f"  {path.name}: {rows:,}행")
            total += rows
        # COPY ... FROM stdin 은 이 한 줄로 끝난다. 빠뜨리면 psql 이 뒤따르는
        # 것을 전부 데이터로 읽어 적재가 통째로 깨진다.
        fh.write("\\.\n")

    size = out.stat().st_size / 1024 / 1024
    print(f"{out} — {len(paths)}개 파일 {total:,}행, {size:.1f}MB (gzip)")


if __name__ == "__main__":
    main()
