"""개발 환경이 제대로 떴는지만 확인하는 스모크 테스트.

앱 로직을 검증하는 곳이 아니라, 컨테이너를 받아서 띄웠을 때
'바로 개발할 수 있는 상태인가'를 확인하는 용도다.
"""

import json
import os
import sys
import time
import uuid

import psycopg
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/appdb")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:9092")


def test_toolchain():
    """이미지에 심어둔 파이썬과 주요 라이브러리가 쓸 수 있는 상태인지."""
    assert sys.version_info[:2] == (3, 13)

    import fastapi  # noqa: F401
    import pandas  # noqa: F401
    import sklearn  # noqa: F401
    import streamlit  # noqa: F401


def test_postgres_roundtrip():
    """DB 에 붙어서 쓰고 읽을 수 있는지."""
    with psycopg.connect(DATABASE_URL, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE smoke (id serial PRIMARY KEY, value text)")
            cur.execute("INSERT INTO smoke (value) VALUES (%s) RETURNING id", ("ok",))
            row_id = cur.fetchone()[0]

            cur.execute("SELECT value FROM smoke WHERE id = %s", (row_id,))
            assert cur.fetchone()[0] == "ok"


def test_kafka_roundtrip():
    """토픽을 만들고, 발행하고, 다시 읽어올 수 있는지."""
    topic = f"smoke-{uuid.uuid4().hex[:8]}"
    payload = {"hello": "kafka", "topic": topic}

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for future in admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    ).values():
        future.result(timeout=30)

    consumer = None
    try:
        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
        producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
        assert producer.flush(30) == 0, "브로커로 전송되지 않은 메시지가 남았습니다."

        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "group.id": f"{topic}-smoke",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            assert json.loads(message.value()) == payload
            return

        pytest.fail("60초 안에 메시지를 받지 못했습니다.")
    finally:
        if consumer is not None:
            consumer.close()
        for future in admin.delete_topics([topic]).values():
            try:
                future.result(timeout=30)
            except Exception:  # 정리 실패가 테스트 결과를 바꾸지 않게 한다
                pass
