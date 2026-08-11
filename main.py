"""FastAPI 껍데기. 여기에 실제 엔드포인트를 채워 나간다."""

import os

from fastapi import FastAPI

app = FastAPI(title="dockerimage")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/appdb")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")


@app.get("/health")
def health():
    return {"status": "ok"}