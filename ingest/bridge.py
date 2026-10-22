"""Consumer Kafka là thành phần duy nhất tạo DAG run ingest qua Airflow API."""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Consumer
from ingest.model import DAG_ID, READY_TOPIC, validate_ready

BASE = "http://127.0.0.1:8080/api/v2"


def trigger(message):
    validate_ready(message)
    batch_id = message["batch_id"]
    path = f"/dags/{DAG_ID}/dagRuns"
    data = {"dag_run_id": batch_id, "logical_date": None, "conf": {"batch_id": batch_id},
            "note": f"Kafka đã nhận đủ ngày mô phỏng {message['event_date']}"}
    req = urllib.request.Request(BASE + path, data=json.dumps(data).encode(), method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            result = json.load(response)
        if result["dag_run_id"] != batch_id:
            raise RuntimeError("Airflow trả sai mã DAG run.")
        return "created"
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise RuntimeError(f"Kích hoạt Airflow lỗi HTTP {exc.code}: {exc.read().decode(errors='replace')}") from exc
        with urllib.request.urlopen(BASE + path + "/" + batch_id, timeout=20) as response:
            existing = json.load(response)
        if existing.get("conf", {}).get("batch_id") != batch_id:
            raise RuntimeError("DAG run tồn tại nhưng mã lô không khớp.")
        return "already_exists"


def main():
    consumer = Consumer({"bootstrap.servers": os.getenv("CHICAGO_KAFKA_BROKERS", "127.0.0.1:19092"),
                         "group.id": "chicago-airflow-trigger", "auto.offset.reset": "earliest",
                         "enable.auto.commit": False})
    consumer.subscribe([READY_TOPIC])
    audit = Path("artifacts/kafka/bridge-audit.jsonl")
    audit.parent.mkdir(parents=True, exist_ok=True)
    print("Consumer: đang nghe thông báo hoàn tất ngày từ Kafka để kích hoạt Airflow.", flush=True)
    try:
        while True:
            record = consumer.poll(1)
            if record is None:
                continue
            if record.error():
                raise RuntimeError(f"Consumer Kafka lỗi: {record.error()}")
            message = validate_ready(json.loads(record.value()))
            stored = Path(f"artifacts/kafka/batches/{message['batch_id']}.json")
            if not stored.is_file() or json.loads(stored.read_text()) != message:
                raise RuntimeError("Thông báo Kafka không khớp manifest Producer.")
            for attempt in range(5):
                try:
                    status = trigger(message)
                    break
                except Exception:
                    if attempt == 4:
                        raise
                    time.sleep(3)
            with audit.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({"batch_id": message["batch_id"], "status": status,
                    "partition": record.partition(), "offset": record.offset(),
                    "created_at": datetime.now(timezone.utc).isoformat()}, ensure_ascii=False) + "\n")
            consumer.commit(message=record, asynchronous=False)
            print(f"Consumer: {message['batch_id']} → Airflow: {'đã tạo DAG run' if status == 'created' else 'DAG run đã có, không tạo trùng'}.", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
