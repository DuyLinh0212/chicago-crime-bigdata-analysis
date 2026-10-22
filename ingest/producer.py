"""Gọi API, chờ broker xác nhận và phát thông báo hoàn tất ngày."""
import argparse
import json
import os
import urllib.request
from pathlib import Path
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from ingest.model import DATES, EVENT_TOPIC, READY_TOPIC, digest, events_for_day, validate_ready

BROKERS = os.getenv("CHICAGO_KAFKA_BROKERS", "127.0.0.1:19092")


def create_topics():
    admin = AdminClient({"bootstrap.servers": BROKERS})
    existing = admin.list_topics(timeout=30).topics
    requested = [NewTopic(name, num_partitions=count, replication_factor=1)
                 for name, count in ((EVENT_TOPIC, 3), (READY_TOPIC, 1)) if name not in existing]
    for name, future in admin.create_topics(requested).items() if requested else []:
        future.result(timeout=30)
        print(f"Đã tạo topic {name}.", flush=True)
    metadata = admin.list_topics(timeout=30)
    for name, count in ((EVENT_TOPIC, 3), (READY_TOPIC, 1)):
        if name not in metadata.topics or len(metadata.topics[name].partitions) != count:
            raise RuntimeError(f"Số partition topic {name} chưa đúng.")


def publish_day(day):
    with urllib.request.urlopen(f"http://127.0.0.1:18080/crimes?date={day}", timeout=15) as response:
        payload = json.load(response)
    events = payload["events"]
    if events != events_for_day(day) or payload["sha256"] != digest(events):
        raise RuntimeError("API không trả đúng mười sự kiện mô phỏng đã xác định.")
    print(f"Producer: API trả đúng 10 vụ giả ngày {day}.", flush=True)
    reader = Consumer({"bootstrap.servers": BROKERS, "group.id": "ci-offset-inspector", "enable.auto.commit": False})
    try:
        def offsets():
            return {str(p): reader.get_watermark_offsets(TopicPartition(EVENT_TOPIC, p), timeout=15, cached=False)[1] for p in range(3)}
        starts = offsets()
        producer = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 60000})
        deliveries, errors = [], []

        def delivered(error, message):
            if error:
                errors.append(str(error))
            else:
                deliveries.append({"partition": message.partition(), "offset": message.offset()})

        for index, event in enumerate(events + events[:1]):
            producer.produce(EVENT_TOPIC, key=event["event_id"], value=json.dumps(event, ensure_ascii=False).encode(),
                             partition=index % 3, on_delivery=delivered)
            producer.poll(0)
        if producer.flush(75) or errors or len(deliveries) != 11:
            raise RuntimeError(f"Kafka chưa xác nhận đủ sự kiện: {errors}")
        ends = offsets()
        ready = validate_ready({"batch_id": f"demo-{day}", "event_date": day, "simulation": True,
                "expected_unique_count": 10, "record_count": 11, "events_sha256": digest(events),
                "start_offsets": starts, "end_offsets": ends})
        folder = Path("artifacts/kafka/batches")
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"demo-{day}.json").write_text(json.dumps(ready, ensure_ascii=False, indent=2), encoding="utf-8")
        (folder / f"demo-{day}-events.json").write_text(json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8")
        deliveries.clear()
        for _ in range(2):
            producer.produce(READY_TOPIC, key=ready["batch_id"], value=json.dumps(ready).encode(), on_delivery=delivered)
        if producer.flush(75) or errors or len(deliveries) != 2:
            raise RuntimeError("Không gửi được thông báo hoàn tất ngày.")
        print(f"Producer: Kafka đã xác nhận 11 thông điệp, gồm 10 vụ duy nhất và một bản gửi lại; ngày {day}.", flush=True)
        print(f"Producer: đã gửi day_ready hai lần để kiểm tra chống kích hoạt trùng; offsets {starts} → {ends}.", flush=True)
        return ready
    finally:
        reader.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", choices=DATES)
    parser.add_argument("--create-topics", action="store_true")
    args = parser.parse_args()
    if args.create_topics:
        create_topics()
    if args.date:
        publish_day(args.date)
