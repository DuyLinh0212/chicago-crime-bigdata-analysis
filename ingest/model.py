"""Dữ liệu giả có thể tái lập; không dùng làm số liệu tội phạm thực tế."""
import hashlib
import json
import re

DATES = ("2023-12-29", "2023-12-30", "2023-12-31")
EVENT_TOPIC = "chicago-crimes-events"
READY_TOPIC = "chicago-crimes-day-ready"
DAG_ID = "chicago_crimes_kafka_daily"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def validate_batch_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"demo-2023-12-(29|30|31)", value):
        raise ValueError("Mã lô mô phỏng không hợp lệ.")
    return value


def events_for_day(day):
    if day not in DATES:
        raise ValueError("Chỉ mô phỏng các ngày 29–31/12/2023.")
    events = []
    for index in range(10):
        payload = {
            "date_reported": f"{day}T{(index * 2) % 24:02d}:{index:02d}:00",
            "city_block": f"CI-DEMO-{day}-{index + 1:03d} (MO PHONG)",
            "primary_type": ("THEFT", "ROBBERY", "ASSAULT", "BATTERY", "HOMICIDE")[index % 5],
            "primary_description": "SYNTHETIC CI EVENT",
            "location_description": "STREET" if index % 2 else "RESIDENCE",
            "arrest": "true" if index % 3 == 0 else "false",
            "domestic": "false", "community_area": str((25, 8, 28, 43, 32)[index % 5]),
            "year": "2023", "latitude": None if index % 3 == 0 else "41.88",
            "longitude": None if index % 3 == 0 else "-87.63", "location": None,
        }
        events.append({"event_id": digest({"day": day, "index": index}),
                       "event_date": day, "simulation": True, "payload": payload})
    return events


def validate_ready(message):
    batch_id = validate_batch_id(message.get("batch_id"))
    day = batch_id[5:]
    expected = events_for_day(day)
    if (message.get("simulation") is not True or message.get("event_date") != day or message.get("expected_unique_count") != 10
            or message.get("events_sha256") != digest(expected) or message.get("record_count") != 11):
        raise ValueError("Thông báo hoàn tất ngày không khớp dữ liệu mô phỏng.")
    starts, ends = message.get("start_offsets", {}), message.get("end_offsets", {})
    if set(starts) != {"0", "1", "2"} or set(ends) != set(starts):
        raise ValueError("Thông báo thiếu offset của ba partition.")
    if any(type(starts[k]) is not int or type(ends[k]) is not int or starts[k] < 0 or ends[k] < starts[k] for k in starts):
        raise ValueError("Khoảng offset Kafka không hợp lệ.")
    if sum(ends[k] - starts[k] for k in starts) != 11:
        raise ValueError("Khoảng offset phải chứa đúng 11 thông điệp (10 vụ và một bản gửi lại).")
    return message
