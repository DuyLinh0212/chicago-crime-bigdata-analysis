"""Kiểm tra API và giao thức hoàn tất ngày; chỉ được workflow chạy trên GitHub."""
import copy
import io
import json
import unittest
import urllib.error
from datetime import date, datetime
from unittest.mock import patch
from fastapi.testclient import TestClient
from ingest.api import app
from ingest.model import DATES, digest, events_for_day, jsonl_record, validate_batch_id, validate_ready
from ingest.bridge import trigger
from ci.run_kafka import wait_day


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def ready(self):
        return {"batch_id": "demo-" + DATES[0], "event_date": DATES[0], "simulation": True,
                "expected_unique_count": 10, "record_count": 11, "events_sha256": digest(events_for_day(DATES[0])),
                "start_offsets": {"0": 0, "1": 0, "2": 0}, "end_offsets": {"0": 4, "1": 4, "2": 3}}

    def test_api_deterministic_and_unique(self):
        ids = set()
        for day in DATES:
            first = self.client.get("/crimes", params={"date": day})
            second = self.client.get("/crimes", params={"date": day})
            self.assertEqual(first.status_code, 200)
            self.assertEqual(first.json(), second.json())
            events = first.json()["events"]
            self.assertEqual(len(events), 10)
            self.assertTrue(all(e["simulation"] and e["payload"]["city_block"].startswith("CI-DEMO-") for e in events))
            ids.update(e["event_id"] for e in events)
        self.assertEqual(len(ids), 30)

    def test_invalid_date(self):
        for value in ("2024-01-01", "not-a-date", "2023-12-28"):
            self.assertEqual(self.client.get("/crimes", params={"date": value}).status_code, 422)

    def test_health(self):
        self.assertTrue(self.client.get("/health").json()["simulation"])

    def test_bronze_dates_serialize_to_jsonl(self):
        record = jsonl_record({"ingest_date": date(2026, 10, 22), "kafka_timestamp": datetime(2026, 10, 22, 12)})
        self.assertTrue(record.endswith("\n"))
        self.assertEqual(json.loads(record)["ingest_date"], "2026-10-22")
        self.assertIsInstance(json.loads(record)["kafka_timestamp"], str)

    def test_ready_protocol(self):
        self.assertEqual(validate_ready(self.ready()), self.ready())
        for key, value in (("events_sha256", "wrong"), ("expected_unique_count", 9), ("record_count", 10),
                           ("start_offsets", {"0": 0}), ("event_date", DATES[1])):
            ready = copy.deepcopy(self.ready())
            ready[key] = value
            with self.assertRaises(ValueError):
                validate_ready(ready)

    def test_batch_id_cannot_traverse(self):
        for value in ("../../silver", "demo-2023-12-32", "demo-2023-12-29;exit", None):
            with self.assertRaises(ValueError):
                validate_batch_id(value)

    def test_bridge_creates_once_and_checks_existing_conf(self):
        ready = self.ready()
        run = {"dag_run_id": ready["batch_id"], "conf": {"batch_id": ready["batch_id"]}}
        with patch("ingest.bridge.urllib.request.urlopen", return_value=io.BytesIO(json.dumps(run).encode())):
            self.assertEqual(trigger(ready), "created")
        conflict = urllib.error.HTTPError("http://airflow", 409, "Conflict", {}, io.BytesIO(b"duplicate"))
        with patch("ingest.bridge.urllib.request.urlopen", side_effect=[conflict, io.BytesIO(json.dumps(run).encode())]):
            self.assertEqual(trigger(ready), "already_exists")
        wrong = {**run, "conf": {"batch_id": "wrong"}}
        conflict = urllib.error.HTTPError("http://airflow", 409, "Conflict", {}, io.BytesIO(b"duplicate"))
        with patch("ingest.bridge.urllib.request.urlopen", side_effect=[conflict, io.BytesIO(json.dumps(wrong).encode())]):
            with self.assertRaises(RuntimeError):
                trigger(ready)

    def test_bridge_does_not_mask_server_error(self):
        failure = urllib.error.HTTPError("http://airflow", 500, "Error", {}, io.BytesIO(b"error"))
        with patch("ingest.bridge.urllib.request.urlopen", side_effect=failure):
            with self.assertRaises(RuntimeError):
                trigger(self.ready())

    def test_failed_dag_fails_ci_controller(self):
        with patch("ci.run_kafka.check_processes"), patch("ci.run_kafka.save"), patch("ci.run_kafka.request", side_effect=[
            {"state": "failed"}, {"task_instances": [{"state": "failed", "task_id": "bo_sung_parquet"}]}
        ]):
            with self.assertRaisesRegex(RuntimeError, "workflow phải thất bại"):
                wait_day(DATES[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
