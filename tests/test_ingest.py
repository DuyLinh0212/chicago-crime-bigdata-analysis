"""Kiểm tra API và giao thức hoàn tất ngày; chỉ được workflow chạy trên GitHub."""
import copy
import unittest
from fastapi.testclient import TestClient
from ingest.api import app
from ingest.model import DATES, digest, events_for_day, validate_batch_id, validate_ready


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
