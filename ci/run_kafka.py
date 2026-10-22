"""Khởi động dịch vụ thật và chờ ba DAG run do Kafka consumer tạo ra."""
import json
import os
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

from ci.run_airflow import start_airflow, request, save
from ingest.model import DATES, DAG_ID

ROOT = Path(__file__).resolve().parents[1]
SERVICES = []


def start(name, args):
    with (ROOT / f"artifacts/logs/{name}.log").open("w") as stream:
        process = subprocess.Popen([sys.executable, *args], cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
    SERVICES.append({"name": name, "pid": process.pid, "process": process})
    save("kafka-processes.json", [{"name": p["name"], "pid": p["pid"]} for p in SERVICES])


def check_processes():
    for item in SERVICES:
        if item["process"].poll() is not None:
            raise RuntimeError(f"Dịch vụ {item['name']} đã dừng ngoài dự kiến; xem log artifact.")


def logged(name, args, env=None):
    with (ROOT / f"artifacts/logs/{name}.log").open("w") as stream:
        with subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, bufsize=1) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                stream.write(line)
                stream.flush()
            if process.wait() != 0:
                raise RuntimeError(f"Bước {name} thất bại; xem log đã xuất.")


def wait_day(day):
    run_id = "demo-" + day
    deadline = time.monotonic() + 2100
    created_deadline = time.monotonic() + 120
    previous = None
    while time.monotonic() < deadline:
        check_processes()
        try:
            run = request(f"/dags/{DAG_ID}/dagRuns/{run_id}")
        except RuntimeError:
            if time.monotonic() > created_deadline:
                raise
            time.sleep(3)
            continue
        tasks = request(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances?limit=100")["task_instances"]
        state = {"dag_run": run, "tasks": tasks}
        save(f"kafka-airflow-{day}.json", state)
        progress = (run["state"], str(dict(Counter(t["state"] or "chưa chạy" for t in tasks))))
        if progress != previous:
            print(f"Ngày {day} | DAG do Kafka kích hoạt: {progress[0]} | {progress[1]}", flush=True)
            previous = progress
        if run["state"] == "failed":
            raise RuntimeError(f"DAG ngày {day} thất bại; workflow phải thất bại.")
        if run["state"] == "success":
            if len(tasks) != 12 or any(t["state"] != "success" for t in tasks):
                raise RuntimeError("DAG báo thành công nhưng chưa đủ 12 task thành công.")
            if day == DATES[0] and not any(t["task_id"] == "bo_sung_parquet" and t["try_number"] >= 2 for t in tasks):
                raise RuntimeError("Chưa chứng minh Airflow thử lại sau lỗi chuyển file.")
            return state
        time.sleep(5)
    raise RuntimeError(f"DAG ngày {day} vượt thời gian chờ.")


def wait_audit():
    path = ROOT / "artifacts/kafka/bridge-audit.jsonl"
    for _ in range(20):
        check_processes()
        audit = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        if len(audit) == 6:
            if Counter(row["status"] for row in audit) != {"created": 3, "already_exists": 3}:
                raise RuntimeError("Consumer chưa chống kích hoạt trùng đúng kỳ vọng.")
            return audit
        time.sleep(1)
    raise RuntimeError("Chưa nhận đủ sáu xác nhận của consumer Kafka.")


def main():
    start_airflow(DAG_ID)
    start("kafka-api", ["-m", "uvicorn", "ingest.api:app", "--host", "127.0.0.1", "--port", "18080"])
    for attempt in range(40):
        check_processes()
        try:
            with urllib.request.urlopen("http://127.0.0.1:18080/health", timeout=2) as response:
                if json.load(response)["simulation"]:
                    break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("API mô phỏng chưa sẵn sàng.")
    logged("kafka-topics", ["-m", "ingest.producer", "--create-topics"])
    start("kafka-consumer", ["-m", "ingest.bridge"])
    for day in DATES:
        logged(f"kafka-producer-{day}", ["-m", "ingest.producer", "--date", day])
        wait_day(day)
        env = {**os.environ, "CHICAGO_BATCH_ID": "demo-" + day, "CHICAGO_SIMULATION_DATE": day,
               "CHICAGO_ARTIFACT_DIR": f"artifacts/days/{day}", "CHICAGO_RUN_ID": os.environ["CHICAGO_RUN_ID"] + "-" + day}
        logged(f"kafka-screenshots-{day}", ["screenshots/capture_evidence.py"], env=env)
        print(f"Đã hoàn tất ngày {day}: 10 vụ mới, đủ phân tích, báo cáo và ảnh bằng chứng.", flush=True)
    audit = wait_audit()
    runs = request(f"/dags/{DAG_ID}/dagRuns?limit=100")["dag_runs"]
    if len(runs) != 3 or {r["dag_run_id"] for r in runs} != {"demo-" + day for day in DATES} or any(r["state"] != "success" for r in runs):
        raise RuntimeError("Không có đúng ba DAG run thành công từ Kafka.")
    save("kafka-airflow-validation.json", {"commit": os.environ["GITHUB_SHA"], "run_id": os.environ["CHICAGO_RUN_ID"],
        "dag_runs": runs, "consumer_acknowledgements": audit, "successful_runs": 3, "successful_tasks_per_run": 12})
    # Chạy lại lô đã commit để kiểm tra không tăng dòng sau khi hoàn thành.
    logged("kafka-replay-committed", ["-m", "ingest.stages", "append"], env={**os.environ, "CHICAGO_BATCH_ID": "demo-" + DATES[-1]})
    logged("kafka-replay-verify", ["-m", "ingest.stages", "verify"], env={**os.environ, "CHICAGO_BATCH_ID": "demo-" + DATES[-1]})


if __name__ == "__main__":
    main()
