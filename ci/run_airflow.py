"""Khởi động Airflow thật, kiểm tra DAG lỗi và chạy pipeline đầy đủ trên GitHub."""
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AIRFLOW = ROOT / ".venv-airflow/bin/airflow"
LOGS = ROOT / "artifacts/logs"
MANIFESTS = ROOT / "artifacts/manifests"
BASE = "http://127.0.0.1:8080/api/v2"
DAG_ID = "chicago_crimes_pipeline"


def request(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Airflow API {method} {path}: HTTP {exc.code}: {detail}") from exc


def save(name, value):
    (MANIFESTS / name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def snapshot(run_id):
    dag_run = request(f"/dags/{DAG_ID}/dagRuns/{run_id}")
    tasks = request(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances?limit=100")
    result = {"dag_run": dag_run, "tasks": tasks["task_instances"]}
    save(f"airflow-{run_id}.json", result)
    return result


def trigger_and_wait(run_id, force_failure):
    request(f"/dags/{DAG_ID}/dagRuns", "POST", {
        "dag_run_id": run_id, "logical_date": None,
        "conf": {"force_failure": force_failure},
        "note": "Thử lỗi có chủ đích" if force_failure else "Chạy toàn bộ sáu CSV trên GitHub",
    })
    deadline = time.monotonic() + (180 if force_failure else 2400)
    previous = None
    while time.monotonic() < deadline:
        state = snapshot(run_id)
        counts = dict(Counter(task["state"] or "chưa chạy" for task in state["tasks"]))
        progress = (state["dag_run"]["state"], str(counts))
        if progress != previous:
            print(f"DAG {run_id}: {progress[0]}; trạng thái các bước: {counts}", flush=True)
            previous = progress
        if state["dag_run"]["state"] in ("success", "failed"):
            expected = "failed" if force_failure else "success"
            if state["dag_run"]["state"] != expected:
                raise RuntimeError(f"DAG kết thúc {state['dag_run']['state']}, cần {expected}.")
            if force_failure:
                if not any(task["task_id"] == "kiem_tra_hdfs" and task["state"] == "failed" for task in state["tasks"]):
                    raise RuntimeError("DAG lỗi nhưng không đúng bước kiểm tra chủ đích.")
                print("Đã xác nhận lỗi task làm DAG thất bại; các bước sau không chạy thành công giả.", flush=True)
            else:
                if len(state["tasks"]) != 13 or any(task["state"] != "success" for task in state["tasks"]):
                    raise RuntimeError("DAG chưa có đủ 13 task thành công.")
                print("Đã xác nhận đủ 13 task Airflow thành công trên toàn bộ dữ liệu.", flush=True)
            return state
        time.sleep(5)
    raise RuntimeError(f"DAG {run_id} vượt thời gian chờ.")


def main():
    LOGS.mkdir(parents=True, exist_ok=True)
    MANIFESTS.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(AIRFLOW), "db", "migrate"], cwd=ROOT, check=True, timeout=180)
    processes = []
    for name, args in [("api", ["api-server", "--host", "127.0.0.1", "--port", "8080"]),
                       ("scheduler", ["scheduler"]), ("dag-processor", ["dag-processor"])]:
        with (LOGS / f"airflow-{name}.log").open("w") as stream:
            process = subprocess.Popen([str(AIRFLOW), *args], cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        processes.append({"name": name, "pid": process.pid})
    save("airflow-processes.json", processes)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            request(f"/dags/{DAG_ID}")
            break
        except Exception:
            time.sleep(3)
    else:
        for name in ("api", "scheduler", "dag-processor"):
            print((LOGS / f"airflow-{name}.log").read_text(errors="replace")[-12000:], flush=True)
        raise RuntimeError("Airflow API hoặc DAG chưa sẵn sàng sau 180 giây.")
    request(f"/dags/{DAG_ID}", "PATCH", {"is_paused": False})
    run = os.environ["CHICAGO_RUN_ID"]
    negative = trigger_and_wait(f"manual__ci_{run}_thu_loi", True)
    positive = trigger_and_wait(f"manual__ci_{run}_thanh_cong", False)
    save("airflow-validation.json", {"commit": os.environ["GITHUB_SHA"], "run_id": run,
         "negative_dag_run": negative["dag_run"]["dag_run_id"], "negative_state": "failed",
         "positive_dag_run": positive["dag_run"]["dag_run_id"], "positive_state": "success", "successful_tasks": 13})
    print("Đã hoàn tất kiểm tra Airflow: một lần lỗi đúng kỳ vọng, một lần chạy đầy đủ thành công.", flush=True)


if __name__ == "__main__":
    main()
