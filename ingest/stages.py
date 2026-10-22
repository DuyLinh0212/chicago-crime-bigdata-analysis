"""Các task Kafka dùng chung stage phân tích/báo cáo với pipeline batch."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from ingest.model import validate_batch_id, validate_ready

ROOT = Path(__file__).resolve().parents[1]


def context():
    batch_id = validate_batch_id(os.environ["CHICAGO_BATCH_ID"])
    ready = validate_ready(json.loads((ROOT / f"artifacts/kafka/batches/{batch_id}.json").read_text()))
    day = ready["event_date"]
    os.environ["CHICAGO_RUN_ID"] = os.environ["CHICAGO_RUN_ID"] + "-" + day
    os.environ["CHICAGO_ARTIFACT_DIR"] = f"artifacts/days/{day}"
    os.environ["CHICAGO_SIMULATION_DATE"] = day
    os.environ["CHICAGO_SPARK_SERVICE"] = "spark-kafka"
    os.environ["CHICAGO_FAIL_AFTER_PROMOTE"] = "1" if day == "2023-12-29" and os.getenv("CHICAGO_TEST_RETRY") == "1" else "0"
    output = ROOT / os.environ["CHICAGO_ARTIFACT_DIR"]
    for folder in ("logs", "manifests", "screenshots"):
        (output / folder).mkdir(parents=True, exist_ok=True)
    return ready, output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["check", "ingest", "append", "verify", "temporal", "crime-types", "spatial", "cross", "report", "report-image", "publish-report", "validate-all"])
    stage = parser.parse_args().stage
    ready, output = context()
    # Import sau khi thiết lập thư mục và mã lần chạy theo ngày.
    from pipeline import stages as batch
    print(f"Ngày {ready['event_date']} | Lô {ready['batch_id']} | Bắt đầu bước {stage}", flush=True)
    if stage == "check":
        if not (ROOT / "artifacts/manifests/baseline-silver.json").is_file():
            raise RuntimeError("Chưa lưu kiểm chứng Silver nền.")
        report = batch.hdfs("dfsadmin", "-report", "-live", capture_output=True, text=True).stdout
        if "Live datanodes (3)" not in report:
            raise RuntimeError("Cụm chưa đủ ba DataNode hoạt động.")
        print(report, flush=True)
    elif stage in ("ingest", "append"):
        batch.run([*batch.COMPOSE, "run", "--rm", "--no-deps", "spark-kafka", "/opt/project/ingest/spark_job.py", stage])
    elif stage == "report-image":
        subprocess.run([sys.executable, "screenshots/capture_evidence.py", "--report-only"], cwd=ROOT, check=True)
    else:
        subprocess.run([sys.executable, "-m", "pipeline.stages", stage], cwd=ROOT, check=True)
    print(f"Ngày {ready['event_date']} | Đã hoàn tất bước {stage}.", flush=True)


if __name__ == "__main__":
    main()
