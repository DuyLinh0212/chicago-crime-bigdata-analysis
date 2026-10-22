"""CLI dùng chung cho workflow và DAG; các lệnh Docker chỉ chạy trên runner."""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "ci/compose.yaml"), "-p", os.getenv("COMPOSE_PROJECT_NAME", "chicago-ci")]
ARTIFACTS = ROOT / "artifacts"


def run(args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, cwd=ROOT, **kwargs)


def hdfs(*args, **kwargs):
    return run([*COMPOSE, "exec", "-T", "namenode", "hdfs", *args], **kwargs)


def save_json(name, value):
    target = ARTIFACTS / "manifests" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def spark_job(script):
    run([*COMPOSE, "run", "--rm", "--no-deps", "spark-job", f"/opt/project/{script}"])


def prepare():
    for folder in ("logs", "manifests", "screenshots", "silver"):
        (ARTIFACTS / folder).mkdir(parents=True, exist_ok=True)
    files = sorted((ROOT / "data").glob("*.csv"))
    expected = json.loads((ROOT / "data/checksums.json").read_text(encoding="utf-8"))
    actual = {p.name: sha256(p) for p in files}
    if len(files) != 6 or actual != expected:
        raise RuntimeError("Sáu CSV không khớp checksum đã commit.")
    save_json("source.json", {"commit": os.getenv("GITHUB_SHA"), "run_id": os.getenv("CHICAGO_RUN_ID"),
                             "created_at": datetime.now(timezone.utc).isoformat(), "files": actual})
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        report = hdfs("dfsadmin", "-report", "-live", capture_output=True, text=True).stdout
        if re.search(r"Live datanodes\s*\(3\)", report):
            print(report, flush=True)
            (ARTIFACTS / "logs/hdfs-report.log").write_text(report, encoding="utf-8")
            break
        time.sleep(3)
    else:
        raise RuntimeError("Chưa đủ ba DataNode hoạt động sau 120 giây.")
    paths = ["/chicago-crimes/bronze", "/chicago-crimes/gold"] + [
        f"/chicago-crimes/silver/year={y}/month={m:02d}" for y in range(2018, 2024) for m in range(1, 13)]
    hdfs("dfs", "-mkdir", "-p", *paths)
    print("Đã sẵn sàng 3 DataNode và cấu trúc Bronze/Silver/Gold; replication=2, block size=128 MiB.", flush=True)


def upload():
    ingest = os.getenv("CHICAGO_INGEST_DATE", date.today().isoformat())
    run([sys.executable, ROOT / "upload_to_hdfs.py", "--backend", "docker", "--compose-file",
         ROOT / "ci/compose.yaml", "--project-name", os.getenv("COMPOSE_PROJECT_NAME", "chicago-ci"),
         "--ingest-date", ingest])
    verified = {}
    for file in sorted((ROOT / "data").glob("*.csv")):
        target = f"/chicago-crimes/bronze/ingest_date={ingest}/{file.name}"
        digest = hashlib.sha256()
        with subprocess.Popen([*COMPOSE, "exec", "-T", "namenode", "hdfs", "dfs", "-cat", target],
                              cwd=ROOT, stdout=subprocess.PIPE) as process:
            for chunk in iter(lambda: process.stdout.read(1024 * 1024), b""):
                digest.update(chunk)
            if process.wait() != 0 or digest.hexdigest() != sha256(file):
                raise RuntimeError(f"Checksum Bronze không khớp: {file.name}")
        verified[file.name] = digest.hexdigest()
    save_json("bronze.json", {"ingest_date": ingest, "verified_sha256": verified})
    hdfs("dfs", "-ls", "-R", "/chicago-crimes/bronze")
    hdfs("fsck", "/chicago-crimes/bronze", "-files", "-blocks", "-locations")
    print("Đã xác nhận checksum sáu file trên Bronze trùng nguồn.", flush=True)


def export():
    hdfs("dfs", "-get", "-f", "/chicago-crimes/silver", "/artifacts/")
    hdfs("dfs", "-ls", "-R", "/chicago-crimes/silver")
    print("Đã xuất Parquet Silver và manifest vào artifact.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["prepare", "upload", "normalize", "verify", "export", "checks"])
    stage = parser.parse_args().stage
    print(f"Bắt đầu bước: {stage}", flush=True)
    actions = {"prepare": prepare, "upload": upload, "normalize": lambda: spark_job("pipeline/normalize_job.py"),
               "verify": lambda: spark_job("pipeline/verify_hdfs.py"), "export": export,
               "checks": lambda: spark_job("tests/test_normalize.py")}
    try:
        actions[stage]()
    except Exception as exc:
        print(f"Bước {stage} thất bại: {exc}", file=sys.stderr, flush=True)
        return 1
    print(f"Đã hoàn tất bước: {stage}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
