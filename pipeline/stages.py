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
ARTIFACTS = ROOT / os.getenv("CHICAGO_ARTIFACT_DIR", "artifacts")


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
    run([*COMPOSE, "run", "--rm", "--no-deps", os.getenv("CHICAGO_SPARK_SERVICE", "spark-job"), f"/opt/project/{script}"])


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


def publish_report():
    folder = ARTIFACTS / "report"
    image = folder / "report-overview.png"
    if not image.is_file():
        raise RuntimeError("Chưa có ảnh tổng quan báo cáo.")
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = [item for item in manifest["files"] if item["name"] != image.name] + [
        {"name": image.name, "sha256": sha256(image), "bytes": image.stat().st_size}]
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    target = f"/chicago-crimes/gold/{os.getenv('CHICAGO_RUN_ID', 'manual')}/report"
    mounted = "/artifacts/" + str(ARTIFACTS.relative_to(ROOT / "artifacts")).replace("\\", "/").strip(".")
    hdfs("dfs", "-put", "-f", f"{mounted}/report/report-overview.png", f"{mounted}/report/manifest.json", target)
    hdfs("dfs", "-ls", "-R", f"/chicago-crimes/gold/{os.getenv('CHICAGO_RUN_ID', 'manual')}")
    print("Đã ghi ảnh báo cáo và manifest cập nhật lên Gold.", flush=True)


def report():
    spark_job("pipeline/report_job.py")
    # Spark chạy root trong container; chuyển riêng đầu ra báo cáo cho user runner
    # để trình duyệt trên host có thể ghi PNG và cập nhật manifest.
    run([*COMPOSE, "run", "--rm", "--no-deps", "--entrypoint", "chown", "spark-job",
         "-R", f"{os.getuid()}:{os.getgid()}", f"/opt/project/{ARTIFACTS.relative_to(ROOT).as_posix()}/report"])


def validate_all():
    normalized = json.loads((ARTIFACTS / "manifests/normalize.json").read_text())
    verified = json.loads((ARTIFACTS / "manifests/hdfs-verification.json").read_text())
    charts = list((ARTIFACTS / "analystics").glob("*/*.png"))
    if len(charts) != 15 or normalized["output_rows"] != verified["silver_rows"]:
        raise RuntimeError("Số biểu đồ hoặc số dòng đầu ra chưa đúng.")
    run_id = os.environ["CHICAGO_RUN_ID"]
    for name in ("thoi_gian", "loai_toi_pham", "khong_gian", "phan_tich_cheo"):
        manifest = json.loads((ARTIFACTS / "analystics" / name / "manifest.json").read_text(encoding="utf-8"))
        if manifest["summary"]["so_vu"] != normalized["output_rows"] or manifest["run_id"] != run_id or manifest["commit"] != os.environ["GITHUB_SHA"]:
            raise RuntimeError(f"Manifest phân tích không khớp: {name}")
        hdfs("dfs", "-test", "-e", f"/chicago-crimes/gold/{run_id}/analystics/{name}/manifest.json")
    image = ARTIFACTS / "report/report-overview.png"
    remote = hdfs("dfs", "-cat", f"/chicago-crimes/gold/{run_id}/report/report-overview.png", capture_output=True).stdout
    if hashlib.sha256(remote).hexdigest() != sha256(image):
        raise RuntimeError("Ảnh báo cáo Gold không khớp artifact.")
    save_json("pipeline-validation.json", {"commit": os.environ["GITHUB_SHA"], "run_id": run_id,
              "silver_rows": normalized["output_rows"], "charts": len(charts), "groups": 4, "gold_report_verified": True})
    print("Đã xác nhận toàn bộ đầu ra: 15 biểu đồ, bốn phân tích, Silver và ảnh báo cáo trên Gold.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["prepare", "upload", "normalize", "verify", "export", "checks", "temporal", "crime-types", "spatial", "cross", "report", "publish-report", "validate-all"])
    stage = parser.parse_args().stage
    print(f"Bắt đầu bước: {stage}", flush=True)
    actions = {"prepare": prepare, "upload": upload, "normalize": lambda: spark_job("pipeline/normalize_job.py"),
               "verify": lambda: spark_job("pipeline/verify_hdfs.py"), "export": export,
               "checks": lambda: spark_job("tests/test_normalize.py"),
               "temporal": lambda: spark_job("analystics/temporal_analysis.py"),
               "crime-types": lambda: spark_job("analystics/crime_type_analysis.py"),
               "spatial": lambda: spark_job("analystics/spatial_analysis.py"),
               "cross": lambda: spark_job("analystics/cross_analysis.py"),
               "report": report,
               "publish-report": publish_report, "validate-all": validate_all}
    try:
        actions[stage]()
    except Exception as exc:
        print(f"Bước {stage} thất bại: {exc}", file=sys.stderr, flush=True)
        return 1
    print(f"Đã hoàn tất bước: {stage}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
