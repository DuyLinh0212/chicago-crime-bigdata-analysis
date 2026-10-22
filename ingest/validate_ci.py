"""Nghiệm thu ba ngày, 45 biểu đồ và bằng chứng từ dịch vụ thật trên GitHub."""
import json
import os
from pathlib import Path
from ingest.model import DATES


def main():
    root = Path("artifacts")
    base = json.loads((root / "manifests/baseline-silver.json").read_text())
    baseline = base["normalized"]["output_rows"]
    results = []
    for index, day in enumerate(DATES, 1):
        folder = root / "days" / day
        stats = json.loads((folder / "manifests/normalize.json").read_text())
        commit = json.loads((folder / "manifests/kafka-commit.json").read_text())
        hdfs = json.loads((folder / "manifests/hdfs-verification.json").read_text())
        pipeline = json.loads((folder / "manifests/pipeline-validation.json").read_text())
        progress = json.loads((folder / "manifests/kafka-progress.json").read_text())
        screenshots = json.loads((folder / "screenshots/capture-manifest.json").read_text())
        expected = baseline + 10 * index
        if any(value != expected for value in (stats["output_rows"], commit["silver_rows"], hdfs["silver_rows"], pipeline["silver_rows"])):
            raise RuntimeError(f"Số dòng Silver ngày {day} không tăng đúng 10 vụ/ngày.")
        if not commit["baseline_files_unchanged"] or commit["new_rows"] != 10 or commit["duplicates_removed"] != 1:
            raise RuntimeError("File nền thay đổi hoặc khử trùng Kafka chưa đúng.")
        if stats["input_rows"] != stats["rejected_rows"] + stats["duplicates_removed"] + stats["output_rows"]:
            raise RuntimeError("Số liệu đầu vào/lỗi/trùng/Silver không cân bằng.")
        if hdfs["live_datanodes"] != 3 or hdfs["partition_directories"] != 72 or hdfs["actual_replicas"] != 2 or hdfs["block_size"] != 134217728 or hdfs["codec"] != "SNAPPY":
            raise RuntimeError("Cấu hình HDFS/Parquet không đúng kế hoạch.")
        charts = list((folder / "analystics").glob("*/*.png"))
        if len(charts) != 15 or not (folder / "report/report-overview.png").is_file() or progress["bronze_records"] != 11:
            raise RuntimeError(f"Thiếu biểu đồ, báo cáo hoặc sự kiện Bronze ngày {day}.")
        if len(screenshots) != 4 or not all(item["success"] for item in screenshots) or not list((folder / "screenshots").glob("log-*.png")):
            raise RuntimeError("Thiếu ảnh HDFS, DAG hoặc terminal.")
        if stats["commit"] != os.environ["GITHUB_SHA"] or pipeline["run_id"] != os.environ["CHICAGO_RUN_ID"] + "-" + day:
            raise RuntimeError("Bằng chứng không đúng commit hoặc lần chạy.")
        if day == DATES[0] and not commit["retry_injected"]:
            raise RuntimeError("Chưa kiểm tra phục hồi sau lỗi chuyển file.")
        results.append({"day": day, "new_rows": 10, "silver_rows": expected, "charts": len(charts), "baseline_unchanged": True})
        print(f"ĐẠT ngày {day}: Silver {expected:,} dòng, tăng {10 * index} vụ mô phỏng, đủ 15 biểu đồ và báo cáo.", flush=True)
    airflow = json.loads((root / "manifests/kafka-airflow-validation.json").read_text())
    if airflow["successful_runs"] != 3:
        raise RuntimeError("Chưa đủ ba DAG run thành công.")
    result = {"commit": os.environ["GITHUB_SHA"], "run_id": os.environ["CHICAGO_RUN_ID"], "baseline_rows": baseline,
              "final_rows": baseline + 30, "simulation_rows": 30, "kafka_records": 33, "charts": 45, "days": results,
              "replayed_committed_batch_stable": True, "successful_dag_runs": 3}
    (root / "manifests/kafka-validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as stream:
        stream.write(f"## Kafka ingest đã thành công\n\nSilver nền: **{baseline:,}** → cuối: **{baseline + 30:,}**.\n\n")
        stream.write("| Ngày mô phỏng | Vụ mới | Tổng Silver | Biểu đồ |\n|---|---:|---:|---:|\n")
        for row in results:
            stream.write(f"| {row['day']} | 10 | {row['silver_rows']:,} | 15 |\n")
        stream.write("\nBa DAG run được consumer Kafka kích hoạt; gửi lại sự kiện và thông báo không tạo trùng. Tải artifact để lấy ảnh terminal, Parquet và ba bộ báo cáo.\n")
    print("ĐÃ NGHIỆM THU: 3 ngày, 30 vụ mới, 3 DAG run, 45 biểu đồ; Silver nền giữ nguyên.", flush=True)


if __name__ == "__main__":
    main()
