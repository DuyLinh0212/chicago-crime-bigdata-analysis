"""Spark AvailableNow vào Bronze, sau đó bổ sung Parquet bằng lô có thể phục hồi."""
import argparse
import json
import os
from functools import reduce
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F, types as T
from normalize import (SOURCE_COLUMNS, SILVER_SCHEMA, normalize_frame, _filesystem,
                       _check_cluster, _validate_staging)
from pipeline.spark_support import make_spark, metadata, write_json
from ingest.model import DATES, EVENT_TOPIC, events_for_day, validate_batch_id, validate_ready

SILVER = "/chicago-crimes/silver"
BRONZE = "/chicago-crimes/bronze/kafka"
CHECKPOINT = "/chicago-crimes/_checkpoints/kafka-bronze"
LEDGER = "/chicago-crimes/_ingest"
RAW_SCHEMA = T.StructType([
    T.StructField("event_json", T.StringType()), T.StructField("topic", T.StringType()),
    T.StructField("partition", T.IntegerType()), T.StructField("offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
])


def paths(spark):
    fs, _ = _filesystem(spark, SILVER)
    return fs, spark._jvm.org.apache.hadoop.fs.Path


def read_hdfs_json(spark, name):
    fs, P = paths(spark)
    stream = fs.open(P(name))
    try:
        return json.loads(spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()


def create_hdfs_json(spark, name, value):
    """Manifest bất biến: ghi file tạm, rồi rename cùng HDFS."""
    fs, P = paths(spark)
    target = P(name)
    if fs.exists(target):
        if read_hdfs_json(spark, name) != value:
            raise RuntimeError(f"Manifest đã tồn tại nhưng nội dung khác: {name}")
        return
    fs.mkdirs(target.getParent())
    temporary = P(name + ".tmp-" + uuid4().hex)
    stream = fs.create(temporary, False)
    try:
        stream.write(bytearray(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")))
    finally:
        stream.close()
    if not fs.rename(temporary, target):
        raise RuntimeError(f"Không công bố được manifest: {name}")


def parquet_inventory(spark, root=SILVER):
    fs, P = paths(spark)
    files = fs.listFiles(P(root), True)
    result = {}
    while files.hasNext():
        status = files.next()
        if status.getPath().getName().endswith(".parquet"):
            name = status.getPath().toUri().getPath()
            result[name] = {"bytes": int(status.getLen()), "hdfs_checksum": str(fs.getFileChecksum(status.getPath()))}
    return result


def baseline(spark):
    inventory = parquet_inventory(spark)
    normalized = json.loads(Path("artifacts/manifests/normalize.json").read_text())
    if not inventory or normalized["output_rows"] <= 0:
        raise RuntimeError("Chưa có Silver nền để mô phỏng Kafka.")
    write_json("artifacts/manifests/baseline-silver.json", {**metadata(), "files": inventory,
               "normalized": normalized, "source": json.loads(Path("artifacts/manifests/source.json").read_text())})
    print(f"Đã lưu checksum {len(inventory)} file Silver nền, {normalized['output_rows']:,} bản ghi.", flush=True)


def batch_context():
    batch_id = validate_batch_id(os.environ["CHICAGO_BATCH_ID"])
    ready = validate_ready(json.loads(Path(f"artifacts/kafka/batches/{batch_id}.json").read_text()))
    return ready


def bounded_records(spark, ready):
    conditions = [(F.col("partition") == int(p)) & (F.col("offset") >= begin)
                  & (F.col("offset") < ready["end_offsets"][p]) for p, begin in ready["start_offsets"].items()]
    records = (spark.read.schema(RAW_SCHEMA).json(BRONZE).where(F.col("topic") == EVENT_TOPIC)
               .where(reduce(lambda a, b: a | b, conditions)).collect())
    actual_offsets = {(row.partition, row.offset) for row in records}
    expected_offsets = {(int(p), value) for p, begin in ready["start_offsets"].items()
                        for value in range(begin, ready["end_offsets"][p])}
    if len(records) != ready["record_count"] or actual_offsets != expected_offsets:
        raise RuntimeError("Bronze chưa chứa đúng và đủ khoảng offset đã được Kafka xác nhận.")
    expected = {event["event_id"]: event for event in events_for_day(ready["event_date"])}
    events = [json.loads(row.event_json) for row in records]
    if any(event != expected.get(event.get("event_id")) for event in events) or {e["event_id"] for e in events} != set(expected):
        raise RuntimeError("Nội dung sự kiện trên Bronze khác với dữ liệu API đã xác định.")
    return records, events


def ingest(spark, ready, output):
    fs, P = paths(spark)
    if not fs.exists(P(CHECKPOINT)) and fs.exists(P(BRONZE)):
        raise RuntimeError("Bronze Kafka đã tồn tại nhưng mất checkpoint; cần phục hồi trước khi chạy tiếp.")
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", EVENT_TOPIC)
              .option("startingOffsets", "earliest").option("failOnDataLoss", "true").load())
    raw = stream.select(F.col("value").cast("string").alias("event_json"), "topic", "partition", "offset",
                        F.col("timestamp").alias("kafka_timestamp"), F.current_date().alias("ingest_date"))
    query = (raw.writeStream.format("json").outputMode("append").partitionBy("ingest_date")
             .option("checkpointLocation", CHECKPOINT).trigger(availableNow=True).start(BRONZE))
    if not query.awaitTermination(180):
        query.stop()
        raise RuntimeError("Spark AvailableNow vượt thời gian chờ.")
    records, events = bounded_records(spark, ready)
    write_json(output / "manifests/kafka-progress.json", {**metadata(), "batch": ready,
               "checkpoint": CHECKPOINT, "progress": list(query.recentProgress), "bronze_records": len(records)})
    raw_artifact = output / "bronze-events.jsonl"
    raw_artifact.write_text("".join(json.dumps({**r.asDict(), "kafka_timestamp": str(r.kafka_timestamp)}, ensure_ascii=False) + "\n" for r in records), encoding="utf-8")
    print(f"Spark AvailableNow: đã xác nhận {len(records)} thông điệp, {len(set(e['event_id'] for e in events))} vụ duy nhất ngày {ready['event_date']} trong Bronze JSONL.", flush=True)


def build_plan(spark, ready):
    fs, P = paths(spark)
    batch_id = ready["batch_id"]
    plan_path = f"{LEDGER}/{batch_id}/plan.json"
    if fs.exists(P(plan_path)):
        plan = read_hdfs_json(spark, plan_path)
        if plan["batch"] != ready:
            raise RuntimeError("Mã lô đã được sử dụng với khoảng offset khác.")
        return plan
    _, events = bounded_records(spark, ready)
    schema = T.StructType([T.StructField(name, T.StringType()) for name in SOURCE_COLUMNS + ["_parse_error"]])
    raw = spark.createDataFrame([tuple(event["payload"].get(name) for name in SOURCE_COLUMNS) + (None,) for event in events], schema)
    normalized = normalize_frame(raw).cache()
    additions = None
    try:
        rejected = normalized.where("_rejected").count()
        if rejected:
            raise RuntimeError(f"Có {rejected} sự kiện mô phỏng không qua quy tắc Normalize.")
        unique = normalized.select(*SOURCE_COLUMNS, "month").dropDuplicates(SOURCE_COLUMNS)
        existing = spark.read.schema(SILVER_SCHEMA).parquet(SILVER).where((F.col("year") == 2023) & (F.col("month") == "12"))
        condition = reduce(lambda a, b: a & b, [F.col(f"new.{name}").eqNullSafe(F.col(f"old.{name}")) for name in SOURCE_COLUMNS])
        additions = unique.alias("new").join(existing.alias("old"), condition, "left_anti").cache()
        count = additions.count()
        if count != ready["expected_unique_count"]:
            raise RuntimeError(f"Lô mới cần bổ sung 10 vụ, thực tế có {count}; chưa thay đổi Silver.")
        stage = f"/chicago-crimes/_staging/kafka-{batch_id}/silver"
        if fs.exists(P(stage)):
            # Chưa có plan nên chưa có file nào được công bố vào Silver.
            fs.delete(P(stage), True)
        additions.coalesce(1).write.mode("errorifexists").option("compression", "snappy").partitionBy("year", "month").parquet(stage)
        _validate_staging(spark, fs, P(stage), count)
        staged = parquet_inventory(spark, stage)
        moves = []
        for index, (source, fingerprint) in enumerate(sorted(staged.items())):
            relative = source[len(stage) + 1:]
            partition = relative.rsplit("/", 1)[0]
            if partition != "year=2023/month=12":
                raise RuntimeError("Lô mô phỏng ghi sai phân vùng năm/tháng.")
            moves.append({"source": source, "target": f"{SILVER}/{partition}/part-{batch_id}-{index:04d}.parquet", **fingerprint})
        plan = {"batch": ready, "new_rows": count, "input_rows": len(events), "duplicates_removed": len(events) - count, "moves": moves}
        create_hdfs_json(spark, plan_path, plan)
        return plan
    finally:
        normalized.unpersist()
        if additions is not None:
            additions.unpersist()


def append(spark, ready, output):
    fs, P = paths(spark)
    plan = build_plan(spark, ready)
    root = f"{LEDGER}/{ready['batch_id']}"
    for move in plan["moves"]:
        destination = P(move["target"])
        if fs.exists(destination):
            status = fs.getFileStatus(destination)
            if int(status.getLen()) != move["bytes"] or str(fs.getFileChecksum(destination)) != move["hdfs_checksum"]:
                raise RuntimeError("File đích đã tồn tại nhưng checksum khác; dừng để bảo vệ Silver.")
            print(f"Đã có file đúng checksum, không ghi trùng: {destination}", flush=True)
        elif not fs.rename(P(move["source"]), destination):
            raise RuntimeError(f"Không chuyển được file Parquet vào Silver: {destination}")
        if os.getenv("CHICAGO_FAIL_AFTER_PROMOTE") == "1" and not fs.exists(P(root + "/retry-injected.json")):
            create_hdfs_json(spark, root + "/retry-injected.json", {"injected": True})
            raise RuntimeError("Lỗi có chủ đích sau khi chuyển file Parquet: Airflow phải thử lại và không tạo dòng trùng.")
    create_hdfs_json(spark, root + "/committed.json", plan)
    base = json.loads(Path("artifacts/manifests/baseline-silver.json").read_text())
    current = parquet_inventory(spark)
    if any(current.get(name) != fingerprint for name, fingerprint in base["files"].items()):
        raise RuntimeError("Checksum file Silver nền đã thay đổi.")
    commits = [read_hdfs_json(spark, f"{LEDGER}/demo-{day}/committed.json") for day in DATES
               if fs.exists(P(f"{LEDGER}/demo-{day}/committed.json"))]
    baseline_stats = base["normalized"]
    added = sum(item["new_rows"] for item in commits)
    normalized = {**baseline_stats, **metadata(), "simulation": True, "simulation_rows": added,
        "input_rows": baseline_stats["input_rows"] + sum(item["input_rows"] for item in commits),
        "duplicates_removed": baseline_stats["duplicates_removed"] + sum(item["duplicates_removed"] for item in commits),
        "output_rows": baseline_stats["output_rows"] + added}
    if normalized["input_rows"] != normalized["rejected_rows"] + normalized["duplicates_removed"] + normalized["output_rows"]:
        raise RuntimeError("Thống kê dữ liệu nền và Kafka không cân bằng.")
    write_json(output / "manifests/normalize.json", normalized)
    write_json(output / "manifests/source.json", {**base["source"], **metadata(), "simulation": True, "batch": ready})
    write_json(output / "manifests/kafka-commit.json", {**metadata(), **plan, "baseline_files_unchanged": True,
        "baseline_file_count": len(base["files"]), "simulation_rows": added, "silver_rows": normalized["output_rows"],
        "retry_injected": fs.exists(P(root + "/retry-injected.json"))})
    print(f"Đã bổ sung an toàn {plan['new_rows']} vụ ngày {ready['event_date']}; tổng Silver {normalized['output_rows']:,}; {len(base['files'])} file nền giữ nguyên checksum.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["baseline", "ingest", "append"])
    action = parser.parse_args().action
    spark = make_spark("Chicago-Kafka-" + action)
    try:
        fs, _ = paths(spark)
        _check_cluster(spark, fs)
        if action == "baseline":
            baseline(spark)
        else:
            ready = batch_context()
            output = Path(os.environ["CHICAGO_ARTIFACT_DIR"])
            output.mkdir(parents=True, exist_ok=True)
            if action == "ingest":
                ingest(spark, ready, output)
            else:
                append(spark, ready, output)
    finally:
        spark.stop()
