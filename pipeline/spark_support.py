"""Cấu hình Spark và metadata dùng chung cho các job."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from normalize import configure_spark


def make_spark(name):
    spark = (SparkSession.builder.appName(name)
             .config("spark.hadoop.fs.defaultFS", os.getenv("CHICAGO_HDFS_URI", "hdfs://namenode:9000"))
             .getOrCreate())
    configure_spark(spark)
    return spark


def metadata():
    return {"commit": os.getenv("CHICAGO_COMMIT", os.getenv("GITHUB_SHA", "local")),
            "run_id": os.getenv("CHICAGO_RUN_ID", "manual"),
            "created_at": datetime.now(timezone.utc).isoformat()}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
