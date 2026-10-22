"""Kiểm tra khử trùng null-safe trên Spark thật trong CI."""
from functools import reduce
from pyspark.sql import functions as F, types as T
from ingest.model import DATES, events_for_day
from normalize import SOURCE_COLUMNS, normalize_frame
from pipeline.spark_support import make_spark

if __name__ == "__main__":
    spark = make_spark("Chicago-KiemTra-Kafka-Null")
    try:
        events = events_for_day(DATES[0])
        schema = T.StructType([T.StructField(name, T.StringType()) for name in SOURCE_COLUMNS + ["_parse_error"]])
        raw = spark.createDataFrame([tuple(e["payload"].get(name) for name in SOURCE_COLUMNS) + (None,) for e in events + events[:1]], schema)
        normalized = normalize_frame(raw)
        if normalized.where("_rejected").count():
            raise RuntimeError("Dữ liệu mô phỏng bị Normalize loại.")
        unique = normalized.select(*SOURCE_COLUMNS, "month").dropDuplicates(SOURCE_COLUMNS)
        if unique.count() != 10:
            raise RuntimeError("Không loại đúng bản gửi lại trong lô mô phỏng.")
        null_row = unique.where(F.col("latitude").isNull()).limit(1)
        condition = reduce(lambda a, b: a & b, [F.col(f"new.{name}").eqNullSafe(F.col(f"old.{name}")) for name in SOURCE_COLUMNS])
        if null_row.alias("new").join(null_row.alias("old"), condition, "left_anti").count() != 0:
            raise RuntimeError("Chống trùng không xử lý đúng trường null.")
        print("ĐẠT: Normalize xử lý 10 vụ giả, một bản gửi lại và so sánh null-safe với Silver.", flush=True)
    finally:
        spark.stop()
