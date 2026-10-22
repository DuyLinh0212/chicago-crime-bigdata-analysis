"""Chuẩn hóa CSV Bronze thành Parquet Silver bằng Spark 3.5."""

from __future__ import annotations

import csv
import os
import sys
from functools import reduce
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


BRONZE_PATH = "/chicago-crimes/bronze"
SILVER_PATH = "/chicago-crimes/silver"
FIRST_YEAR, LAST_YEAR = 2018, 2023
BLOCK_SIZE = 134217728
SOURCE_COLUMNS = [
    "date_reported", "city_block", "primary_type", "primary_description",
    "location_description", "arrest", "domestic", "community_area",
    "year", "latitude", "longitude", "location",
]
TEXT_COLUMNS = [
    "city_block", "primary_type", "primary_description", "location_description",
]
SILVER_SCHEMA = T.StructType([
    T.StructField("date_reported", T.TimestampType()),
    T.StructField("city_block", T.StringType()),
    T.StructField("primary_type", T.StringType()),
    T.StructField("primary_description", T.StringType()),
    T.StructField("location_description", T.StringType()),
    T.StructField("arrest", T.BooleanType()),
    T.StructField("domestic", T.BooleanType()),
    T.StructField("community_area", T.IntegerType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("location", T.StringType()),
    T.StructField("month", T.StringType()),
])


def configure_spark(spark: SparkSession) -> None:
    spark.sparkContext.setLogLevel("WARN")
    for key, value in {
        "spark.sql.session.timeZone": "America/Chicago",
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        "spark.sql.ansi.enabled": "false",
        "spark.sql.shuffle.partitions": "12",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
    }.items():
        spark.conf.set(key, value)
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop.set("dfs.replication", "2")
    hadoop.set("dfs.blocksize", str(BLOCK_SIZE))


def _filesystem(spark: SparkSession, path: str):
    java_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = java_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs, fs.makeQualified(java_path)


def _check_cluster(spark: SparkSession, fs) -> None:
    if fs.getUri().getScheme() != "hdfs":
        raise RuntimeError("Đầu vào phải nằm trên HDFS của cụm thực hành.")
    report_type = spark._jvm.org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType.LIVE
    live_nodes = len(fs.getDataNodeStats(report_type))
    if live_nodes < 3:
        raise RuntimeError(
            f"Chỉ có {live_nodes} DataNode hoạt động; cần đủ 3 DataNode trước khi chuẩn hóa."
        )
    print(f"Đã xác nhận {live_nodes} DataNode hoạt động; replication=2, block size=128 MiB.", flush=True)


def _list_csv(fs, root) -> list[str]:
    if not fs.exists(root) or not fs.isDirectory(root):
        raise RuntimeError(f"Không tìm thấy thư mục Bronze trên HDFS: {root}")
    files = []
    iterator = fs.listFiles(root, True)
    root_path = root.toUri().getPath().rstrip("/") + "/"
    while iterator.hasNext():
        item = iterator.next()
        path = item.getPath()
        relative = path.toUri().getPath()[len(root_path):]
        if any(part.startswith((".", "_")) for part in relative.split("/")):
            continue
        if path.getName().lower().endswith(".csv"):
            files.append(path.toString())
    if not files:
        raise RuntimeError(
            f"Bronze không có file CSV: {root}. Hãy bật HDFS và upload dữ liệu trước."
        )
    return sorted(files)


def _parse_partition(index, lines, positions):
    """CSV nguồn hiện tại có một bản ghi trên mỗi dòng; kiểm tra cả số trường."""
    if index == 0:
        next(lines, None)  # Bỏ đúng dòng header của từng file.
    for line in lines:
        if not line.strip():
            continue
        try:
            values = next(csv.reader([line], strict=True))
            if len(values) != len(SOURCE_COLUMNS):
                raise ValueError("Số trường không khớp header.")
            yield tuple(values[position] for position in positions) + (None,)
        except (csv.Error, ValueError):
            yield (None,) * len(SOURCE_COLUMNS) + ("csv_invalid",)


def _read_csv(spark: SparkSession, path: str) -> DataFrame:
    lines = spark.sparkContext.textFile(path)
    try:
        header = next(csv.reader([lines.first().lstrip("\ufeff")], strict=True))
    except (ValueError, csv.Error) as exc:
        raise RuntimeError(f"Không đọc được header CSV: {path}") from exc
    names = [name.strip().lower() for name in header]
    names = ["primary_description" if name == "primery_description" else name for name in names]
    if len(names) != len(SOURCE_COLUMNS) or set(names) != set(SOURCE_COLUMNS):
        raise RuntimeError(
            f"Header không đúng 12 cột Chicago Crimes: {path}. Các cột nhận được: {names}"
        )
    positions = tuple(names.index(name) for name in SOURCE_COLUMNS)
    records = lines.mapPartitionsWithIndex(
        lambda index, values: _parse_partition(index, values, positions)
    )
    schema = T.StructType([
        T.StructField(name, T.StringType()) for name in SOURCE_COLUMNS + ["_parse_error"]
    ])
    return spark.createDataFrame(records, schema)


def normalize_frame(raw: DataFrame) -> DataFrame:
    """Đổi kiểu và đánh dấu dòng lỗi; giữ dòng thiếu các trường tùy chọn."""
    result = raw
    for name in SOURCE_COLUMNS:
        value = F.trim(F.regexp_replace(F.col(name), r"\s+", " "))
        result = result.withColumn(name, F.when(F.length(value) > 0, value))
    for name in TEXT_COLUMNS:
        result = result.withColumn(name, F.upper(F.col(name)))

    timestamp = F.coalesce(
        *[
            F.try_to_timestamp("date_reported", F.lit(pattern))
            for pattern in (
                "M/d/yyyy H:mm", "M/d/yyyy H:mm:ss",
                "M/d/yyyy h:mm:ss a", "M/d/yyyy h:mm a",
            )
        ],
        F.try_to_timestamp("date_reported"),
    )
    result = result.withColumn("date_reported", timestamp)
    result = result.withColumn("year", F.year("date_reported"))
    result = result.withColumn("month", F.date_format("date_reported", "MM"))

    for name in ("arrest", "domestic"):
        value = F.upper(F.col(name))
        result = result.withColumn(
            name,
            F.when(value.isin("TRUE", "1"), F.lit(True))
            .when(value.isin("FALSE", "0"), F.lit(False)),
        )
    area = F.col("community_area")
    area_number = area.cast("double")
    result = result.withColumn(
        "community_area",
        F.when(
            area.rlike(r"^[0-9]+(?:\.0+)?$") & area_number.between(1, 77),
            area_number.cast("int"),
        ),
    )
    for name, minimum, maximum in (("latitude", -90, 90), ("longitude", -180, 180)):
        value = F.col(name).cast("double")
        result = result.withColumn(
            name, F.when(~F.isnan(value) & value.between(minimum, maximum), value)
        )
    coordinates_present = F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    result = result.withColumn(
        "location",
        F.when(
            coordinates_present,
            F.concat(
                F.lit("("), F.col("latitude").cast("string"), F.lit(", "),
                F.col("longitude").cast("string"), F.lit(")"),
            ),
        ),
    )
    well_formed = F.col("_parse_error").isNull()
    result = result.withColumn("_bad_csv", ~well_formed)
    result = result.withColumn("_bad_date", well_formed & F.col("date_reported").isNull())
    result = result.withColumn("_missing_type", well_formed & F.col("primary_type").isNull())
    result = result.withColumn(
        "_outside_years",
        well_formed & F.col("year").isNotNull() & ~F.col("year").between(FIRST_YEAR, LAST_YEAR),
    )
    return result.withColumn(
        "_rejected",
        F.col("_bad_csv") | F.col("_bad_date") | F.col("_missing_type") | F.col("_outside_years"),
    )


def _count_flag(name: str):
    return F.coalesce(F.sum(F.col(name).cast("long")), F.lit(0))


def _validate_staging(spark: SparkSession, fs, stage, expected_rows: int) -> None:
    physical = spark.read.option("recursiveFileLookup", "true").option(
        "mergeSchema", "true"
    ).parquet(stage.toString())
    physical_types = {field.name: field.dataType for field in physical.schema}
    expected_types = {
        field.name: field.dataType for field in SILVER_SCHEMA
        if field.name not in ("year", "month")
    }
    if physical_types != expected_types:
        raise RuntimeError("Kiểu dữ liệu vật lý trong Parquet không đúng schema Silver.")
    data = spark.read.schema(SILVER_SCHEMA).parquet(stage.toString())
    if data.count() != expected_rows:
        raise RuntimeError("Số dòng đọc lại từ Parquet không khớp; giữ nguyên Silver cũ.")
    wrong_partition = (
        F.col("date_reported").isNull() | F.col("primary_type").isNull()
        | (F.year("date_reported") != F.col("year"))
        | (F.date_format("date_reported", "MM") != F.col("month"))
    )
    if data.filter(wrong_partition).limit(1).count():
        raise RuntimeError("Phân vùng Parquet không khớp ngày báo cáo.")
    iterator = fs.listFiles(stage, True)
    parquet_count = 0
    while iterator.hasNext():
        status = iterator.next()
        if status.getPath().getName().endswith(".parquet"):
            parquet_count += 1
            if fs.getUri().getScheme() == "hdfs":
                if status.getReplication() != 2 or status.getBlockSize() != BLOCK_SIZE:
                    raise RuntimeError("Parquet chưa có replication=2 hoặc block size=128 MiB.")
    if not parquet_count:
        raise RuntimeError("Không tạo được file Parquet nào; giữ nguyên Silver cũ.")


def _publish_silver(fs, stage, silver, backup) -> None:
    """Chỉ thay Silver khi staging hợp lệ; khôi phục bản cũ nếu đổi tên lỗi."""
    had_silver = fs.exists(silver)
    if had_silver and not fs.rename(silver, backup):
        raise RuntimeError("Không chuyển được Silver cũ vào backup; chưa thay dữ liệu.")
    try:
        if not fs.rename(stage, silver):
            raise RuntimeError("Không chuyển được Parquet mới vào Silver.")
    except Exception as exc:
        if had_silver:
            try:
                if fs.exists(silver) or not fs.rename(backup, silver):
                    raise RuntimeError("Đích Silver tồn tại hoặc không đổi tên được backup.")
            except Exception as restore_error:
                raise RuntimeError(
                    f"Không khôi phục được Silver. Bản cũ còn tại {backup}; "
                    f"cần phục hồi trước lần chạy tiếp theo. Chi tiết: {restore_error}"
                ) from exc
        raise RuntimeError("Thay Silver thất bại; đã giữ/khôi phục dữ liệu cũ.") from exc
    if had_silver:
        try:
            if not fs.delete(backup, True):
                print(f"Silver mới đã sẵn sàng; chưa xóa được backup tại {backup}.", flush=True)
        except Exception:
            print(f"Silver mới đã sẵn sàng; backup vẫn còn tại {backup}.", flush=True)


def normalize_data(
    spark: SparkSession,
    bronze_path: str = BRONZE_PATH,
    silver_path: str = SILVER_PATH,
    *,
    require_three_datanodes: bool = True,
) -> dict:
    """Dựng lại toàn bộ Silver; đường dẫn tùy chọn phục vụ kiểm tra bằng dữ liệu mẫu."""
    configure_spark(spark)
    fs, bronze = _filesystem(spark, bronze_path)
    silver_fs, silver = _filesystem(spark, silver_path)
    if fs.getUri().toString() != silver_fs.getUri().toString():
        raise RuntimeError("Bronze và Silver phải thuộc cùng filesystem.")
    source = bronze.toUri().getPath().rstrip("/")
    target = silver.toUri().getPath().rstrip("/")
    if not target or source == target or source.startswith(target + "/") or target.startswith(source + "/"):
        raise RuntimeError("Bronze và Silver phải là hai thư mục riêng, không lồng nhau.")
    if require_three_datanodes:
        _check_cluster(spark, fs)
    files = _list_csv(fs, bronze)
    print(f"Tìm thấy {len(files)} file CSV trong Bronze.", flush=True)
    raw = reduce(DataFrame.unionByName, [_read_csv(spark, path) for path in files])
    normalized = normalize_frame(raw).persist(StorageLevel.MEMORY_AND_DISK)
    unique = None
    Path = spark._jvm.org.apache.hadoop.fs.Path
    run_root = Path(silver.getParent(), f"_staging/normalize-{uuid4().hex}")
    stage = Path(run_root, "silver")
    backup = Path(run_root, "silver-backup")
    try:
        values = normalized.agg(
            F.count("*").alias("input_rows"),
            *[_count_flag(name).alias(name) for name in (
                "_bad_csv", "_bad_date", "_missing_type", "_outside_years", "_rejected"
            )],
        ).first().asDict()
        valid_rows = values["input_rows"] - values["_rejected"]
        unique = (
            normalized.filter(~F.col("_rejected"))
            .select(*SOURCE_COLUMNS, "month")
            .dropDuplicates(SOURCE_COLUMNS)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        output_rows = unique.count()
        duplicates = valid_rows - output_rows
        print(
            f"Đầu vào: {values['input_rows']:,} dòng; loại lỗi: {values['_rejected']:,}; "
            f"xóa trùng: {duplicates:,}; còn lại: {output_rows:,}.", flush=True,
        )
        print(
            f"Lý do loại (có thể trùng nhau): CSV hỏng={values['_bad_csv']:,}, "
            f"sai ngày={values['_bad_date']:,}, thiếu loại tội phạm={values['_missing_type']:,}, "
            f"ngoài năm 2018–2023={values['_outside_years']:,}.", flush=True,
        )
        if output_rows == 0:
            raise RuntimeError("Không có dòng hợp lệ để ghi Parquet; giữ nguyên Silver hiện có.")
        if values["input_rows"] != values["_rejected"] + duplicates + output_rows:
            raise RuntimeError("Thống kê số dòng không cân bằng; chưa cập nhật Silver.")
        print(f"Đang ghi Parquet Snappy vào staging: {stage}", flush=True)
        unique.repartition(12, "year", "month").write.mode("errorifexists").option(
            "compression", "snappy"
        ).partitionBy("year", "month").parquet(stage.toString())
        for year in range(FIRST_YEAR, LAST_YEAR + 1):
            for month in range(1, 13):
                if not fs.mkdirs(Path(stage, f"year={year}/month={month:02d}")):
                    raise RuntimeError("Không tạo được đủ 72 thư mục năm/tháng.")
        _validate_staging(spark, fs, stage, output_rows)
        _publish_silver(fs, stage, silver, backup)
        print(f"Đã chuẩn hóa thành công {output_rows:,} dòng Parquet tại {silver}.", flush=True)
        return {
            "input_files": len(files), "input_rows": values["input_rows"],
            "rejected_rows": values["_rejected"], "duplicates_removed": duplicates,
            "output_rows": output_rows,
            "errors": {
                "csv_invalid": values["_bad_csv"], "date_invalid": values["_bad_date"],
                "primary_type_missing": values["_missing_type"],
                "year_out_of_range": values["_outside_years"],
            },
        }
    finally:
        normalized.unpersist()
        if unique is not None:
            unique.unpersist()
        # Không dọn run_root nếu còn backup cần phục hồi.
        try:
            if fs.exists(run_root) and not fs.exists(backup):
                if not fs.delete(run_root, True):
                    print(f"Chưa dọn được thư mục tạm: {run_root}", flush=True)
        except Exception:
            print(f"Cần kiểm tra thư mục tạm: {run_root}", flush=True)


def main() -> int:
    spark = None
    try:
        builder = (
            SparkSession.builder.appName("ChicagoCrimesBronzeToSilver")
            .config("spark.hadoop.dfs.replication", "2")
            .config("spark.hadoop.dfs.blocksize", str(BLOCK_SIZE))
        )
        if os.environ.get("CHICAGO_HDFS_URI"):
            builder = builder.config("spark.hadoop.fs.defaultFS", os.environ["CHICAGO_HDFS_URI"])
        spark = builder.getOrCreate()
        normalize_data(spark)
        return 0
    except Exception as exc:
        print(f"Chuẩn hóa thất bại: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
