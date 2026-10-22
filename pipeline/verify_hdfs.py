"""Đọc lại Silver và kiểm tra block thực tế; chỉ chạy trên GitHub."""
import json
from pathlib import Path

from normalize import BLOCK_SIZE, SILVER_SCHEMA, _filesystem, _check_cluster
from pipeline.spark_support import make_spark, metadata, write_json


if __name__ == "__main__":
    spark = make_spark("Chicago-KiemTra-HDFS")
    try:
        fs, root = _filesystem(spark, "/chicago-crimes/silver")
        _check_cluster(spark, fs)
        jpath = spark._jvm.org.apache.hadoop.fs.Path
        for year in range(2018, 2024):
            for month in range(1, 13):
                if not fs.isDirectory(jpath(root, f"year={year}/month={month:02d}")):
                    raise RuntimeError("Thiếu thư mục phân vùng Silver.")
        files = fs.listFiles(root, True)
        count = blocks = 0
        while files.hasNext():
            status = files.next()
            if not status.getPath().getName().endswith(".parquet"):
                continue
            count += 1
            if status.getReplication() != 2 or status.getBlockSize() != BLOCK_SIZE:
                raise RuntimeError("Replication hoặc block size không đúng.")
            for block in fs.getFileBlockLocations(status, 0, status.getLen()):
                blocks += 1
                if len(set(block.getHosts())) != 2:
                    raise RuntimeError("Block chưa có đúng hai bản sao trên hai DataNode.")
            footer = spark._jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
                spark.sparkContext._jsc.hadoopConfiguration(), status.getPath())
            for row_group in footer.getBlocks():
                for column in row_group.getColumns():
                    if str(column.getCodec()) != "SNAPPY":
                        raise RuntimeError("Parquet không dùng Snappy.")
        data = spark.read.schema(SILVER_SCHEMA).parquet(root.toString())
        rows = data.count()
        expected = json.loads(Path("artifacts/manifests/normalize.json").read_text())
        if not count or not blocks or rows != expected["output_rows"]:
            raise RuntimeError("Số dòng hoặc số file Silver không hợp lệ.")
        write_json("artifacts/manifests/silver-schema.json", SILVER_SCHEMA.jsonValue())
        write_json("artifacts/manifests/hdfs-verification.json", {
            **metadata(), "live_datanodes": 3, "partition_directories": 72,
            "parquet_files": count, "blocks": blocks, "actual_replicas": 2,
            "block_size": BLOCK_SIZE, "codec": "SNAPPY", "silver_rows": rows})
        print(f"Đã kiểm tra {rows:,} dòng, {count} file Parquet, {blocks} block có hai bản sao; đủ 72 phân vùng.", flush=True)
    finally:
        spark.stop()
