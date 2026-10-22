"""Chạy toàn bộ Normalize hai lần để kiểm tra tính ổn định trên HDFS thật."""
from normalize import normalize_data
from pipeline.spark_support import make_spark, metadata, write_json


if __name__ == "__main__":
    spark = make_spark("Chicago-ChuanHoa-ToanBo")
    try:
        first = normalize_data(spark)
        print("Đang chạy lại chuẩn hóa để kiểm tra không tăng số dòng...", flush=True)
        second = normalize_data(spark)
        if first != second:
            raise RuntimeError("Kết quả hai lần chuẩn hóa không khớp.")
        write_json("artifacts/manifests/normalize.json", {**metadata(), **second, "rerun_stable": True})
        print("Đã xác nhận hai lần chuẩn hóa cho kết quả giống nhau.", flush=True)
    finally:
        spark.stop()
