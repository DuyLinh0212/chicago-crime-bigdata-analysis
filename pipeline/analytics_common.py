"""Đọc Silver, xuất số liệu/ảnh và công bố Gold cho bốn chương trình phân tích."""
import argparse
import hashlib
import json
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from normalize import SILVER_SCHEMA, _filesystem
from pipeline.spark_support import make_spark, metadata, write_json

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "figure.facecolor": "#f8fafc", "axes.facecolor": "#ffffff",
                     "axes.titleweight": "bold", "axes.titlesize": 15,
                     "axes.labelcolor": "#334155", "text.color": "#0f172a",
                     "savefig.dpi": 150, "axes.unicode_minus": False})
COLORS = ["#2563eb", "#e87924", "#10a37f", "#9333ea", "#dc2626", "#0891b2", "#be185d", "#64748b"]
TYPE_NAMES = {
    "THEFT": "Trộm cắp", "BATTERY": "Hành hung (tiếp xúc)", "ASSAULT": "Đe dọa hành hung",
    "ROBBERY": "Cướp tài sản", "HOMICIDE": "Giết người", "BURGLARY": "Đột nhập trộm cắp",
    "MOTOR VEHICLE THEFT": "Trộm xe cơ giới", "CRIMINAL DAMAGE": "Hủy hoại tài sản",
    "OTHER OFFENSE": "Vi phạm khác", "DECEPTIVE PRACTICE": "Gian lận",
    "NARCOTICS": "Ma túy", "CRIMINAL TRESPASS": "Xâm nhập trái phép",
    "WEAPONS VIOLATION": "Vi phạm vũ khí", "OFFENSE INVOLVING CHILDREN": "Vi phạm liên quan trẻ em",
    "CRIMINAL SEXUAL ASSAULT": "Tấn công tình dục", "CRIM SEXUAL ASSAULT": "Tấn công tình dục (mã cũ)",
    "SEX OFFENSE": "Vi phạm tình dục", "PUBLIC PEACE VIOLATION": "Gây rối trật tự",
    "INTERFERENCE WITH PUBLIC OFFICER": "Cản trở người thi hành công vụ", "PROSTITUTION": "Mại dâm",
    "ARSON": "Phóng hỏa", "STALKING": "Theo dõi, quấy rối", "INTIMIDATION": "Đe dọa",
    "KIDNAPPING": "Bắt cóc", "LIQUOR LAW VIOLATION": "Vi phạm quy định rượu",
    "GAMBLING": "Cờ bạc", "OBSCENITY": "Hành vi khiêu dâm", "PUBLIC INDECENCY": "Hành vi phản cảm công cộng",
    "CONCEALED CARRY LICENSE VIOLATION": "Vi phạm giấy phép mang súng kín",
    "NON-CRIMINAL": "Không thuộc hình sự", "NON - CRIMINAL": "Không thuộc hình sự (mã cũ)",
    "NON-CRIMINAL (SUBJECT SPECIFIED)": "Không thuộc hình sự (có đối tượng)",
    "HUMAN TRAFFICKING": "Buôn người", "OTHER NARCOTIC VIOLATION": "Vi phạm ma túy khác",
}
LOCATION_NAMES = {"STREET": "Đường phố", "RESIDENCE": "Nhà ở", "APARTMENT": "Căn hộ",
                  "SIDEWALK": "Vỉa hè", "OTHER": "Khác", "OTHER (SPECIFY)": "Khác (có mô tả)",
                  "PARKING LOT/GARAGE(NON.RESID.)": "Bãi đỗ xe/nhà xe", "ALLEY": "Hẻm",
                  "SMALL RETAIL STORE": "Cửa hàng bán lẻ nhỏ", "RESTAURANT": "Nhà hàng",
                  "GAS STATION": "Trạm xăng", "VEHICLE NON-COMMERCIAL": "Xe cá nhân",
                  "DEPARTMENT STORE": "Cửa hàng bách hóa", "GROCERY FOOD STORE": "Cửa hàng thực phẩm"}


def type_name(value):
    return TYPE_NAMES.get(value, f"Loại: {value}")


def read_silver(spark, path):
    data = spark.read.schema(SILVER_SCHEMA).parquet(path)
    return (data.withColumn("ngay", F.to_date("date_reported"))
            .withColumn("gio", F.hour("date_reported"))
            .withColumn("nam_thang", F.date_format("date_reported", "yyyy-MM"))
            .withColumn("khu_dan_cu", F.coalesce(F.col("community_area").cast("string"), F.lit("Không xác định")))
            .withColumn("dia_diem", F.coalesce(F.col("location_description"), F.lit("Không xác định")))
            .withColumn("khung_gio", F.when(F.col("gio") < 6, "00–05").when(F.col("gio") < 12, "06–11")
                        .when(F.col("gio") < 18, "12–17").otherwise("18–23")).cache())


def counts(data, *columns):
    return data.groupBy(*columns).count().withColumnRenamed("count", "so_vu").toPandas()


def maximum(frame, key):
    largest = int(frame.so_vu.max())
    return {"so_vu": largest, "dong_hang": [str(x) for x in frame.loc[frame.so_vu == largest, key].tolist()]}


def figure(title, xlabel="", ylabel="Số vụ", size=(11, 5)):
    fig, ax = plt.subplots(figsize=size)
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    ax.grid(axis="y", alpha=0.18)
    ax.set_axisbelow(True)
    return fig, ax


def save_figure(fig, folder, name):
    fig.text(0.01, 0.01, "Chicago Crimes 2018–2023 · Bản ghi sau khử trùng · Giờ Chicago", fontsize=8, color="#64748b")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(Path(folder) / name, bbox_inches="tight")
    plt.close(fig)
    print(f"Đã xuất biểu đồ: {name}", flush=True)


def heatmap(table, folder, name, title, xlabel, ylabel, xlabels=None, ylabels=None):
    fig, ax = plt.subplots(figsize=(max(11, len(table.columns) * .45), max(5, len(table.index) * .4)))
    image = ax.imshow(table.to_numpy(dtype=float), aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(table.columns)), xlabels if xlabels is not None else table.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(table.index)), ylabels if ylabels is not None else table.index)
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    fig.colorbar(image, ax=ax, label="Số vụ")
    save_figure(fig, folder, name)


def publish(spark, output, gold_path, summary):
    fs, target = _filesystem(spark, gold_path)
    jpath = spark._jvm.org.apache.hadoop.fs.Path
    fs.mkdirs(target)
    files = []
    for file in sorted(Path(output).glob("*")):
        if file.is_file() and file.name != "manifest.json":
            files.append({"name": file.name, "sha256": hashlib.sha256(file.read_bytes()).hexdigest(), "bytes": file.stat().st_size})
    manifest = {**metadata(), "silver_path": "/chicago-crimes/silver", "gold_path": gold_path,
                "timezone": "America/Chicago", "summary": summary, "files": files}
    write_json(Path(output) / "manifest.json", manifest)
    for file in sorted(Path(output).glob("*")):
        if file.is_file():
            fs.copyFromLocalFile(False, True, jpath(str(file.resolve())), jpath(target, file.name))
    print(f"Đã ghi {len(files)} file kết quả và manifest vào Gold: {target}", flush=True)
    return manifest


def entrypoint(name, analyze):
    parser = argparse.ArgumentParser(description=f"Phân tích {name} từ Parquet Silver.")
    parser.add_argument("--silver-path", default="/chicago-crimes/silver")
    parser.add_argument("--output-dir", default=f"artifacts/analystics/{name}")
    parser.add_argument("--gold-path", default=f"/chicago-crimes/gold/{os.getenv('CHICAGO_RUN_ID', 'manual')}/analystics/{name}")
    args = parser.parse_args()
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    spark = make_spark(f"Chicago-PhanTich-{name}")
    try:
        analyze(spark, args.silver_path, args.output_dir, args.gold_path)
    finally:
        spark.stop()
