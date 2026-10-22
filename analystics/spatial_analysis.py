"""Xếp hạng khu dân cư, địa điểm; nguồn không có cột quận cảnh sát."""
from pathlib import Path
from pyspark.sql import functions as F
from pipeline.analytics_common import read_silver, counts, maximum, figure, save_figure, heatmap, publish, entrypoint, LOCATION_NAMES, COLORS


def analyze(spark, silver_path, output_dir, gold_path) -> dict:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    data = read_silver(spark, silver_path)
    try:
        total = data.count()
        areas = counts(data, "khu_dan_cu").sort_values(["so_vu", "khu_dan_cu"], ascending=[False, True])
        locations = counts(data, "dia_diem").sort_values(["so_vu", "dia_diem"], ascending=[False, True])
        for table, name in [(areas, "xep_hang_khu_dan_cu"), (locations, "xep_hang_dia_diem")]:
            if int(table.so_vu.sum()) != total:
                raise RuntimeError(f"Tổng bảng {name} không khớp Silver.")
            table["ty_trong_phan_tram"] = table.so_vu / total * 100
            table.to_csv(output / f"{name}.csv", index=False, encoding="utf-8-sig")
        top = areas.head(15).iloc[::-1]
        fig, ax = figure("15 khu dân cư có nhiều vụ nhất", "Số vụ", "Mã community_area", size=(12, 8))
        ax.barh(top.khu_dan_cu, top.so_vu, color=COLORS[0])
        save_figure(fig, output, "xep_hang_khu_dan_cu.png")
        top = locations.head(15).iloc[::-1]
        labels = [LOCATION_NAMES.get(x, f"Địa điểm: {x}") for x in top.dia_diem]
        fig, ax = figure("15 loại địa điểm có nhiều vụ nhất", "Số vụ", "Loại địa điểm", size=(13, 8))
        ax.barh(labels, top.so_vu, color=COLORS[2])
        save_figure(fig, output, "xep_hang_dia_diem.png")
        matrix = counts(data, "khu_dan_cu", "month")
        matrix.to_csv(output / "khu_dan_cu_theo_thang.csv", index=False, encoding="utf-8-sig")
        pivot = matrix.pivot(index="khu_dan_cu", columns="month", values="so_vu").fillna(0)
        pivot = pivot.reindex(index=areas.head(15).khu_dan_cu, columns=[f"{m:02d}" for m in range(1, 13)], fill_value=0)
        heatmap(pivot, output, "nhiet_khu_dan_cu_thang.png", "Số vụ theo khu dân cư và tháng (gộp các năm)", "Tháng", "Mã khu dân cư")
        missing_area = data.filter(F.col("community_area").isNull()).count()
        missing_location = data.filter(F.col("location_description").isNull()).count()
        summary = {"so_vu": total, "khu_dan_cu_cao_nhat": maximum(areas, "khu_dan_cu"),
                   "thieu_khu_dan_cu": missing_area, "ty_le_thieu_khu_phan_tram": missing_area / total * 100,
                   "thieu_dia_diem": missing_location, "ty_le_thieu_dia_diem_phan_tram": missing_location / total * 100,
                   "district_available": False,
                   "ghi_chu": "Sáu CSV không có district: chưa có thống kê theo quận cảnh sát. community_area là khu dân cư, không phải quận cảnh sát."}
        print(f"Đã phân tích {len(areas)} nhóm khu dân cư; thiếu khu: {missing_area:,}; thiếu địa điểm: {missing_location:,}.", flush=True)
        print(summary["ghi_chu"], flush=True)
        return publish(spark, output, gold_path, summary)
    finally:
        data.unpersist()


if __name__ == "__main__":
    entrypoint("khong_gian", analyze)
