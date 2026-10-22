"""Thống kê thời gian từ Parquet Silver, theo giờ America/Chicago."""
from pathlib import Path
import pandas as pd
from pipeline.analytics_common import read_silver, counts, maximum, figure, save_figure, publish, entrypoint, COLORS


def analyze(spark, silver_path, output_dir, gold_path) -> dict:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    data = read_silver(spark, silver_path)
    try:
        total = data.count()
        summary = {"so_vu": total}
        definitions = [
            ("year", range(2018, 2024), "theo_nam", "Số vụ theo năm", "Năm", False),
            ("month", [f"{m:02d}" for m in range(1, 13)], "thang_trong_nam", "Số vụ theo tháng trong năm (gộp 2018–2023)", "Tháng", False),
            ("nam_thang", pd.date_range("2018-01-01", "2023-12-01", freq="MS").strftime("%Y-%m"), "xu_huong_thang", "Xu hướng số vụ theo tháng", "Năm–tháng", True),
            ("ngay", pd.date_range("2018-01-01", "2023-12-31").date, "xu_huong_ngay", "Xu hướng số vụ theo ngày", "Ngày", True),
            ("gio", range(24), "theo_gio", "Số vụ theo giờ trong ngày", "Giờ Chicago", False),
        ]
        for key, index, name, title, xlabel, line in definitions:
            table = counts(data, key).set_index(key).reindex(index, fill_value=0).rename_axis(key).reset_index()
            table.so_vu = table.so_vu.astype(int)
            if int(table.so_vu.sum()) != total:
                raise RuntimeError(f"Tổng số vụ {name} không khớp Silver.")
            table.to_csv(output / f"{name}.csv", index=False, encoding="utf-8-sig")
            summary[f"cao_nhat_{name}"] = maximum(table, key)
            fig, ax = figure(title, xlabel)
            if key == "ngay":
                ax.plot(pd.to_datetime(table[key]), table.so_vu, color=COLORS[0], linewidth=.65)
            elif line:
                ax.plot(range(len(table)), table.so_vu, color=COLORS[0], linewidth=1.8)
                ticks = list(range(0, len(table), 6))
                ax.set_xticks(ticks, table[key].iloc[ticks], rotation=45, ha="right")
            else:
                ax.bar(table[key].astype(str), table.so_vu, color=COLORS[0])
            save_figure(fig, output, f"{name}.png")
        slots = counts(data, "khung_gio").sort_values("khung_gio")
        slots.to_csv(output / "khung_gio.csv", index=False, encoding="utf-8-sig")
        summary["khung_gio_cao_nhat"] = maximum(slots, "khung_gio")
        print(f"Thống kê thời gian: {total:,} vụ; giờ cao nhất: {summary['cao_nhat_theo_gio']}; tháng cao nhất: {summary['cao_nhat_thang_trong_nam']}.", flush=True)
        return publish(spark, output, gold_path, summary)
    finally:
        data.unpersist()


if __name__ == "__main__":
    entrypoint("thoi_gian", analyze)
