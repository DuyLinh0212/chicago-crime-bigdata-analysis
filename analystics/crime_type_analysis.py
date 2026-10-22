"""Top loại tội phạm, tỷ trọng và so sánh bốn nhóm."""
from pathlib import Path
import pandas as pd
from pipeline.analytics_common import read_silver, counts, maximum, figure, save_figure, publish, entrypoint, type_name, COLORS


def analyze(spark, silver_path, output_dir, gold_path) -> dict:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    data = read_silver(spark, silver_path)
    try:
        total = data.count()
        table = counts(data, "primary_type").sort_values(["so_vu", "primary_type"], ascending=[False, True])
        table["ty_trong_phan_tram"] = table.so_vu / total * 100
        table["ten_tieng_viet"] = table.primary_type.map(type_name)
        if int(table.so_vu.sum()) != total:
            raise RuntimeError("Tổng theo loại tội phạm không khớp Silver.")
        table.to_csv(output / "tat_ca_loai.csv", index=False, encoding="utf-8-sig")
        top = table.head(10).iloc[::-1]
        fig, ax = figure("10 loại tội phạm phổ biến nhất", "Số vụ", "Loại tội phạm", size=(12, 7))
        ax.barh(top.ten_tieng_viet, top.so_vu, color=COLORS[0])
        save_figure(fig, output, "top_loai.png")
        pie = table.head(7)[["ten_tieng_viet", "so_vu"]].copy()
        other = total - int(pie.so_vu.sum())
        if other:
            pie = pd.concat([pie, pd.DataFrame([{"ten_tieng_viet": "Các loại khác", "so_vu": other}])], ignore_index=True)
        pie.to_csv(output / "ty_trong.csv", index=False, encoding="utf-8-sig")
        fig, ax = figure("Tỷ trọng các loại tội phạm", ylabel="", size=(12, 7))
        wedges, _, _ = ax.pie(pie.so_vu, labels=None, autopct="%1.1f%%", startangle=90, colors=COLORS)
        ax.legend(wedges, pie.ten_tieng_viet, loc="center left", bbox_to_anchor=(1, .5))
        ax.grid(False)
        save_figure(fig, output, "ty_trong.png")
        lookup = dict(zip(table.primary_type, table.so_vu))
        compare = pd.DataFrame([
            {"nhom": "Trộm cắp", "ma_nguon": "THEFT", "so_vu": int(lookup.get("THEFT", 0))},
            {"nhom": "Cướp", "ma_nguon": "ROBBERY", "so_vu": int(lookup.get("ROBBERY", 0))},
            {"nhom": "Hành hung", "ma_nguon": "ASSAULT + BATTERY", "so_vu": int(lookup.get("ASSAULT", 0) + lookup.get("BATTERY", 0))},
            {"nhom": "Giết người", "ma_nguon": "HOMICIDE", "so_vu": int(lookup.get("HOMICIDE", 0))},
        ])
        compare.to_csv(output / "so_sanh_bon_nhom.csv", index=False, encoding="utf-8-sig")
        fig, ax = figure("So sánh trộm cắp, cướp, hành hung và giết người", "Nhóm tội phạm")
        ax.bar(compare.nhom, compare.so_vu, color=COLORS[:4])
        save_figure(fig, output, "so_sanh_bon_nhom.png")
        summary = {"so_vu": total, "pho_bien_nhat": maximum(table, "primary_type"), "so_loai": len(table),
                   "quy_uoc": "Trộm cắp = THEFT; hành hung = ASSAULT + BATTERY; trộm xe là loại riêng."}
        print(f"Đã thống kê {len(table)} loại tội phạm trên {total:,} bản ghi.", flush=True)
        return publish(spark, output, gold_path, summary)
    finally:
        data.unpersist()


if __name__ == "__main__":
    entrypoint("loai_toi_pham", analyze)
