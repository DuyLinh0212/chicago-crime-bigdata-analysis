"""Phân tích chéo loại tội phạm, tháng, khu dân cư và thời gian."""
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from pipeline.community_areas import community_label
from pipeline.analytics_common import read_silver, counts, heatmap, save_figure, publish, entrypoint, type_name, COLORS


def analyze(spark, silver_path, output_dir, gold_path) -> dict:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    data = read_silver(spark, silver_path)
    try:
        total = data.count()
        types = counts(data, "primary_type").sort_values(["so_vu", "primary_type"], ascending=[False, True]).head(10).primary_type.tolist()
        areas = counts(data, "khu_dan_cu").sort_values(["so_vu", "khu_dan_cu"], ascending=[False, True])
        top_areas = areas.loc[areas.khu_dan_cu != "Không xác định"].head(15).khu_dan_cu.tolist()
        winners = {}
        for key, name, title, order in [
            ("month", "loai_theo_thang", "Loại tội phạm theo tháng trong năm", [f"{m:02d}" for m in range(1, 13)]),
            ("khu_dan_cu", "loai_theo_khu", "Loại tội phạm theo khu dân cư", top_areas),
            ("gio", "loai_theo_gio", "Loại tội phạm theo giờ Chicago", list(range(24))),
        ]:
            table = counts(data, key, "primary_type")
            if int(table.so_vu.sum()) != total:
                raise RuntimeError(f"Tổng bảng chéo {name} không khớp Silver.")
            table.to_csv(output / f"{name}.csv", index=False, encoding="utf-8-sig")
            maxima = table.groupby(key).so_vu.transform("max")
            leader = table.loc[table.so_vu == maxima].sort_values([key, "primary_type"])
            leader.to_csv(output / f"pho_bien_nhat_{name}.csv", index=False, encoding="utf-8-sig")
            winners[name] = len(leader)
            pivot = table.pivot(index="primary_type", columns=key, values="so_vu").fillna(0)
            pivot = pivot.reindex(index=types, columns=order, fill_value=0)
            heatmap(pivot, output, f"{name}.png", title, {"month": "Tháng", "gio": "Giờ", "khu_dan_cu": "Mã khu dân cư"}[key],
                    "Loại tội phạm (top 10)", xlabels=[community_label(code) for code in order] if key == "khu_dan_cu" else None,
                    ylabels=[type_name(x) for x in types])
        slots = counts(data, "khung_gio", "primary_type")
        slots.to_csv(output / "loai_theo_khung_gio.csv", index=False, encoding="utf-8-sig")
        slots.loc[slots.so_vu == slots.groupby("khung_gio").so_vu.transform("max")].to_csv(
            output / "pho_bien_nhat_theo_khung_gio.csv", index=False, encoding="utf-8-sig")
        trends = counts(data, "khu_dan_cu", "nam_thang")
        trends.to_csv(output / "xu_huong_khu_dan_cu.csv", index=False, encoding="utf-8-sig")
        months = pd.date_range("2018-01-01", "2023-12-01", freq="MS").strftime("%Y-%m")
        fig, axes = plt.subplots(3, 2, figsize=(14, 10), sharex=True, sharey=True)
        for index, ax in enumerate(axes.flat):
            if index >= len(top_areas[:6]):
                ax.set_visible(False)
                continue
            area = top_areas[index]
            table = trends.loc[trends.khu_dan_cu == area].set_index("nam_thang").reindex(months, fill_value=0)
            ax.plot(range(72), table.so_vu, color=COLORS[index], linewidth=1.7)
            ax.set_title(community_label(area))
            ax.set_ylabel("Số vụ")
            ax.grid(alpha=.2)
            ticks = list(range(0, 72, 12))
            ax.set_xticks(ticks, [months[i] for i in ticks], rotation=45, ha="right")
        fig.suptitle("So sánh xu hướng tháng của sáu khu dân cư có nhiều vụ nhất", fontsize=16)
        fig.subplots_adjust(top=.92)
        save_figure(fig, output, "so_sanh_xu_huong_khu.png")
        print("Đã xác định loại phổ biến theo từng tháng, khu dân cư và giờ; giữ tất cả trường hợp đồng hạng.", flush=True)
        return publish(spark, output, gold_path, {"so_vu": total, "top_types": types, "top_areas": top_areas[:6],
                                                "ten_khu_dan_cu": [community_label(code) for code in top_areas[:6]],
                                                "so_nhom_dan_dau": winners, "so_bieu_do": 4})
    finally:
        data.unpersist()


if __name__ == "__main__":
    entrypoint("phan_tich_cheo", analyze)
