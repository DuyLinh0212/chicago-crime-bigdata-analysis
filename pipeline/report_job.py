"""Tổng hợp manifest phân tích thành báo cáo HTML và công bố Gold."""
import hashlib
import html
import json
import os
from pathlib import Path
from PIL import Image
from matplotlib import font_manager
from pipeline.spark_support import make_spark, metadata, write_json
from pipeline.analytics_common import publish


def build_report():
    root = Path(os.getenv("CHICAGO_ARTIFACT_DIR", "artifacts"))
    output = root / "report"
    output.mkdir(parents=True, exist_ok=True)
    expected = json.loads((root / "manifests/normalize.json").read_text())["output_rows"]
    sections, manifests = [], []
    titles = {"thoi_gian": "1. Phân tích thời gian", "loai_toi_pham": "2. Loại tội phạm",
              "khong_gian": "3. Khu dân cư và địa điểm", "phan_tich_cheo": "4. Phân tích đa chiều"}
    image_count = 0
    for name, title in titles.items():
        folder = root / "analystics" / name
        manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
        if manifest["summary"]["so_vu"] != expected or manifest["commit"] != metadata()["commit"] or manifest["run_id"] != metadata()["run_id"]:
            raise RuntimeError(f"Manifest {name} không khớp dữ liệu/lần chạy.")
        images = []
        for item in manifest["files"]:
            path = folder / item["name"]
            if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
                raise RuntimeError(f"Checksum đầu ra không khớp: {path}")
            if path.suffix == ".png":
                with Image.open(path) as image:
                    image.verify()
                with Image.open(path) as image:
                    if image.width < 600 or image.height < 300 or len(image.convert("RGB").resize((100, 100)).getcolors(10001)) < 10:
                        raise RuntimeError(f"Biểu đồ trống hoặc quá nhỏ: {path}")
                image_count += 1
                images.append(f'<figure><img src="../analystics/{name}/{path.name}" alt="Biểu đồ {html.escape(path.stem)}"><figcaption>{html.escape(path.stem.replace("_", " "))}</figcaption></figure>')
        summary = html.escape(json.dumps(manifest["summary"], ensure_ascii=False, indent=2))
        sections.append(f'<section><h2>{title}</h2><div class="charts">{"".join(images)}</div><details><summary>Số liệu và kết luận chi tiết</summary><pre>{summary}</pre></details></section>')
        manifests.append(manifest)
    if image_count < 15:
        raise RuntimeError(f"Chỉ có {image_count} biểu đồ; cần ít nhất 15.")
    font = font_manager.get_font(font_manager.findfont("DejaVu Sans"))
    sample = "Thống kê số vụ theo năm tháng ngày giờ Khu dân cư Địa điểm Không xác định"
    if any(ord(char) not in font.get_charmap() for char in sample if not char.isspace()):
        raise RuntimeError("Font biểu đồ thiếu ký tự tiếng Việt.")
    source = json.loads((root / "manifests/source.json").read_text())
    normalized = json.loads((root / "manifests/normalize.json").read_text())
    temporal = manifests[0]["summary"]
    simulation_note = ""
    if normalized.get("simulation"):
        simulation_note = (f'<p class="note"><strong>DỮ LIỆU MÔ PHỎNG CI</strong>: '
                           f'đã bổ sung {normalized["simulation_rows"]} vụ giả đến ngày '
                           f'{html.escape(os.environ["CHICAGO_SIMULATION_DATE"])}. '
                           'Báo cáo kết hợp dữ liệu nền thật và các vụ giả phục vụ thực hành Kafka.</p>')
    temporal_text = html.escape(f"Tháng cao nhất (gộp các năm): {temporal['cao_nhat_thang_trong_nam']['dong_hang']}; giờ cao nhất: {temporal['cao_nhat_theo_gio']['dong_hang']}; khung giờ: {temporal['khung_gio_cao_nhat']['dong_hang']}.")
    document = f'''<!doctype html><html lang="vi"><head><meta charset="utf-8"><title>Báo cáo Chicago Crimes</title>
<style>body{{margin:0;background:#f1f5f9;color:#0f172a;font:16px 'DejaVu Sans',sans-serif}}main{{max-width:1400px;margin:auto;padding:32px}}header,section{{background:white;border-radius:12px;padding:24px;margin-bottom:24px}}h1{{font-size:36px;margin:0 0 16px}}h2{{color:#1d4ed8}}.kpi{{display:flex;gap:24px;flex-wrap:wrap}}.card{{background:#eff6ff;padding:20px;border-radius:8px;flex:1}}.card strong{{display:block;font-size:30px}}.charts{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}figure{{margin:0;border:1px solid #e2e8f0;border-radius:8px;padding:8px}}img{{width:100%;height:auto}}figcaption{{color:#64748b;font-size:12px}}pre{{white-space:pre-wrap}}.note{{border-left:4px solid #e87924;padding:12px;background:#fff7ed}}footer{{color:#64748b}}</style></head><body><main>
<header><h1>Báo cáo phân tích Chicago Crimes</h1><p>Toàn bộ dữ liệu 2018–2023 · Múi giờ America/Chicago · {image_count} biểu đồ</p>{simulation_note}
<div class="kpi"><div class="card"><strong>{expected:,}</strong>Bản ghi Silver</div><div class="card"><strong>{normalized['input_rows']:,}</strong>Dòng đầu vào</div><div class="card"><strong>{normalized['duplicates_removed']:,}</strong>Dòng trùng đã loại</div></div><p>{temporal_text}</p>
<p class="note">Nguồn hiện tại không có district, vì vậy chưa có thống kê theo quận cảnh sát. community_area là khu dân cư. Số vụ ở đây là số bản ghi sau khử trùng 12 trường; dữ liệu không có ID vụ việc.</p>
<p>Commit: <code>{html.escape(source['commit'])}</code> · Lần chạy: {html.escape(source['run_id'])}</p></header>{''.join(sections)}
<footer>Số liệu tổng hợp bằng Spark từ Parquet Silver. Bảng CSV, manifest và checksum nằm trong artifact cùng báo cáo.</footer></main></body></html>'''
    (output / "index.html").write_text(document, encoding="utf-8")
    write_json(output / "validation.json", {**metadata(), "silver_rows": expected, "charts": image_count,
                                            "groups": 4, "vietnamese_font": True, "district_available": False})
    spark = make_spark("Chicago-XuatBaoCao")
    try:
        publish(spark, output, f"/chicago-crimes/gold/{os.getenv('CHICAGO_RUN_ID', 'manual')}/report",
                {"so_vu": expected, "so_bieu_do": image_count})
    finally:
        spark.stop()
    print(f"Đã xác nhận {image_count} biểu đồ và xuất báo cáo HTML tiếng Việt.", flush=True)


if __name__ == "__main__":
    build_report()
