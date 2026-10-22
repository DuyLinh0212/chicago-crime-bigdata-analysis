"""Chụp HDFS và ảnh hiển thị log thật bằng trình duyệt trên GitHub runner."""
import argparse
import html
import json
import os
import re
import shutil
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / os.getenv("CHICAGO_ARTIFACT_DIR", "artifacts")
OUT = ARTIFACTS / "screenshots"


def capture_report(page, overwrite=False):
    report = ARTIFACTS / "report/index.html"
    if not report.is_file():
        raise RuntimeError("Chưa có báo cáo HTML để xuất ảnh.")
    page.goto(report.as_uri(), wait_until="networkidle")
    if not page.locator("img").evaluate_all("images => images.length >= 15 && images.every(i => i.complete && i.naturalWidth > 0)"):
        raise RuntimeError("Báo cáo HTML chưa hiển thị đủ 15 ảnh biểu đồ.")
    image = report.parent / "report-overview.png"
    if overwrite or not image.is_file():
        page.screenshot(path=str(image), full_page=True)
    print("Đã xác nhận báo cáo hiển thị đủ ảnh và xuất PNG tổng quan.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-only", action="store_true", help="Chỉ xuất PNG báo cáo trong task Airflow.")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=shutil.which("google-chrome") or shutil.which("chromium"), args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
        if args.report_only:
            capture_report(page, overwrite=True)
            browser.close()
            return
        targets = [
            ("01-namenode", "http://127.0.0.1:9870/dfshealth.html#tab-datanode"),
            ("02-bronze", "http://127.0.0.1:9870/explorer.html#/chicago-crimes/bronze"),
            ("03-silver", "http://127.0.0.1:9870/explorer.html#/chicago-crimes/silver/year=2018/month=01"),
        ]
        if os.getenv("CHICAGO_AIRFLOW") == "1":
            targets.append(("04-airflow-dag", "http://127.0.0.1:8080/dags/" + os.getenv("CHICAGO_AIRFLOW_DAG_ID", "chicago_crimes_pipeline")))
        if os.getenv("CHICAGO_KAFKA_MODE") == "1":
            targets[1] = ("02-bronze-kafka", "http://127.0.0.1:9870/explorer.html#/chicago-crimes/bronze/kafka")
            targets[2] = ("03-silver-incremental", "http://127.0.0.1:9870/explorer.html#/chicago-crimes/silver/year=2023/month=12")
        for name, url in targets:
            try:
                page.goto(url, wait_until="networkidle", timeout=25000)
                provenance = (f"BẰNG CHỨNG CI · Commit: {os.getenv('GITHUB_SHA')} · "
                              f"Workflow: {os.getenv('GITHUB_RUN_ID')} · Lần chạy: {os.getenv('CHICAGO_RUN_ID')} · "
                              f"Ngày mô phỏng: {os.getenv('CHICAGO_SIMULATION_DATE', 'toàn bộ')} · "
                              f"Lô: {os.getenv('CHICAGO_BATCH_ID', '—')}")
                page.evaluate("""text => {
                    const banner = document.createElement('div');
                    banner.textContent = text;
                    banner.style.cssText = 'position:fixed;bottom:0;left:0;right:0;z-index:2147483647;background:#0f172a;color:#fff;padding:8px 12px;font:12px sans-serif;pointer-events:none';
                    document.body.appendChild(banner);
                }""", provenance)
                page.screenshot(path=str(OUT / f"{name}.png"), full_page=True)
                results.append({"image": name, "url": url, "success": True, "provenance": provenance})
            except Exception as exc:
                results.append({"image": name, "success": False, "error": str(exc)})
                print(f"Không chụp được {name}: {exc}", flush=True)
        for logfile in sorted((ROOT / "artifacts/logs").glob("*.log")):
            day = os.getenv("CHICAGO_SIMULATION_DATE")
            if day and logfile.name.startswith("kafka-demo-") and day not in logfile.name:
                continue
            lines = logfile.read_text(encoding="utf-8", errors="replace").splitlines()
            # Chụp phần cuối có kết quả thật; log đầy đủ vẫn nằm trong artifact.
            body = html.escape(re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", "\n".join(lines[-65:])))
            title = html.escape(f"Ảnh hiển thị log thực tế — {logfile.name}")
            provenance = html.escape(f"Commit: {os.getenv('GITHUB_SHA')} | Workflow: {os.getenv('GITHUB_RUN_ID')} | Lần chạy: {os.getenv('CHICAGO_RUN_ID')} | Ngày: {day or 'toàn bộ'} | Lô: {os.getenv('CHICAGO_BATCH_ID', '—')}")
            page.set_content(f"<html lang='vi'><meta charset='utf-8'><style>body{{background:#0b1020;color:#e5e7eb;font:15px 'DejaVu Sans',sans-serif;padding:24px}}h1{{font-size:22px;color:#6ee7b7}}.terminal{{border:1px solid #334155;border-radius:12px;overflow:hidden}}.bar{{padding:12px;background:#1e293b;color:#cbd5e1}}pre{{padding:20px;margin:0;white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.5;font-family:'DejaVu Sans Mono',monospace}}</style><h1>{title}</h1><p>{provenance}</p><div class='terminal'><div class='bar'>🔴 🟡 🟢 &nbsp; GitHub runner · Terminal · {html.escape(logfile.name)}</div><pre>$ Nhật ký thực thi thực tế\n{body}</pre></div></html>")
            page.screenshot(path=str(OUT / f"log-{logfile.stem}.png"), full_page=True)
        report = ARTIFACTS / "report/index.html"
        if report.is_file():
            capture_report(page)
        browser.close()
    (OUT / "capture-manifest.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    if not all(item["success"] for item in results):
        raise RuntimeError("Chưa chụp đủ bằng chứng HDFS; xem capture-manifest.json.")
    print("Đã xuất ảnh HDFS và ảnh hiển thị log thực tế.", flush=True)


if __name__ == "__main__":
    main()
