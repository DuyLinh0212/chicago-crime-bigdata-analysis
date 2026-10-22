"""Chụp HDFS và ảnh hiển thị log thật bằng trình duyệt trên GitHub runner."""
import html
import json
import os
import shutil
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts/screenshots"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=shutil.which("google-chrome") or shutil.which("chromium"), args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
        for name, url in [
            ("01-namenode", "http://127.0.0.1:9870/dfshealth.html#tab-datanode"),
            ("02-bronze", "http://127.0.0.1:9870/explorer.html#/chicago-crimes/bronze"),
            ("03-silver", "http://127.0.0.1:9870/explorer.html#/chicago-crimes/silver/year=2018/month=01"),
        ]:
            try:
                page.goto(url, wait_until="networkidle", timeout=25000)
                page.screenshot(path=str(OUT / f"{name}.png"), full_page=True)
                results.append({"image": name, "url": url, "success": True})
            except Exception as exc:
                results.append({"image": name, "success": False, "error": str(exc)})
                print(f"Không chụp được {name}: {exc}", flush=True)
        for logfile in sorted((ROOT / "artifacts/logs").glob("*.log")):
            lines = logfile.read_text(encoding="utf-8", errors="replace").splitlines()
            # Chụp phần cuối có kết quả thật; log đầy đủ vẫn nằm trong artifact.
            body = html.escape("\n".join(lines[-65:]))
            title = html.escape(f"Ảnh hiển thị log thực tế — {logfile.name}")
            provenance = html.escape(f"Commit: {os.getenv('GITHUB_SHA')} | Lần chạy: {os.getenv('CHICAGO_RUN_ID')}")
            page.set_content(f"<html lang='vi'><meta charset='utf-8'><style>body{{background:#111827;color:#e5e7eb;font:16px 'DejaVu Sans',sans-serif;padding:24px}}h1{{font-size:24px;color:#6ee7b7}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.55;font-family:'DejaVu Sans Mono',monospace}}</style><h1>{title}</h1><p>{provenance}</p><pre>{body}</pre></html>")
            page.screenshot(path=str(OUT / f"log-{logfile.stem}.png"), full_page=True)
        report = ROOT / "artifacts/report/index.html"
        if report.is_file():
            page.goto(report.as_uri(), wait_until="networkidle")
            if not page.locator("img").evaluate_all("images => images.length >= 15 && images.every(i => i.complete && i.naturalWidth > 0)"):
                raise RuntimeError("Báo cáo HTML chưa hiển thị đủ 15 ảnh biểu đồ.")
            page.screenshot(path=str(report.parent / "report-overview.png"), full_page=True)
        browser.close()
    (OUT / "capture-manifest.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    if not all(item["success"] for item in results):
        raise RuntimeError("Chưa chụp đủ bằng chứng HDFS; xem capture-manifest.json.")
    print("Đã xuất ảnh HDFS và ảnh hiển thị log thực tế.", flush=True)


if __name__ == "__main__":
    main()
