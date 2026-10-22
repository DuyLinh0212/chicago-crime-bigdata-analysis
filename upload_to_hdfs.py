"""Tải file từ ./data lên HDFS có sẵn trong Ubuntu WSL."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from wsl_runtime import run_hdfs


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
HDFS_BRONZE_ROOT = "/chicago-crimes/bronze"


def parse_ingest_date(value: str) -> str:
    """Validate and return an ISO date used as the HDFS partition name."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Ngày ingest phải có định dạng YYYY-MM-DD, ví dụ 2026-10-15."
        ) from exc


def get_files(data_dir: Path = DATA_DIR) -> list[Path]:
    if not data_dir.is_dir():
        raise RuntimeError(f"Không tìm thấy thư mục dữ liệu: {data_dir}")

    return sorted(
        (path for path in data_dir.rglob("*") if path.is_file() and not path.is_symlink() and path.suffix.lower() == ".csv"),
        key=lambda path: path.relative_to(data_dir).as_posix().casefold(),
    )


def upload_file(source: Path, destination: str, *, overwrite: bool, data_dir=DATA_DIR, execute=run_hdfs) -> bool:
    """Stream one local file into HDFS; return False when an existing file is skipped."""
    relative_path = source.relative_to(data_dir).as_posix()
    parent = destination.rpartition("/")[0]

    if not overwrite:
        exists = execute(["-test", "-e", destination])
        if exists.returncode == 0:
            print(f"Bỏ qua (đã tồn tại): {relative_path} -> {destination}")
            return False

    mkdir = execute(["-mkdir", "-p", parent])
    if mkdir.returncode != 0:
        raise RuntimeError(
            f"Không tạo được thư mục HDFS {parent}:\n{mkdir.stderr.strip()}"
        )

    put_args = ["-put"]
    if overwrite:
        put_args.append("-f")
    put_args.extend(["-", destination])

    print(f"Đang tải: {relative_path} -> {destination}", flush=True)
    with source.open("rb") as input_file:
        result = execute(put_args, stdin=input_file)

    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "Lỗi không rõ nguyên nhân."
        raise RuntimeError(f"Tải thất bại: {relative_path}\n{detail}")

    print(f"Đã tải xong: {relative_path}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Tải file trong thư mục data lên HDFS bronze theo ngày ingest."
    )
    parser.add_argument("--backend", choices=["wsl", "docker"], default="wsl", help="Môi trường HDFS.")
    parser.add_argument("--compose-file", type=Path, default=PROJECT_DIR / "ci/compose.yaml")
    parser.add_argument("--project-name", default="chicago-ci")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument(
        "--ingest-date",
        type=parse_ingest_date,
        default=date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Ngày phân vùng HDFS (mặc định là ngày hiện tại).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Ghi đè file cùng tên đã có trong phân vùng ingest ngày này.",
    )
    options = parser.parse_args()

    try:
        files = get_files(options.data_dir)
        execute = run_hdfs
        if options.backend == "docker":
            def execute(args, *, stdin=None):
                return subprocess.run([
                    "docker", "compose", "-f", str(options.compose_file), "-p", options.project_name,
                    "exec", "-T", "namenode", "hdfs", "dfs", *args,
                ], stdin=stdin, capture_output=True, encoding=None if stdin else "utf-8", check=False)
            binary_execute = execute
            def execute(args, *, stdin=None):
                result = binary_execute(args, stdin=stdin)
                if isinstance(result.stdout, bytes):
                    result.stdout = result.stdout.decode("utf-8", errors="replace")
                    result.stderr = result.stderr.decode("utf-8", errors="replace")
                return result
        if not files:
            print(
                f"Không tìm thấy file nào trong {DATA_DIR}. "
                "Hãy chép dữ liệu vào thư mục này rồi chạy lại.",
                file=sys.stderr,
            )
            return 1

        partition = f"ingest_date={options.ingest_date}"
        uploaded = 0
        skipped = 0

        for source in files:
            relative_path = source.relative_to(options.data_dir).as_posix()
            destination = f"{HDFS_BRONZE_ROOT}/{partition}/{relative_path}"
            if upload_file(source, destination, overwrite=options.overwrite, data_dir=options.data_dir, execute=execute):
                uploaded += 1
            else:
                skipped += 1

        print(
            f"Hoàn tất. Đã tải {uploaded} file, bỏ qua {skipped} file. "
            f"Đích HDFS: {HDFS_BRONZE_ROOT}/{partition}/"
        )
        return 0
    except (OSError, RuntimeError) as exc:
        print(f"Lỗi: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
