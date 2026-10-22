"""Cầu nối Windows -> HDFS/Spark có sẵn trong Ubuntu WSL."""

from __future__ import annotations

import json
import subprocess
import sys
from functools import lru_cache
from importlib.util import find_spec
from pathlib import Path
from typing import BinaryIO, Sequence

PROJECT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = PROJECT_DIR / "hdfs_wsl.json"


def load_settings() -> dict:
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig"))


def decode_output(value: bytes) -> str:
    if b"\x00" in value:
        return value.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    return value.decode("utf-8", errors="replace")


def _wsl_prefix() -> list[str]:
    settings = load_settings()
    return [
        "wsl.exe", "--distribution", settings["distribution"],
        "--user", settings["user"], "--exec",
    ]


@lru_cache(maxsize=8)
def to_wsl_path(path: str) -> str:
    try:
        result = subprocess.run(
            [*_wsl_prefix(), "wslpath", "-a", "-u", path],
            capture_output=True, timeout=30, check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Không tìm thấy WSL. Cần Ubuntu WSL để dùng HDFS hiện có.") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            "Ubuntu WSL không phản hồi sau 30 giây. Hãy mở Ubuntu và kiểm tra WSL trước khi chạy lại."
        ) from exc
    if result.returncode != 0:
        detail = decode_output(result.stderr + result.stdout).strip()
        raise RuntimeError(f"Không kết nối được Ubuntu WSL:\n{detail}")
    converted = decode_output(result.stdout).strip()
    if not converted.startswith("/"):
        raise RuntimeError(f"WSL không chuyển được đường dẫn Windows: {path}")
    return converted


def build_wsl_command(action: str, *args: str) -> list[str]:
    helper = to_wsl_path(str(PROJECT_DIR / "scripts" / "hdfs_wsl.py"))
    command = [*_wsl_prefix(), "python3", helper, action, *args]
    if action in ("normalize", "check-normalize"):
        # Dùng chung PySpark đã cài trên Windows qua /mnt, không tải/copy package.
        spec = find_spec("pyspark")
        if spec is not None and spec.origin:
            home = Path(spec.origin).parent
            if (home / "bin" / "spark-submit").is_file():
                command.extend(["--spark-home-hint", to_wsl_path(str(home))])
    return command


def run_hdfs(args: Sequence[str], *, stdin: BinaryIO | None = None) -> subprocess.CompletedProcess:
    command = build_wsl_command("hdfs", "dfs", *args)
    result = subprocess.run(
        command, cwd=PROJECT_DIR, stdin=stdin, capture_output=True, check=False,
    )
    return subprocess.CompletedProcess(
        command, result.returncode, decode_output(result.stdout), decode_output(result.stderr)
    )


def main() -> int:
    if len(sys.argv) < 2:
        print("Cách chạy: python wsl_runtime.py start|status|normalize|check-normalize|hdfs <lệnh>", file=sys.stderr)
        return 2
    try:
        result = subprocess.run(
            build_wsl_command(sys.argv[1], *sys.argv[2:]), cwd=PROJECT_DIR, check=False,
        )
        return result.returncode
    except (OSError, RuntimeError) as exc:
        print(f"Lỗi: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
