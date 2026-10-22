"""Điểm chạy Linux: tái sử dụng cấu hình, metadata và block HDFS trong Ubuntu."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

PROJECT_DIR = Path(__file__).resolve().parents[1]
BLOCK_SIZE = "134217728"


def read_properties(directory: Path) -> dict[str, str]:
    values = {}
    for name in ("core-site.xml", "hdfs-site.xml"):
        path = directory / name
        if path.is_file():
            for prop in ET.parse(path).getroot().findall("property"):
                key, value = prop.findtext("name"), prop.findtext("value")
                if key and value is not None:
                    values[key.strip()] = value.strip()
    return values


def expand_value(value: str, properties: dict) -> str:
    substitutions = {
        "user.home": str(Path.home()), "user.name": os.environ.get("USER", Path.home().name),
        "java.io.tmpdir": "/tmp",
        **properties,
    }
    substitutions.setdefault("hadoop.tmp.dir", "/tmp/hadoop-${user.name}")
    for _ in range(12):
        changed = re.sub(
            r"\$\{([^}]+)\}", lambda m: substitutions.get(m.group(1), m.group(0)), value
        )
        if changed == value:
            break
        value = changed
    if "${" in value:
        raise RuntimeError(f"Không giải được biến trong cấu hình HDFS: {value}")
    return value


def local_storage_dirs(value: str, properties: dict) -> list[Path]:
    directories = []
    for item in expand_value(value, properties).split(","):
        item = re.sub(r"^\[[A-Z_]+\]", "", item.strip())
        uri = urlsplit(item)
        if uri.scheme == "file":
            path = Path(url2pathname(uri.path))
        elif not uri.scheme:
            path = Path(item)
        else:
            raise RuntimeError(f"Đường dẫn lưu block/metadata không phải filesystem local: {item}")
        if not path.is_absolute():
            raise RuntimeError(f"Cần đường dẫn lưu trữ tuyệt đối trong cấu hình HDFS: {item}")
        directories.append(path)
    return directories


def cluster_id(directory: Path) -> str | None:
    version = directory / "current" / "VERSION"
    if not version.is_file():
        return None
    for line in version.read_text(encoding="utf-8").splitlines():
        if line.startswith("clusterID="):
            return line.partition("=")[2].strip()
    return None


def filesystem_uri(properties: dict) -> str:
    uri = properties.get("fs.defaultFS", "")
    parsed = urlsplit(uri)
    if parsed.scheme != "hdfs" or not parsed.hostname:
        raise RuntimeError("core-site.xml chưa có fs.defaultFS dạng hdfs://<NameNode>:<cổng>.")
    return uri


def endpoint(uri: str) -> tuple:
    parsed = urlsplit(uri)
    host = parsed.hostname
    if host in ("localhost", "127.0.0.1", "::1"):
        host = "loopback"
    return host, parsed.port or 8020


def find_namenode_conf(settings: dict, hadoop_home: Path, node_confs: list[Path]) -> Path:
    explicit = settings.get("namenode_conf")
    if explicit:
        path = Path(explicit).expanduser()
        filesystem_uri(read_properties(path))
        return path
    candidates = [
        Path(os.environ["HADOOP_CONF_DIR"]) if os.environ.get("HADOOP_CONF_DIR") else None,
        hadoop_home / "etc" / "hadoop", Path.home() / "hadoop" / "etc" / "hadoop",
        Path.home() / "hadoop-conf" / "namenode",
        *node_confs,
    ]
    node_endpoints = {
        endpoint(filesystem_uri(read_properties(path))) for path in node_confs
    }
    if len(node_endpoints) != 1:
        raise RuntimeError("Ba cấu hình DataNode đang trỏ tới các NameNode khác nhau.")
    matches = {}
    for path in candidates:
        if path is None or not path.is_dir():
            continue
        properties = read_properties(path)
        try:
            if endpoint(filesystem_uri(properties)) not in node_endpoints:
                continue
            storage = local_storage_dirs(
                properties.get("dfs.namenode.name.dir", "${hadoop.tmp.dir}/dfs/name"), properties
            )
        except RuntimeError:
            continue
        if any(cluster_id(directory) for directory in storage):
            key = tuple(sorted(str(p.resolve()) for p in storage))
            matches.setdefault(key, path)
    if len(matches) != 1:
        raise RuntimeError(
            "Chưa xác định duy nhất cấu hình NameNode có metadata đã format. "
            "Hãy đặt namenode_conf trong hdfs_wsl.json tới thư mục cấu hình đang dùng. "
            "Script không tự format NameNode."
        )
    return next(iter(matches.values()))


def find_java_home() -> str:
    supplied = os.environ.get("JAVA_HOME")
    if supplied and (Path(supplied) / "bin" / "java").is_file():
        return supplied
    roots = sorted(Path("/usr/lib/jvm").glob("*17*"))
    roots += sorted(Path("/usr/lib/jvm").glob("*11*"))
    for root in roots:
        if (root / "bin" / "java").is_file():
            return str(root)
    java = shutil.which("java")
    if java:
        return str(Path(java).resolve().parents[1])
    raise RuntimeError("Ubuntu chưa tìm thấy Java. Cần Java tương thích với Hadoop/Spark, ưu tiên Java 17.")


def set_property(root: ET.Element, key: str, value: str) -> None:
    for prop in list(root.findall("property")):
        if prop.findtext("name") == key:
            root.remove(prop)
    prop = ET.SubElement(root, "property")
    ET.SubElement(prop, "name").text = key
    ET.SubElement(prop, "value").text = value


def find_spark_python(settings: dict) -> str:
    """Spark 3.5.1 dùng Python 3.8–3.11; ưu tiên bản 3.11 uv đã có trong Ubuntu."""
    explicit = settings.get("spark_python")
    candidates = [Path(explicit).expanduser()] if explicit else []
    uv_root = Path.home() / ".local" / "share" / "uv" / "python"
    for minor in (11, 10, 9, 8):
        candidates.extend(sorted(uv_root.glob(f"cpython-3.{minor}*/bin/python3")))
        installed = shutil.which(f"python3.{minor}")
        if installed:
            candidates.append(Path(installed))
    if not candidates and shutil.which("python3"):
        candidates.append(Path(shutil.which("python3")))
    for python in candidates:
        if not python.is_file():
            continue
        result = subprocess.run(
            [str(python), "-c", "import sys; print('%s.%s' % sys.version_info[:2])"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        if result.returncode == 0 and result.stdout.strip() in ("3.8", "3.9", "3.10", "3.11"):
            return str(python)
        if explicit:
            break
    raise RuntimeError(
        "Spark 3.5.1 cần Python 3.8–3.11. Đặt spark_python trong hdfs_wsl.json "
        "tới bản Python tương thích trong Ubuntu; script không tự tải Python."
    )


def make_overlay(original: Path, destination: Path, hadoop_home: Path, env: dict,
                 uri: str, component: str) -> Path:
    """Chỉ sửa bản cấu hình riêng; giữ nguyên mọi đường dẫn metadata/block gốc."""
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    base = hadoop_home / "etc" / "hadoop"
    for folder in (base, original):
        if folder.is_dir():
            for source in folder.iterdir():
                if source.is_file() and source.suffix in (".xml", ".properties", ".sh"):
                    shutil.copy2(source, destination / source.name)
    for name in ("core-site.xml", "hdfs-site.xml"):
        path = destination / name
        tree = ET.parse(path) if path.exists() else ET.ElementTree(ET.Element("configuration"))
        root = tree.getroot()
        overrides = {"fs.defaultFS": uri} if name == "core-site.xml" else {
            "dfs.replication": "2", "dfs.blocksize": BLOCK_SIZE,
        }
        original_properties = read_properties(original)
        if name == "hdfs-site.xml":
            storage_key = "dfs.datanode.data.dir" if component.startswith("dn") else "dfs.namenode.name.dir"
            default = "${hadoop.tmp.dir}/dfs/data" if component.startswith("dn") else "${hadoop.tmp.dir}/dfs/name"
            overrides[storage_key] = ",".join(
                path.as_uri() for path in local_storage_dirs(
                    original_properties.get(storage_key, default), original_properties
                )
            )
        for key, value in overrides.items():
            set_property(root, key, value)
        tree.write(path, encoding="utf-8", xml_declaration=True)
    cache = destination.parents[1]
    pid_dir, log_dir = cache / "pids" / component, cache / "logs" / component
    pid_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    env_path = destination / "hadoop-env.sh"
    existing = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
    exports = {
        "JAVA_HOME": env["JAVA_HOME"], "HADOOP_PID_DIR": str(pid_dir),
        "HADOOP_LOG_DIR": str(log_dir), "HADOOP_IDENT_STRING": f"chicago-{component}",
    }
    env_path.write_text(
        existing + "\n# Cấu hình riêng của dự án Chicago Crimes.\n"
        + "\n".join(f"export {key}={shlex.quote(value)}" for key, value in exports.items()) + "\n",
        encoding="utf-8",
    )
    return destination


class Runtime:
    def __init__(self):
        self.settings = json.loads((PROJECT_DIR / "hdfs_wsl.json").read_text(encoding="utf-8-sig"))
        self.home = Path(self.settings["hadoop_home"]).expanduser()
        if not (self.home / "bin" / "hdfs").is_file():
            raise RuntimeError(f"Không tìm thấy Hadoop hiện có: {self.home}/bin/hdfs")
        self.node_confs = [Path(value).expanduser() for value in self.settings["datanode_confs"]]
        if len(self.node_confs) != 3:
            raise RuntimeError("hdfs_wsl.json cần đúng ba thư mục cấu hình DataNode.")
        for path in self.node_confs:
            if not (path / "hdfs-site.xml").is_file() or not (path / "core-site.xml").is_file():
                raise RuntimeError(f"Thiếu core-site.xml hoặc hdfs-site.xml tại {path}")
        self.namenode_conf = find_namenode_conf(self.settings, self.home, self.node_confs)
        self.uri = filesystem_uri(read_properties(self.namenode_conf))
        self.env = {
            **os.environ, "HADOOP_HOME": str(self.home), "JAVA_HOME": find_java_home(),
            "PYTHONIOENCODING": "utf-8",
        }
        self.env.pop("HADOOP_USER_NAME", None)
        self.cache = Path.home() / ".cache" / "chicago-crimes-hdfs"
        self.client_conf = make_overlay(
            self.namenode_conf, self.cache / "conf" / "client",
            self.home, self.env, self.uri, "client",
        )

    def command(self, *args, conf=None):
        return ["bash", str(self.home / "bin" / "hdfs"), "--config",
                str(conf or self.client_conf), *args]

    def report(self, timeout=15):
        try:
            return subprocess.run(
                self.command("dfsadmin", "-report", "-live"), env=self.env,
                capture_output=True, text=True, timeout=timeout, check=False,
            )
        except subprocess.TimeoutExpired:
            return None

    def wait_for_nodes(self, expected: int) -> str:
        deadline = time.monotonic() + self.settings.get("startup_timeout_seconds", 120)
        latest = ""
        while time.monotonic() < deadline:
            report = self.report()
            if report:
                latest = report.stderr.strip() or report.stdout
                match = re.search(r"Live datanodes\s*\((\d+)\)", report.stdout)
                if report.returncode == 0 and match and int(match.group(1)) >= expected:
                    return report.stdout
            time.sleep(2)
        raise RuntimeError(f"HDFS chưa sẵn sàng với {expected} DataNode.\n{latest}")

    def validate_storage_and_ports(self):
        used_dirs, used_ports = set(), set()
        nn_props = read_properties(self.namenode_conf)
        nn_dirs = local_storage_dirs(
            nn_props.get("dfs.namenode.name.dir", "${hadoop.tmp.dir}/dfs/name"), nn_props
        )
        nn_ids = {cluster_id(path) for path in nn_dirs if cluster_id(path)}
        if len(nn_ids) != 1:
            raise RuntimeError("Metadata NameNode chưa có clusterID duy nhất; không tự format hoặc sửa metadata.")
        for conf in self.node_confs:
            properties = read_properties(conf)
            if endpoint(filesystem_uri(properties)) != endpoint(self.uri):
                raise RuntimeError(f"Cấu hình {conf} trỏ tới NameNode khác {self.uri}.")
            if "dfs.datanode.data.dir" not in properties:
                raise RuntimeError(f"Cần chỉ rõ dfs.datanode.data.dir trong {conf}/hdfs-site.xml.")
            for storage in local_storage_dirs(properties["dfs.datanode.data.dir"], properties):
                resolved = str(storage.resolve())
                if resolved in used_dirs or storage.resolve() in [p.resolve() for p in nn_dirs]:
                    raise RuntimeError(f"Đường dẫn lưu trữ DataNode bị dùng chung: {storage}")
                used_dirs.add(resolved)
                existing_id = cluster_id(storage)
                if existing_id and existing_id not in nn_ids:
                    raise RuntimeError(f"DataNode {storage} thuộc cluster khác; giữ nguyên dữ liệu và dừng.")
            for key, default in (
                ("dfs.datanode.address", "0.0.0.0:9866"),
                ("dfs.datanode.http.address", "0.0.0.0:9864"),
                ("dfs.datanode.ipc.address", "0.0.0.0:9867"),
            ):
                address = properties.get(key, default)
                port = int(address.rsplit(":", 1)[1])
                if port and port in used_ports:
                    raise RuntimeError(
                        f"Cổng DataNode bị trùng ({address}) trong {conf}. "
                        "Ba DataNode trên cùng Ubuntu cần các cổng riêng."
                    )
                if port:
                    used_ports.add(port)

    def daemon_running(self, kind: str, original: Path, overlay: Path) -> bool:
        class_name = "org.apache.hadoop.hdfs.server."
        class_name += "namenode.NameNode" if kind == "namenode" else "datanode.DataNode"
        for command in Path("/proc").glob("[0-9]*/cmdline"):
            try:
                text = command.read_bytes().replace(b"\x00", b" ").decode(errors="replace")
            except OSError:
                continue
            if class_name in text and (str(original) in text or str(overlay) in text):
                return True
        return False

    def start_daemon(self, kind: str, component: str, original: Path):
        conf = make_overlay(
            original, self.cache / "conf" / component, self.home, self.env, self.uri, component
        )
        if self.daemon_running(kind, original, conf):
            print(f"Đã có tiến trình {component} đang chạy; sử dụng lại.", flush=True)
            return
        print(f"Đang khởi động {component} bằng Hadoop có sẵn trong Ubuntu...", flush=True)
        result = subprocess.run(
            self.command("--daemon", "start", kind, conf=conf),
            env=self.env, timeout=30, check=False,
        )
        if result.returncode:
            raise RuntimeError(f"Không khởi động được {component}; xem log tại {self.cache / 'logs' / component}.")

    def start(self):
        self.validate_storage_and_ports()
        report = self.report()
        if not report or report.returncode != 0:
            self.start_daemon("namenode", "namenode", self.namenode_conf)
            self.wait_for_nodes(0)
        for number, conf in enumerate(self.node_confs, 1):
            self.start_daemon("datanode", f"dn{number}", conf)
        report = self.wait_for_nodes(3)
        paths = ["/chicago-crimes/bronze", "/chicago-crimes/gold"]
        paths.extend(
            f"/chicago-crimes/silver/year={year}/month={month:02d}"
            for year in range(2018, 2024) for month in range(1, 13)
        )
        subprocess.run(self.command("dfs", "-mkdir", "-p", *paths), env=self.env, check=True)
        print(f"Đã sẵn sàng 3 DataNode trên HDFS có sẵn: {self.uri}", flush=True)
        print("Đã tạo cấu trúc Bronze/Silver/Gold; replication=2, block size=128 MiB.", flush=True)
        print(report, flush=True)

    def normalize(self, spark_hint: str | None, *, check_only: bool = False):
        if not check_only:
            self.wait_for_nodes(3)
        candidates = []
        for value in (self.settings.get("spark_home"), os.environ.get("SPARK_HOME")):
            if value:
                candidates.append(Path(value).expanduser())
        submit = shutil.which("spark-submit")
        if submit:
            candidates.append(Path(submit).resolve().parents[1])
        for root in (Path.home(), Path("/opt"), Path("/usr/local")):
            candidates.extend(sorted(root.glob("spark*")))
        for venv in [Path.home() / "pyspark-env", *sorted((Path.home() / ".venvs").glob("*"))]:
            candidates.extend(sorted(venv.glob("lib/python*/site-packages/pyspark")))
        if spark_hint:
            candidates.append(Path(spark_hint))
        spark_home = next(
            (path for path in candidates if (path / "bin" / "spark-submit").is_file()), None
        )
        if spark_home is None:
            raise RuntimeError(
                "Chưa tìm thấy Spark. Đặt spark_home trong hdfs_wsl.json hoặc dùng "
                "PySpark có sẵn trên Windows để chia sẻ qua WSL."
            )
        python = find_spark_python(self.settings)
        env = {
            **self.env, "SPARK_HOME": str(spark_home), "HADOOP_CONF_DIR": str(self.client_conf),
            "CHICAGO_HDFS_URI": self.uri, "PYSPARK_PYTHON": python, "PYSPARK_DRIVER_PYTHON": python,
            "PYTHONPATH": str(PROJECT_DIR) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        }
        print(f"Dùng Spark tại {spark_home}; HDFS tại {self.uri}.", flush=True)
        script = PROJECT_DIR / ("tests/test_normalize.py" if check_only else "normalize.py")
        os.execvpe("bash", [
            "bash", str(spark_home / "bin" / "spark-submit"),
            "--master", "local[2]", "--driver-memory", "1g",
            str(script),
        ], env)


def main() -> int:
    parser = argparse.ArgumentParser(description="Dùng HDFS có sẵn trong Ubuntu WSL.")
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("start")
    commands.add_parser("status")
    normalize = commands.add_parser("normalize")
    normalize.add_argument("--spark-home-hint")
    checks = commands.add_parser("check-normalize")
    checks.add_argument("--spark-home-hint")
    hdfs = commands.add_parser("hdfs")
    hdfs.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        runtime = Runtime()
        if args.action == "start":
            runtime.start()
        elif args.action == "status":
            print(f"Hadoop: {runtime.home}\nNameNode config: {runtime.namenode_conf}\nHDFS: {runtime.uri}")
            report = runtime.report()
            if report is None:
                raise RuntimeError("NameNode chưa phản hồi.")
            print(report.stdout)
            if report.returncode:
                raise RuntimeError(report.stderr.strip())
        elif args.action in ("normalize", "check-normalize"):
            runtime.normalize(args.spark_home_hint, check_only=args.action == "check-normalize")
        else:
            if not args.arguments:
                parser.error("Cần lệnh hdfs, ví dụ: hdfs dfs -ls /")
            os.execvpe("bash", runtime.command(*args.arguments), runtime.env)
        return 0
    except (OSError, RuntimeError, ValueError, ET.ParseError, subprocess.SubprocessError) as exc:
        print(f"Lỗi HDFS Ubuntu: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
