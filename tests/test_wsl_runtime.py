"""Kiểm tra cấu hình/bridge WSL bằng fixture, không khởi động HDFS thật."""

import importlib.util
from contextlib import redirect_stdout
import io
import json
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import wsl_runtime

spec = importlib.util.spec_from_file_location("native_hdfs", ROOT / "scripts" / "hdfs_wsl.py")
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)


def xml_properties(folder, name, properties):
    folder.mkdir(parents=True, exist_ok=True)
    root = ET.Element("configuration")
    for key, value in properties.items():
        native.set_property(root, key, value)
    ET.ElementTree(root).write(folder / name, encoding="utf-8", xml_declaration=True)


class NativeHdfsChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="chicago-wsl-check-")
        self.root = Path(self.temp.name)
        self.home = self.root / "hadoop"
        self.nn = self.home / "etc" / "hadoop"
        self.storage = self.root / "namenode"
        (self.storage / "current").mkdir(parents=True)
        (self.storage / "current" / "VERSION").write_text("clusterID=CID-existing\n")
        xml_properties(self.nn, "core-site.xml", {"fs.defaultFS": "hdfs://localhost:19000"})
        xml_properties(self.nn, "hdfs-site.xml", {
            "dfs.namenode.name.dir": self.storage.as_uri(), "dfs.replication": "3",
        })
        self.nodes = []
        for i in range(1, 4):
            conf = self.root / f"dn{i}"
            data = self.root / "blocks" / f"dn{i}"
            (data / "current").mkdir(parents=True)
            (data / "current" / "VERSION").write_text("clusterID=CID-existing\n")
            xml_properties(conf, "core-site.xml", {"fs.defaultFS": "hdfs://127.0.0.1:19000"})
            xml_properties(conf, "hdfs-site.xml", {
                "dfs.datanode.data.dir": data.as_uri(),
                "dfs.datanode.address": f"0.0.0.0:{19866+i*10}",
                "dfs.datanode.http.address": f"0.0.0.0:{19864+i*10}",
                "dfs.datanode.ipc.address": f"0.0.0.0:{19867+i*10}",
            })
            self.nodes.append(conf)
        self.runtime = native.Runtime.__new__(native.Runtime)
        self.runtime.home = self.home
        self.runtime.namenode_conf = self.nn
        self.runtime.node_confs = self.nodes
        self.runtime.uri = "hdfs://localhost:19000"
        self.runtime.cache = self.root / "cache"
        self.runtime.env = {"JAVA_HOME": "/test/java17"}

    def tearDown(self):
        self.temp.cleanup()

    def test_discovers_formatted_existing_namenode(self):
        with mock.patch.dict(native.os.environ, {"HADOOP_CONF_DIR": ""}):
            selected = native.find_namenode_conf({}, self.home, self.nodes)
        self.assertEqual(selected, self.nn)
        self.runtime.validate_storage_and_ports()

    def test_refuses_missing_metadata_without_format(self):
        (self.storage / "current" / "VERSION").unlink()
        with mock.patch.dict(native.os.environ, {"HADOOP_CONF_DIR": ""}):
            with self.assertRaisesRegex(RuntimeError, "không tự format"):
                native.find_namenode_conf({}, self.home, self.nodes)

    def test_overlay_keeps_source_and_storage_and_sets_defaults(self):
        before = (self.nodes[0] / "hdfs-site.xml").read_bytes()
        destination = self.root / "cache" / "conf" / "dn1"
        native.make_overlay(
            self.nodes[0], destination, self.home, self.runtime.env, self.runtime.uri, "dn1"
        )
        values = native.read_properties(destination)
        self.assertEqual(values["dfs.replication"], "2")
        self.assertEqual(values["dfs.blocksize"], "134217728")
        self.assertEqual(
            native.local_storage_dirs(values["dfs.datanode.data.dir"], values),
            [self.root / "blocks" / "dn1"],
        )
        self.assertEqual((self.nodes[0] / "hdfs-site.xml").read_bytes(), before)
        self.assertEqual(native.cluster_id(self.root / "blocks" / "dn1"), "CID-existing")
        self.assertIn("chicago-dn1", (destination / "hadoop-env.sh").read_text(encoding="utf-8"))

    def test_refuses_other_cluster_and_duplicate_ports(self):
        version = self.root / "blocks" / "dn2" / "current" / "VERSION"
        version.write_text("clusterID=CID-other\n")
        with self.assertRaisesRegex(RuntimeError, "cluster khác"):
            self.runtime.validate_storage_and_ports()
        version.write_text("clusterID=CID-existing\n")
        values = native.read_properties(self.nodes[1])
        values["dfs.datanode.address"] = "0.0.0.0:19876"
        xml_properties(self.nodes[1], "hdfs-site.xml", values)
        with self.assertRaisesRegex(RuntimeError, "Cổng DataNode bị trùng"):
            self.runtime.validate_storage_and_ports()

    def test_starts_daemon_without_format_command(self):
        with mock.patch.object(self.runtime, "daemon_running", return_value=False):
            with mock.patch.object(native.subprocess, "run") as run:
                run.return_value = subprocess.CompletedProcess([], 0)
                with redirect_stdout(io.StringIO()):
                    self.runtime.start_daemon("datanode", "dn1", self.nodes[0])
        args = run.call_args.args[0]
        self.assertIn("--daemon", args)
        self.assertEqual(args[-2:], ["start", "datanode"])
        self.assertNotIn("-format", args)

    def test_explicit_python_version_is_checked(self):
        python = self.root / "python"
        python.touch()
        with mock.patch.object(native.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "3.14\n", "")
            with self.assertRaisesRegex(RuntimeError, "Python 3.8"):
                native.find_spark_python({"spark_python": str(python)})
            run.return_value = subprocess.CompletedProcess([], 0, "3.11\n", "")
            self.assertEqual(native.find_spark_python({"spark_python": str(python)}), str(python))


class WindowsBridgeChecks(unittest.TestCase):
    def test_utf16_wsl_errors_decode(self):
        message = "Lỗi WSL 0x8007274c"
        self.assertEqual(wsl_runtime.decode_output(message.encode("utf-16-le")), message)

    def test_command_targets_ubuntu_user_without_docker(self):
        settings = {"distribution": "Ubuntu", "user": "linh"}
        with mock.patch.object(wsl_runtime, "load_settings", return_value=settings):
            with mock.patch.object(wsl_runtime, "to_wsl_path", return_value="/mnt/d/project/scripts/hdfs_wsl.py"):
                command = wsl_runtime.build_wsl_command("hdfs", "dfs", "-ls", "/")
        self.assertEqual(command[:6], ["wsl.exe", "--distribution", "Ubuntu", "--user", "linh", "--exec"])
        self.assertNotIn("docker", command)
        self.assertEqual(command[-4:], ["hdfs", "dfs", "-ls", "/"])

    def test_upload_stream_is_passed_directly(self):
        stream = io.BytesIO(b"\xef\xbb\xbfdata,unchanged\n")
        with mock.patch.object(wsl_runtime, "build_wsl_command", return_value=["wsl.exe", "fake"]):
            with mock.patch.object(wsl_runtime.subprocess, "run") as run:
                run.return_value = subprocess.CompletedProcess([], 0, b"ok", b"")
                result = wsl_runtime.run_hdfs(["-put", "-", "/bronze/file.csv"], stdin=stream)
        self.assertIs(run.call_args.kwargs["stdin"], stream)
        self.assertEqual(result.stdout, "ok")
        self.assertEqual(stream.getvalue(), b"\xef\xbb\xbfdata,unchanged\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
