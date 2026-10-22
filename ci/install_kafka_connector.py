"""Cài connector đúng Spark 3.5.1 khi build trên GitHub, xác minh SHA-1 Maven."""
import hashlib
import time
import urllib.request
from pathlib import Path

ARTIFACTS = [
    ("org/apache/spark", "spark-sql-kafka-0-10_2.12", "3.5.1"),
    ("org/apache/spark", "spark-token-provider-kafka-0-10_2.12", "3.5.1"),
    ("org/apache/kafka", "kafka-clients", "3.4.1"),
    ("org/apache/commons", "commons-pool2", "2.11.1"),
]


def fetch(url):
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=45) as response:
                return response.read()
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2)


if __name__ == "__main__":
    for group, name, version in ARTIFACTS:
        filename = f"{name}-{version}.jar"
        url = f"https://repo.maven.apache.org/maven2/{group}/{name}/{version}/{filename}"
        payload = fetch(url)
        expected = fetch(url + ".sha1").decode().strip().split()[0]
        if hashlib.sha1(payload).hexdigest() != expected:
            raise RuntimeError(f"Checksum connector không khớp: {filename}")
        (Path("/opt/spark/jars") / filename).write_bytes(payload)
        print(f"Đã cài và kiểm tra connector: {filename}", flush=True)
