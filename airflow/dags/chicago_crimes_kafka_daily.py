"""DAG do consumer day_ready kích hoạt; chỉ ghi bổ sung các sự kiện mới."""
import os
import shlex
import sys
from datetime import timedelta
from pathlib import Path
import pendulum
from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator

ROOT = Path(os.environ["CHICAGO_PROJECT_ROOT"])
PYTHON = shlex.quote(os.environ.get("CHICAGO_HOST_PYTHON", sys.executable))


def task(task_id, stage):
    return BashOperator(task_id=task_id, cwd=str(ROOT),
        env={"CHICAGO_BATCH_ID": "{{ dag_run.conf['batch_id'] }}"}, append_env=True,
        bash_command=("set -Eeuo pipefail; [[ \"$CHICAGO_BATCH_ID\" =~ ^demo-2023-12-(29|30|31)$ ]] || exit 2; "
                      f"{PYTHON} -m ingest.stages {stage} 2>&1 | "
                      f'tee -a "artifacts/logs/kafka-$CHICAGO_BATCH_ID-{stage}.log"'))


with DAG(dag_id="chicago_crimes_kafka_daily", description="Kafka theo ngày → Bronze JSONL → Parquet bổ sung → báo cáo tiếng Việt.",
         start_date=pendulum.datetime(2023, 12, 29, tz="America/Chicago"), schedule=None, catchup=False,
         max_active_runs=1, max_active_tasks=1, dagrun_timeout=timedelta(minutes=35),
         default_args={"retries": 1, "retry_delay": timedelta(seconds=10), "execution_timeout": timedelta(minutes=12)},
         tags=["kafka", "mo-phong", "available-now", "tieng-viet"]) as dag:
    check = task("kiem_tra_hdfs", "check")
    ingest = task("kafka_vao_bronze", "ingest")
    normalize = task("bo_sung_parquet", "append")
    verify = task("kiem_tra_silver", "verify")
    with TaskGroup(group_id="phan_tich") as analyses:
        for name, stage in [("thoi_gian", "temporal"), ("loai_toi_pham", "crime-types"), ("khong_gian", "spatial"), ("phan_tich_cheo", "cross")]:
            task(name, stage)
    report = task("xuat_bao_cao_html", "report")
    image = task("xuat_anh_bao_cao", "report-image")
    gold = task("cong_bo_gold", "publish-report")
    validate = task("xac_nhan_ket_qua", "validate-all")
    check >> ingest >> normalize >> verify >> analyses >> report >> image >> gold >> validate
