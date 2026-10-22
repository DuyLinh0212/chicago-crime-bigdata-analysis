"""DAG thật: kiểm tra HDFS → Bronze → Silver → bốn phân tích → báo cáo."""
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


def command(stage, log):
    return (f"set -Eeuo pipefail; {PYTHON} -m pipeline.stages {stage} "
            f"2>&1 | tee {shlex.quote(str(ROOT / 'artifacts/logs' / log))}")


with DAG(
    dag_id="chicago_crimes_pipeline",
    description="Đẩy dữ liệu lên HDFS, chuẩn hóa Parquet, phân tích và xuất báo cáo tiếng Việt.",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Chicago"),
    schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10), "execution_timeout": timedelta(minutes=15)},
    tags=["chicago", "hdfs", "parquet", "tieng-viet"],
) as dag:
    prepare = BashOperator(
        task_id="kiem_tra_hdfs", cwd=str(ROOT), retries=0,
        env={"CHICAGO_FORCE_FAILURE": "{{ dag_run.conf.get('force_failure', False) }}"}, append_env=True,
        bash_command=("set -Eeuo pipefail; if [ \"$CHICAGO_FORCE_FAILURE\" = \"True\" ]; then "
                      "echo 'Lỗi có chủ đích: xác minh DAG truyền trạng thái thất bại.'; exit 42; fi; "
                      + command("prepare", "dag-01-hdfs.log")),
    )
    upload = BashOperator(task_id="upload_bronze", cwd=str(ROOT), bash_command=command("upload", "dag-02-upload.log"))
    normalize = BashOperator(task_id="chuan_hoa_parquet", cwd=str(ROOT), bash_command=command("normalize", "dag-03-normalize.log"))
    verify = BashOperator(task_id="kiem_tra_silver", cwd=str(ROOT), bash_command=command("verify", "dag-04-verify.log"))
    with TaskGroup(group_id="phan_tich") as analyses:
        for task_id, stage in [("thoi_gian", "temporal"), ("loai_toi_pham", "crime-types"),
                               ("khong_gian", "spatial"), ("phan_tich_cheo", "cross")]:
            BashOperator(task_id=task_id, cwd=str(ROOT), bash_command=command(stage, f"dag-05-{stage}.log"))
    report = BashOperator(task_id="xuat_bao_cao_html", cwd=str(ROOT), bash_command=command("report", "dag-06-report.log"))
    screenshot = BashOperator(
        task_id="xuat_anh_bao_cao", cwd=str(ROOT),
        bash_command=f"set -Eeuo pipefail; {PYTHON} screenshots/capture_evidence.py --report-only 2>&1 | tee artifacts/logs/dag-07-report-image.log",
    )
    gold = BashOperator(task_id="cong_bo_bao_cao_gold", cwd=str(ROOT), bash_command=command("publish-report", "dag-08-gold.log"))
    export = BashOperator(task_id="xuat_parquet_artifact", cwd=str(ROOT), bash_command=command("export", "dag-09-export.log"))
    validate = BashOperator(task_id="xac_nhan_ket_qua", cwd=str(ROOT), bash_command=command("validate-all", "dag-10-validate.log"))
    prepare >> upload >> normalize >> verify >> analyses >> report >> screenshot >> gold >> export >> validate
