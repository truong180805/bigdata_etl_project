from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cho Airflow tìm thấy file scripts/etl_tasks.py
sys.path.append("/opt/airflow/scripts")

from etl_tasks import run_etl_pipeline


default_args = {
    "owner": "team_b",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="order_etl_pipeline",
    description="Pipeline ETL/ELT xử lý dữ liệu đơn hàng thương mại điện tử",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ecommerce", "bigdata"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl_pipeline,
        op_kwargs={
            "logical_date_str": "{{ ds }}"
        },
    )

    run_etl