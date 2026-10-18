from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/scripts")

from etl_tasks import run_etl_pipeline


default_args = {
    "owner": "team_b",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="order_etl_pipeline",
    description="ETL don hang: Incremental, Backfill, Retry, Quarantine",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ecommerce", "bigdata"],
    render_template_as_native_obj=True,
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl_pipeline,
        op_kwargs={
            "logical_date_str": (
                "{{ dag_run.conf.get('backfill_date', ds) "
                "if dag_run and dag_run.conf else ds }}"
            ),
            "backfill": (
                "{{ dag_run.conf.get('backfill', false) "
                "if dag_run and dag_run.conf else false }}"
            ),
        },
    )