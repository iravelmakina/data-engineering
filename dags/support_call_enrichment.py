import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from include.pipeline import (
    detect_new_calls,
    load_telephony_details,
    transform_and_load_duckdb,
)

logger = logging.getLogger(__name__)


def alert_on_failure(context):
    """Log an alert when a task fails."""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    exec_date = context["execution_date"]
    logger.error(
        f"ALERT: Task {task_id} in DAG {dag_id} failed on {exec_date}. "
        f"Exception: {context.get('exception')}"
    )


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

dag = DAG(
    "support_call_enrichment",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    description="Hourly enrichment of support calls from MySQL + JSON into DuckDB",
    max_active_runs=1,
    tags=["support", "enrichment", "hourly"],
)

detect = PythonOperator(
    task_id="detect_new_calls",
    python_callable=detect_new_calls,
    dag=dag,
)

load_telephony = PythonOperator(
    task_id="load_telephony_details",
    python_callable=load_telephony_details,
    dag=dag,
)

transform_load = PythonOperator(
    task_id="transform_and_load_duckdb",
    python_callable=transform_and_load_duckdb,
    dag=dag,
)

detect >> load_telephony >> transform_load
