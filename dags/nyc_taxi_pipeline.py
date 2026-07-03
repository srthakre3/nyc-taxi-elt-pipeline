"""
NYC Taxi ELT Pipeline DAG
Orchestrates: data ingestion → dbt run → dbt test
Schedule: daily at 6am UTC
"""

import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
DBT_DIR = "/opt/airflow/dbt_project"


def slack_success(context):
    if not SLACK_WEBHOOK:
        return
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    requests.post(SLACK_WEBHOOK, json={
        "text": f":white_check_mark: *{dag_id}* pipeline completed successfully\n`{run_id}`"
    })


def slack_failure(context):
    if not SLACK_WEBHOOK:
        return
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context["task_instance"].log_url
    requests.post(SLACK_WEBHOOK, json={
        "text": f":x: *{dag_id}* failed on task `{task_id}`\n`{run_id}`\n<{log_url}|View logs>"
    })


default_args = {
    "owner": "sanket",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": slack_failure,
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="NYC Taxi ELT: ingest → dbt transform → dbt test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["elt", "nyc-taxi", "dbt"],
    on_success_callback=slack_success,
) as dag:

    # Task 1: Ingest raw data from NYC TLC
    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command="cd /opt/airflow && python ingestion/download_trips.py",
    )

    # Task 2: Install dbt packages (dbt_utils)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
    )

    # Task 3: Run dbt models (staging → intermediate → marts)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    # Task 4: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    # Task 5: Generate dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_DIR} && dbt docs generate --profiles-dir .",
    )

    # DAG order
    ingest >> dbt_deps >> dbt_run >> dbt_test >> dbt_docs
