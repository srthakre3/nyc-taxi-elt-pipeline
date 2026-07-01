"""
NYC Taxi ELT Pipeline DAG
Orchestrates: data ingestion → dbt run → dbt test
Schedule: daily at 6am UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sanket",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt_project"

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="NYC Taxi ELT: ingest → dbt transform → dbt test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["elt", "nyc-taxi", "dbt"],
) as dag:

    # Task 1: Ingest raw data from NYC TLC
    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command="cd /opt/airflow && python ingestion/download_trips.py",
    )

    # Task 2: Run dbt models (staging → intermediate → marts)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    # Task 4: Generate dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_DIR} && dbt docs generate --profiles-dir .",
    )

    # DAG order
    ingest >> dbt_run >> dbt_test >> dbt_docs
