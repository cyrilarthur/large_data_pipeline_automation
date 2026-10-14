# tourism_pipeline_dag.py
import logging
from datetime import datetime
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
import sys
import os

# ---------------------------
# Ensure Airflow can find your src folder
# ---------------------------
SRC_PATH = "/opt/airflow/src"  # folder inside container where your src code is mounted
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

# Now we can import the pipeline function safely
from data_cleaning import run_cleaning_pipeline

# ---------------------------
# Default DAG arguments
# ---------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# ---------------------------
# Define the DAG
# ---------------------------
with DAG(
    dag_id="tourism_pipeline_dag",
    default_args=default_args,
    description="ETL pipeline for tourism data cleaning",
    start_date=datetime(2026, 3, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=["tourism", "data_cleaning"]
) as dag:

    # ---------------------------
    # Task: Run data cleaning pipeline
    # ---------------------------
    def run_pipeline_task():
        logging.info("Starting data cleaning pipeline...")
        run_cleaning_pipeline()
        logging.info("Data cleaning pipeline completed.")

    run_pipeline = PythonOperator(
        task_id="run_cleaning_pipeline",
        python_callable=run_pipeline_task,
    )

    # Single task DAG
    run_pipeline