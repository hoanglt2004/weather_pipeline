import sys
sys.path.append("/opt/airflow")

import logging
from datetime import datetime

from airflow.operators.python import PythonOperator
# Import Slack alert
from plugins.slack_alert import slack_failure_alert

from airflow import DAG
# Import hàm ETL
from etl.extract.extract_weather import extract_all_cities
from etl.load.load_to_dwh import load_to_dwh
from etl.tranform.tranform_weather import tranform_city

# ============================
#  Task Python functions
# ============================


def extract_task():
    logging.info(">>> Running extract_all_citites() ...")
    file_list = extract_all_cities()
    logging.info(f"Extract done. Files: {file_list}")
    return file_list


def tranform_task(**context):
    file_list = context["ti"].xcom_pull(task_ids="extract_task")

    if not file_list:
        logging.warning("Không có file raw → bỏ qua transform")
        return None

    out_files = []
    for file in file_list:
        parts = file.split("/")
        city = parts[2]
        date = parts[3]

        out_path = tranform_city(city, date)
        out_files.append(out_path)

    logging.info("Transform xong:", out_files)
    return out_files


def load_task(**context):
    logging.info(">>> Running load_to_dwh()")
    parquet_files = context["ti"].xcom_pull(task_ids="tranform_task")

    if not parquet_files:
        logging.warning("Không có file clean → không load được.")
        return None

    for parquet in parquet_files:
        logging.info(f"Loading parquet: {parquet}")
        load_to_dwh(parquet)

    logging.info("Load xong!")


# ============================
#  DAG Definition
# ============================

default_args = {
    "owner": "airflow",
    "email": ["tkdidkdao@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task,
        on_failure_callback=slack_failure_alert,
    )

    tranform = PythonOperator(
        task_id="tranform_task",
        python_callable=tranform_task,
        on_failure_callback=slack_failure_alert,
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
        on_failure_callback=slack_failure_alert,
    )

    extract >> tranform >> load
