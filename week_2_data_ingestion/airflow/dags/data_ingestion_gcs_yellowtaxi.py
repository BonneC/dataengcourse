import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage

from download_upload_script import download_upload_gcs_dag

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GCS_PATH = "raw/yellow_tripdata/" \
           "{{ execution_date.strftime(\'%Y\') }}/" \
           "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\')}}.parquet "

yellow_data_dag = DAG(
    "data_ingestion_yellowtaxi3",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 1, 1),
    tags=['dtc-de-test'],
    catchup=True,
    max_active_runs=2
)

download_upload_gcs_dag(
    project_id=PROJECT_ID,
    dag=yellow_data_dag,
    url_template=URL_TEMPLATE,
    local_parquet_template=OUTPUT_FILE_TEMPLATE,
    gcs_path_template=GCS_PATH,
    gcs_bucket=BUCKET,
    bigquery_dataset=BIGQUERY_DATASET,
    bigquery_tablename="yellow_tripdata"
)
