import os

from airflow import DAG
from airflow.utils.dates import days_ago

from download_upload_script import download_upload_gcs_dag

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
GCS_PATH = "raw/taxi_zone/taxi_zone_lookup.parquet"

# todo: CURRENTLY DOESN'T WORK DUE TO NYTAXI DATA DOING CHANGES WITH THEIR STORAGE
zones_data_dag = DAG(
    "data_ingestion_zones",
    schedule_interval="@once",
    start_date=days_ago(1),
    tags=['dtc-de-test'],
    catchup=True,
    max_active_runs=2
)

download_upload_gcs_dag(
    project_id=PROJECT_ID,
    dag=zones_data_dag,
    url_template=URL_TEMPLATE,
    local_parquet_template=OUTPUT_FILE_TEMPLATE,
    gcs_path_template=GCS_PATH,
    gcs_bucket=BUCKET,
    bigquery_dataset="all_tripdata",
    bigquery_tablename="zones"
)
