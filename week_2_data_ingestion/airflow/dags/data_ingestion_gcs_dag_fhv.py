import os

from datetime import datetime

from airflow import DAG

from download_upload_script import download_upload_gcs_dag

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GCS_PATH = "raw/fhv_tripdata/" \
           "{{ execution_date.strftime(\'%Y\') }}/" \
           "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\')}}.parquet "

fhv_data_dag = DAG(
    "data_ingestion_fhv3",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 1, 1),
    tags=['dtc-de-test'],
    catchup=True,
    max_active_runs=2
)

download_upload_gcs_dag(
    project_id=PROJECT_ID,
    dag=fhv_data_dag,
    url_template=URL_TEMPLATE,
    local_parquet_template=OUTPUT_FILE_TEMPLATE,
    gcs_path_template=GCS_PATH,
    gcs_bucket=BUCKET,
    bigquery_dataset=BIGQUERY_DATASET,
    bigquery_tablename="fhv_tripdata"
)
