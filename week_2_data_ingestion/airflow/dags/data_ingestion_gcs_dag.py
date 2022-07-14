import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
PARQUET_NAME = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# dataset_file = "yellow_tripdata_2022-01.parquet"
# dataset_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.info("File is parquet, no transformation needed...")
        # table = pq.read_table(src_file)
        # pq.write_table(table, src_file.replace('.csv', '.parquet'))
    else:
        logging.info("File is .csv, starting transformation...")
        table = pv.read_csv(src_file)
        pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    logging.info("Uploading to gcs...")
    logging.info(f"BUCKET: {BUCKET}")
    logging.info(f"PROJ ID: {PROJECT_ID}")
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


# default_args = {
#     "owner": "airflow",
#     "start_date": days_ago(1),
#     "depends_on_past": False,
#     "retries": 1,
# }

local_workflow = DAG(
    "data_ingestion_gcs_all",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    tags=['dtc-de-test'],
    catchup=True,
    max_active_runs=2
)

with local_workflow:
    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{PARQUET_NAME}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table2",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{PARQUET_NAME}"],
            },
        },
    )

    download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task
