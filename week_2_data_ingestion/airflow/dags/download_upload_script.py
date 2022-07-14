from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage


def download_upload_gcs_dag(
        project_id,
        dag,
        url_template,
        local_parquet_template,
        gcs_path_template,
        gcs_bucket,
        bigquery_dataset,
        bigquery_tablename
):
    with dag:
        download_dataset_task = BashOperator(
            task_id='download_dataset_task',
            bash_command=f'curl -sSLf {url_template} > {local_parquet_template}'
            # -f command will fail and exit if needed
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": gcs_bucket,
                "object_name": f"{gcs_path_template}",
                "local_file": f"{local_parquet_template}",
            },
        )
        remove_data_task = BashOperator(
            task_id="rm_data_task",
            bash_command=f"rm {local_parquet_template}"
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": bigquery_dataset,
                    "tableId": bigquery_tablename,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{gcs_bucket}/{gcs_path_template}"],
                },
            },
        )
        download_dataset_task >> local_to_gcs_task >> remove_data_task >> bigquery_external_table_task


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
