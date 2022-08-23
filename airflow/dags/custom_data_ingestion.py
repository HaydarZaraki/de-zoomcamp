import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

# Our airflow operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Helps us to interact with GCP Storage
from google.cloud import storage

# To allow us to interact with BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Helps to convert our data to parquet format
import pyarrow.csv as pv
import pyarrow.parquet as pq

# Take environmental variables into local variables. These were set in the docker-compose setup.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag_v02",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,12,31),
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )


    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    download_dataset_task >> local_to_gcs_task

fhv_file = "fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
fhv_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{fhv_file}"

with DAG(
    dag_id="data_ingestion_gcs_fhv",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,12,31),
    max_active_runs=3,
    tags=['dtc-de'],
) as dag_1:

    download_fhv_dataset_task = BashOperator(
        task_id="download_fhv_dataset_task",
        bash_command=f"curl -sSLf {fhv_url} > {path_to_local_home}/{fhv_file}"
    )


    local_fhv_to_gcs_task = PythonOperator(
        task_id="local_fhv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{fhv_file}",
            "local_file": f"{path_to_local_home}/{fhv_file}",
        },
    )

    download_fhv_dataset_task >> local_fhv_to_gcs_task

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
lookup_file = "taxi_zone_lookup.csv"
parquet_file = lookup_file.replace('.csv', '.parquet')
with DAG(
    dag_id="data_ingestion_gcs_lookup",
    schedule_interval="@once",
    default_args=default_args,
    start_date=days_ago(1),
    max_active_runs=3,
    tags=['dtc-de'],
) as dag_2:

    download_lookup_dataset_task = BashOperator(
        task_id="download_lookup_dataset_task",
        bash_command=f"curl -sSLf {lookup_url} > {path_to_local_home}/{parquet_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    local_lookup_to_gcs_task = PythonOperator(
        task_id="local_lookup_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    download_lookup_dataset_task >> format_to_parquet_task >> local_lookup_to_gcs_task