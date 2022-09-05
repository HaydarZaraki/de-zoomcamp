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
yellow_gcp_storage_template = "raw/yellow_taxi_data/{{execution_date.strftime(\'%Y\')}}/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"

def upload_to_gcs(bucket, object_name, local_file):
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
    catchup=True,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021,1,1),
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
            "object_name": yellow_gcp_storage_template,
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    download_dataset_task >> local_to_gcs_task