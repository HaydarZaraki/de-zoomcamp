import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

# Our airflow operators

# Helps us to interact with GCP Storage
from google.cloud import storage

# To allow us to interact with BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


# Helps to convert our data to parquet format
import pyarrow.csv as pv
import pyarrow.parquet as pq

# Take environmental variables into local variables. These were set in the docker-compose setup.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "ny_taxi")
# dataset_file = "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
# dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# yellow_gcp_storage_template = "raw/yellow_taxi_data/{{execution_date.strftime(\'%Y\')}}/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"

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
    dag_id="yellow_green_gcs_to_external_partitioned",
    schedule_interval=None,
    default_args=default_args,
    start_date=days_ago(1),
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:
    for color,partition in {'yellow':'tpep_pickup_datetime','green':'lpep_pickup_datetime'}.items():
        bq_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"{color}_bq_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{color}_ext_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/{color}/*.parquet"],
                },
            },
        )
        exclude_columns = ''
        if color == 'yellow':
            exclude_columns = 'except(airport_fee)'
        else:
            exclude_columns = 'except(ehail_fee)'
        QUERY = f"CREATE OR REPLACE TABLE `{PROJECT_ID}.ny_taxi.{color}_taxi_part` PARTITION BY DATE({partition}) AS SELECT * {exclude_columns} FROM `{PROJECT_ID}.ny_taxi.{color}_ext_table`;"
        part_cluster_table_task = BigQueryInsertJobOperator(
            task_id=f"{color}_part_cluster_table_task",
            configuration={
                "query": {
                    "query": QUERY,
                    "useLegacySql": False,
                }
            },
        )

        bq_external_table_task >> part_cluster_table_task