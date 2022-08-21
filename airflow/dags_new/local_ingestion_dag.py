import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

with DAG(
        'Download Parquet + List Directories',
        description='A simple tutorial DAG to showcase Airflow basics',
        schedule_interval='0 6 2 * *',
        start_date=datetime(2022, 1, 1),
) as dag:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sS {url} > {AIRFLOW_HOME}/output.parquet'
    )

    ingest_task = BashOperator(
        task_id='ingest',
        bash_command=f'ls -al {AIRFLOW_HOME}'
    )

    wget_task >> ingest_task
