import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from ingestion_script import ingest_callable

url_head = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
url_template = url_head + '/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
output_file = AIRFLOW_HOME+'/output_{{execution_date.strftime(\'%Y-%m\')}}.parquet'

PG_USERNAME = os.environ.get(PG_USERNAME)
PG_PASSWORD =os.environ.get(PG_PASSWORD)
PG_HOST =os.environ.get(PG_HOST)
PG_PORT =os.environ.get(PG_PORT)
PG_DATABASE =os.environ.get(PG_DATABASE)

with DAG(
        'Ingestion',
        description='A simple tutorial DAG to showcase Airflow basics',
        schedule_interval='0 6 2 * *',
        start_date=datetime(2021, 1, 1),
) as dag:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sS {url_template} > {output_file}'
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs={
            "user": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    wget_task >> ingest_task
