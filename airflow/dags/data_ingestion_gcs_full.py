import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "ben",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}
# Upload to gcs
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# DAG
def download_parquet_upload_dag(dag, url_template, local_parquet_template,gcs_path_template):
    with dag:
        # Download
        download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {url_template} > {local_parquet_template}"
    )

        # Upload to gcs
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_template,
            },
        )

        # Remove local file
        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_parquet_template}"
        )
        download_dataset_task >> local_to_gcs_task >> rm_task

# GREEN
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-12.parquet
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_FILE_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_PATH_GCS_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

green_taxi_data = DAG(
    dag_id="green_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022,  1, 1),
    end_date=datetime(2022,  12, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-green'],
)

download_parquet_upload_dag(
    dag = green_taxi_data,
    url_template = YELLOW_TAXI_URL_TEMPLATE, 
    local_parquet_template = YELLOW_TAXI_FILE_TEMPLATE,
    gcs_path_template= YELLOW_TAXI_PATH_GCS_TEMPLATE)

# YELLOW
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_PATH_GCS_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_data = DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022,  1, 1),
    end_date=datetime(2022,  12, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-yellow'],
)

download_parquet_upload_dag(
    dag = yellow_taxi_data,
    url_template = YELLOW_TAXI_URL_TEMPLATE, 
    local_parquet_template = YELLOW_TAXI_FILE_TEMPLATE,
    gcs_path_template= YELLOW_TAXI_PATH_GCS_TEMPLATE)

# FHV
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
FHV_URL_TEMPLATE = URL_PREFIX + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_PATH_GCS_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_data = DAG(
    dag_id="fhv_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022,  1, 1),
    end_date=datetime(2022,  12, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-fhv'],
)

download_parquet_upload_dag(
    dag = fhv_data,
    url_template = FHV_URL_TEMPLATE, 
    local_parquet_template = FHV_FILE_TEMPLATE,
    gcs_path_template= FHV_PATH_GCS_TEMPLATE)

# Zone
# https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
