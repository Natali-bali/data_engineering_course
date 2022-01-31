import os

from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from functions import format_to_parquet # args: src_file
from functions import upload_to_gcs # args: bucket_name, object_name, local_file



PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_TEMPLATE = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_TEMPLATE_PARQUET = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}'
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    'start_date': datetime(2021, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id = "DataIngestionGcsDag",
    schedule_interval = "0 6 2 * *",
    default_args = default_args,
) as dag:
    download_task = BashOperator(
        task_id = 'download_csv',
        bash_command = f'curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_TEMPLATE}'
    )
    format_to_parquet_task = PythonOperator(
        task_id = 'format_to_parquet',
        python_callable = format_to_parquet,
        op_kwargs = {
            'src_file': f'{AIRFLOW_HOME}/{OUTPUT_TEMPLATE}'
            }
    )
    upload_to_gcs_task = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable = upload_to_gcs,
        # args: bucket_name, object_name, local_file
        op_kwargs = {'bucket_name': GCP_GCS_BUCKET,
                    'object_name': f'raw/{OUTPUT_TEMPLATE_PARQUET}',
                    'local_file': f'{AIRFLOW_HOME}/{OUTPUT_TEMPLATE_PARQUET}'}
    )

    download_task >> format_to_parquet_task >> upload_to_gcs_task








    # To check downloaded output.csv file:
    # >> docker ps
    # >> docker exec -it <airflow worker container id> bash
    # >> bash commands to see the file: more output.csv