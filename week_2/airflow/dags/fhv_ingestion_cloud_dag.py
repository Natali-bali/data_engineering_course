import os

from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from functions import format_to_parquet # args: src_file
from functions import upload_to_gcs # args: bucket_name, object_name, local_file

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_TEMPLATE = 'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_TEMPLATE = 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_TEMPLATE_PARQUET = 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
TABLE_NAME_TEMPLATE = 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}'
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2020, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id = "FhvIngestionGcsDag",
    schedule_interval = "0 6 2 * *",
    default_args = default_args,
    catchup=True,
    max_active_runs=2
) as dag:
    download_task = BashOperator(
        task_id = 'download_csv_fhv',
        bash_command = f'curl -sSLf {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_TEMPLATE}'
    )
    format_to_parquet_task = PythonOperator(
        task_id = 'format_to_parquet_fhv',
        python_callable = format_to_parquet,
        op_kwargs = {
            'src_file': f'{AIRFLOW_HOME}/{OUTPUT_TEMPLATE}'
            }
    )
    upload_to_gcs_task = PythonOperator(
        task_id = 'upload_to_gcs_fhv',
        python_callable = upload_to_gcs,
        # args: bucket_name, object_name, local_file
        op_kwargs = {'bucket_name': GCP_GCS_BUCKET,
                    'object_name': f'raw/{OUTPUT_TEMPLATE_PARQUET}',
                    'local_file': f'{AIRFLOW_HOME}/{OUTPUT_TEMPLATE_PARQUET}'}
    )
    clean_memory_task = BashOperator(
        task_id = 'clean_memory',
        bash_command = f'rm {AIRFLOW_HOME}/{OUTPUT_TEMPLATE} {AIRFLOW_HOME}/{OUTPUT_TEMPLATE_PARQUET}'
    )
    # upload_to_bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="upload_to_bigquery_external_table",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{GCP_GCS_BUCKET}/raw/{OUTPUT_TEMPLATE_PARQUET}"],
    #         },
    #     },
    # )
    download_task >> format_to_parquet_task >> upload_to_gcs_task >> clean_memory_task# >> upload_to_bigquery_external_table_task

