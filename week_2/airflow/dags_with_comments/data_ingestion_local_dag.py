import os
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_data_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_TEMPLATE = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
TABLE_NAME_TEMPLATE = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}'
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

with DAG(
    dag_id = "LocalIngestionDag",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2021, 1, 1)
) as local_workflow:
    wget_task = BashOperator(
        task_id = 'wget',
        bash_command = f'curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_TEMPLATE}'
    )
    ingest_task = PythonOperator(
        task_id = 'ingest',
        python_callable = ingest_callable,
        # user, password, host, port, db, table_name, csv_name
        op_kwargs = {
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'port': PG_PORT,
            'db': PG_DATABASE,
            'table_name': TABLE_NAME_TEMPLATE,
            'csv_name': OUTPUT_TEMPLATE
        }
    )

    wget_task >> ingest_task








    # To check downloaded output.csv file:
    # >> docker ps
    # >> docker exec -it <airflow worker container id> bash
    # >> bash commands to see the file: more output.csv