import os
from airflow import DAG
# from airflow.utils.dates import days_ago  -- another way to create start_date with days_ago
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# default_args = {
#     "start_date": days_ago(60)}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_TEMPLATE = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'


with DAG(
    dag_id = "LocalIngestionDag",
    schedule_interval = "0 6 2 * *",
    # www.crontab.guru to set up day for scheduler. Now every second day of month at 6 am
    start_date = datetime(2021, 1, 12)
) as local_workflow:
    wget_task = BashOperator(
        task_id = 'wget',
        # bash_command = f'wget {url} -O {AIRFLOW_HOME}/output.csv'
        # in new dockerfile i installed wget, so can run this command
        #  to check first 10 rows in log we can run this script: bash_command = f'curl -sS {url} | head -n 10'
        # bash_command = 'echo "{{execution_date.strftime(\'%Y-%m\')}}"' # Jinja way to execute the date
        bash_command = f'curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_TEMPLATE}'

    )
    ingest_task = BashOperator(
        task_id = 'ingest',
        bash_command = 'echo "Done!"'
    )

    wget_task >> ingest_task








    # To check downloaded output.csv file:
    # >> docker ps
    # >> docker exec -it <airflow worker container id> bash
    # >> bash commands to see the file: more output.csv