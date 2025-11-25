from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"start_date": datetime(2024, 1, 1)}

with DAG("nfl_etl",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False):
    
    ingest = BashOperator(
        task_id="fetch_nfl_data",
        bash_command="python /opt/airflow/dags/scripts/fetch_data.py"
    )