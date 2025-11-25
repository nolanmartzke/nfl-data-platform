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
        bash_command="python /opt/airflow/src/ingestion/fetch_nfl_stats.py"
    )

    transform = BashOperator(
        task_id="clean_nfl_data",
        bash_command="python /opt/airflow/src/transformations/clean_nfl_data.py"
    )

    load = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/src/loading/load_to_postgres.py"
    )

    ingest >> transform >> load