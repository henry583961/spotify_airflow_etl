from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 19),
    'email': ['henry583961@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG(
    dag_id = "spotify_etl",
    default_args = default_args,
    description = "spotify ETL"
)

run_spotify_etl = PythonOperator(
    task_id = "complete_spotify_etl",
    python_callable = run_spotify_etl,
    dag = dag
)

run_spotify_etl