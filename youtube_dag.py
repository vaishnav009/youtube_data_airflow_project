from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from youtube_etl import get_comment_threads


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,12,16),
    'email': ['example@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Youtube_API_Dag',
    default_args=default_args,
    description='Dag for orchestrating youtube data etl.'
)

etl_operation = PythonOperator(
    task_id='Python-Task',
    python_callable=get_comment_threads,
    dag=dag
)

etl_operation