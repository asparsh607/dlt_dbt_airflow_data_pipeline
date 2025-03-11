import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'Asparsh Raj',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email': ['memymyself@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    'dbt_airflow_pipeline',
    default_args=default_args,
    description='A dbt pipeline running every Sunday at 2 PM IST',
    schedule_interval='30 8 * * 0',  # 08:30 UTC = 14:00 IST
    catchup=False
)

BASE_DIR = "/home/asparsh/data_pipeline"

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {BASE_DIR}/dbt_weather_trends_pipeline/ && source ../dev_env/bin/activate && dbt test || exit 1',
    dag=dag
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'cd {BASE_DIR}/dbt_weather_trends_pipeline/ && source ../dev_env/bin/activate && dbt run || exit 1',
    dag=dag
)

dbt_test >> dbt_run
