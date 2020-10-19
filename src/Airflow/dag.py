from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'catch_up':False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG('network', default_args=default_args)

scraper = BashOperator(
    task_id = 'spark_process',
    bash_command = 'sh spark_run.sh' ,
    dag = dag
)