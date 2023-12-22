from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_ID = "test_dag" 

default_args = {
    'owner': 'airflow',
    'schedule_interval': '@hourly',
    'start_date': datetime(2023, 9, 24),
    'tags': ['temp'],
    'retries': 1,
}

dag =  DAG(
    dag_id=DAG_ID,
    default_args=default_args, 
    catchup=False)

#Bash Operator
cmd = 'echo "Hello, Airflow"'
sample_task = BashOperator(
    task_id="sample_task",
    bash_command=cmd,
    dag=dag)
