from plugins import slack
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os


os.environ['SPARK_CLASSPATH'] = '/usr/share/java/mysql-connector-java-8.2.0.jar' 
os.environ['PYSPARK_SUBMIT_ARGS'] = '/usr/share/java/mysql-connector-java-8.2.0.jar' 

DAG_ID = "spark_bash_dag" 

default_args = {
    'owner': 'airflow',
    'schedule_interval': '@daily',
    'start_date': datetime(2023, 9, 28),
    'tags': ['shop'],
    # 'retries': 1,
    'on_failure_callback': slack.on_failure_callback, # 실패시 SLACK 함수 요청
    'on_success_callback': slack.on_success_callback, # 실패시 SLACK 함수 요청
}


dag = DAG(DAG_ID,     
        default_args=default_args, 
        catchup=False,
        schedule_interval = '0 9 * * *')

# Spark job 실행을 위한 BashOperator
spark_submit_command = "spark-submit --master spark://f36d8aff7140:7077 --name arrow-spark /opt/***/dags/job.py"
run_spark_job = BashOperator(
    task_id='run_spark_bash_job',
    bash_command=spark_submit_command,
    dag=dag,
)

run_spark_job 

if __name__ == "__main__":
    dag.test()