from plugins import slack
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow import DAG
import os 
from airflow.operators.python_operator import PythonOperator

DAG_ID = "proc_spark_dag" 

default_args = {
    'owner': 'airflow',
    'schedule_interval': '@daily',
    'start_date': datetime(2023, 9, 28),
    'tags': ['shop'],
    # 'retries': 1,
    'on_failure_callback': slack.on_failure_callback, # 실패시 SLACK 함수 요청
    'on_success_callback': slack.on_success_callback, # 실패시 SLACK 함수 요청
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args, 
    catchup=False,
    schedule_interval = '0 9 * * *')

def print_error(**kwargs):

    try:
        # Get absolute path to the Spark job file
        current_dir = os.path.dirname(__file__)
        spark_job_file = os.path.join(current_dir, 'job.py') 
        print(spark_job_file)

        spark_task = SparkSubmitOperator(
            task_id='spark_job',
            conn_id='spark_default',  # connection ID for your Spark Cluster
            application=spark_job_file,  # replace with the path with your pysaprk script 
            conf={
            'spark.driver.extraClassPath': '/usr/share/java/mysql-connector-java-8.2.0.jar'
        },
            dag=dag
    )
        
        task = spark_task.execute(kwargs)
        return task


    except Exception as e:
        # 발생한 예외 정보를 출력
        print(f"Error occurred: {str(e)}")
        raise

# print_error 함수를 SparkSubmitOperator로 호출하여 실행
run_spark_job = PythonOperator(
    task_id='run_spark_job',
    python_callable=print_error,
    provide_context=True,  # 컨텍스트 정보를 제공하기 위해 True로 설정
    dag=dag
)
 
run_spark_job 

if __name__ == "__main__":
    dag.test()