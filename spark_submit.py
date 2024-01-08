from plugins import slack
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow import DAG
import os 
from airflow.operators.python_operator import PythonOperator
import requests


DAG_ID = "proc_spark_dag" 

default_args = {
    'owner': 'airflow',
    'schedule_interval': '@daily',
    'start_date': datetime(2023, 9, 28),
    'tags': ['shop'],
    'on_failure_callback': slack.on_failure_callback,
    'on_success_callback': slack.on_success_callback,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args, 
    catchup=False,
    schedule_interval='0 9 * * *'
)

save_file_name = "./spark/file" 


def run_spark_job(**kwargs):
    try:
        # Get absolute path to the Spark job file
        current_dir = os.path.dirname(__file__)
        spark_job_file = os.path.join(current_dir, 'spark', 'job.py') 
        file_path = os.path.join(current_dir, 'spark', 'file') 
 
        spark_task = SparkSubmitOperator(
            task_id='spark_job',
            conn_id='spark_default',  # Replace with your Spark connection ID
            application=spark_job_file,
            conf={
                'spark.driver.extraClassPath': '/usr/share/java/mysql-connector-java-8.2.0.jar'
            },
            dag=dag
        )
        
        task = spark_task.execute(kwargs)
        print(os.listdir(file_path), "파일 저장 후")
        return task

    except Exception as e:
        raise


def upload_to_hdfs():
    httpfs_base_url = "http://httpfs:14000/webhdfs/v1"
    hdfs_destination_path = f"/data/{save_file_name}"
    local_file_path = save_file_name

    # HttpFS operation: Create a file in HDFS
    httpfs_operation = "CREATE"

    # Full URL for the HttpFS request
    url = f"{httpfs_base_url}{hdfs_destination_path}?op={httpfs_operation}&user.name=root"
    headers = {'Content-Type': 'application/octet-stream'}


    # Open the local file and read its contents
    with open(local_file_path, 'rb') as file:
        file_upload = {"file":file}
        # Make the HttpFS request to create the file in HDFS
        response = requests.put(url, headers=headers, files=file_upload )

    # Check if the request was successful
    if response.status_code == 201:
        print("File uploaded to HDFS successfully.")
    else:
        raise Exception(f"HTTP request failed. HTTP response was {response.status_code} : {response}")
        

# PythonOperator로 Spark 작업 실행 함수를 호출하여 DAG에 추가
run_spark_job_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    provide_context=True,
    dag=dag
)

upload_data = PythonOperator(
    task_id="upload_data",
    python_callable = upload_to_hdfs,
    dag = dag,
)

 
run_spark_job_task >> upload_data

if __name__ == "__main__":
    dag.test()