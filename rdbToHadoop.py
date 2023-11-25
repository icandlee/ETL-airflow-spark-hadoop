from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from plugins import slack
from airflow.utils.dates import days_ago
import psycopg2
import pandas as pd
import requests
import os 

DAG_ID = "hadoop_operator_dag" 

default_args = {
    'owner': 'airflow',
    'schedule_interval': '@hourly',
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

save_file_name = "hello.csv"

# Function to extract data from RDB
def extract_from_rdb():

    # Connect to the RDB and extract data
    # Assuming PostgreSQL as the RDB in this example
    conn = psycopg2.connect(database="shop", user="airflow", password="airflow", host="postgres")
    query = "SELECT * FROM product"
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Save the extracted data to a CSV file
    df.to_csv(save_file_name, index=False) 


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
        


get_products = PythonOperator(
    task_id="get_product",
    python_callable = extract_from_rdb,
    dag = dag,
)

upload_data = PythonOperator(
    task_id="upload_data",
    python_callable = upload_to_hdfs,
    dag = dag,
)



get_products >> upload_data
if __name__ == "__main__":
    dag.test()