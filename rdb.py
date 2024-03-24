from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins import slack
import pendulum
# from airflow.operators.hadoop_operator import HadoopOperator

DAG_ID = "postgres_operator_dag" 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 28),
    'tags': ['shop'],
    # 'retries': 1,
    'on_failure_callback': slack.on_failure_callback, # 실패시 SLACK 함수 요청
    'on_success_callback': slack.on_success_callback, # 실패시 SLACK 함수 요청
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args, 
    catchup=False,
    schedule_interval = '41 14 * * *', 
) as dag:
    get_products = PostgresOperator(
        postgres_conn_id="postgres_shop",
        task_id="get_product",
        sql="SELECT * FROM product",
        # postgres_conn_id="postgres_default",
        # parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        # hook_params={"options": "-c statement_timeout=3000ms"},
    )

    # save_data_to_hadoop = HadoopOperator(
    #     task_id="save_data_to_hadoop",
    #     command="hadoop fs -put /path/to/my/data /path/to/hadoop/file",
    #     hadoop_conn_id="my_hadoop_connection",
    #     dag=dag,
    # )

    #create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
get_products

if __name__ == "__main__":
    dag.test()