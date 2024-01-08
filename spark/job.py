from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests 
import os

try :   
    # SparkSession을 생성합니다.
    spark = SparkSession.builder\
        .appName("MySQL to Spark")\
            .getOrCreate()

    # MySQL 연결 정보
    mysql_host = "host.docker.internal"
    mysql_port = "3306"
    mysql_database = "shop"
    mysql_table = "product"
    mysql_user = "airflow"
    mysql_password = "airflow"

    # MySQL 데이터를 Spark DataFrame으로 읽어옵니다.
    jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
    df_mysql = spark.read.format("jdbc").option("url", jdbc_url) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .load()

    df_mysql.show()
    print(df_mysql.show())

    current_dir = os.path.dirname(__file__)
    file_path = spark_job_file = os.path.join(current_dir, 'file') 
    # DataFrame을 CSV 파일로 저장합니다.
    output_path = "file"
    df_mysql.write.csv(file_path , header=True, mode="overwrite")

    # 데이터 전처리 예시: 간단히 컬럼을 선택하여 필터링하고 출력
    #processed_df = df_mysql.select("*").filter(col("column1") > 100)

    # SparkSession을 종료합니다.
    spark.stop()

except Exception as e :
    print(e)