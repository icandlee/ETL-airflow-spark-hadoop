# DataPiepeline : Airflow DAG

이 프로젝트는 Apache Airflow를 사용하여 MariaDB에서 데이터를 추출하고 이를 Hadoop HDFS에 CSV 파일로 저장하는 DAG를 구축합니다.

## :bookmark_tabs: Contents

- [Table of Contents](#table-of-contents)
- [About](#about)
- [Features](#Features)
- [Prerequisites](#Prerequisites)
- [Installation](#installation)

## About
<img width="812" alt="스크린샷 2023-12-25 오후 6 09 08" src="https://github.com/hyunsoo2936/Data-engineering-shop/assets/69141658/d3c08147-f821-4775-84cf-fd769b8fc148">

```bash
dags
├── plugins
│   └── slack.py
│   └── teams.py
│   rdbToHadopp.py
└── spark_submit.py
```

## Features
- MariaDB 데이터 조회 및 Spark로 전처리
- 가공된 데이터 CSV로 저장 및 Hadoop에 적재
- Airflow로 매일 1회 실행

## Prerequisites
- Python 3.8
- MariaDB 11.1.3 
- Apache Airflow 2.8.0
- Spark
- Hadoop, HttpFS 

## Installation

Provide instructions on how to install or set up your project. Include any dependencies or prerequisites needed.

Install airflow usiing docker-compose
```bash
$ mkdir airflow
$ cd airflow
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'
$ mkdir -p ./dags ./logs ./plugins
$ echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Install DB Connect module 
```bash
$ pip install pymysql  
```
