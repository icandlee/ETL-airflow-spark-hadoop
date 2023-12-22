# DataPiepeline : Airflow DAG

이 프로젝트는 Apache Airflow를 사용하여 MariaDB에서 데이터를 추출하고 이를 Hadoop HDFS에 CSV 파일로 저장하는 DAG를 구축합니다.

## :bookmark_tabs: Contents

- [Table of Contents](#table-of-contents)
- [About](#about)
- [Features](#Features)
- [Prerequisites](#Prerequisites)
- [Installation](#installation)

## About
프로젝트 파이프라인 구조 이미지
```bash
dags
├── plugins
│   └── slack.py
└── rdbToHadopp.py
```

## Features
- MariaDB 데이터 조회 및 Spark로 전처리
- 가공된 데이터 CSV로 저장 및 Hadoop에 적재
- Airflow로 매일 1회 

## Prerequisites
- Python 3.9
- MariaDB 11.1.3 
- Apache Airflow
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
