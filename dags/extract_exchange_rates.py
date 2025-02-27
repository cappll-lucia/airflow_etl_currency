from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append("/opt/airflow/scripts")
from api_currency import fetch_exchange_rates
from db_scripts import create_raw_table

default_args={
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'extract_exchange_rates',
    default_args=default_args,
    description='extract exchange ranges and save data to PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

create_table_task = PythonOperator(
    task_id='create_raw_table',
    python_callable=create_raw_table,
    dag=dag
)


fetch_task = PythonOperator(
    task_id='fetch_exchange_rates',
    python_callable=fetch_exchange_rates,
    dag=dag
)


create_table_task >> fetch_task