from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import sys
import os
sys.path.append("/opt/airflow/scripts")
from api_currency import fetch_exchange_rates
from db_scripts import create_raw_table, get_lastest_raw_data, insert_processed_data, insert_raw_data
from transform_data import process_raw_exchange_rates

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

# create_table_task = PythonOperator(
#     task_id='create_raw_table',
#     python_callable=create_raw_table,
#     dag=dag
# )


# fetch_task = PythonOperator(
#     task_id='fetch_exchange_rates',
#     python_callable=fetch_exchange_rates,
#     dag=dag
# )

# get_lastest_raw_data_task = PythonOperator(
#     task_id = 'get_latest_raw_data',
#     python_calleable=get_lastest_raw_data,
#     dag=dag
# )

# transform_task = PythonOperator(
#     task_id='process_raw_data',
#     python_callable=process_raw_exchange_rates,
#     dag=dag
# )

# insert_processed_task = PythonOperator(
#     task_id='insert_processed_data',
#     python_callable=insert_processed_data,
#     dag=dag
# )


@task
def fetch_data():
    raw_data = fetch_exchange_rates()
    insert_raw_data(raw_data)


@task
def get_last_raw_data():
    return get_lastest_raw_data()
    

@task
def transform_data(raw_data):
    return process_raw_exchange_rates(raw_data)


@task
def load_processed_data(transformed_data):
    insert_processed_data(transformed_data)


with dag:
    raw_data = fetch_data() >> get_last_raw_data()
    transformed = transform_data(raw_data)
    load_processed_data(transformed)