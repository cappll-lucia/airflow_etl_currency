import requests
import os
import sys
sys.path.append("/opt/airflow/scripts")
from db_scripts import fetch_raw_data
import psycopg2
from psycopg2.extras import DictCursor

API_URL = os.getenv("CURRENCY_API_URL")
API_KEY = os.getenv("CURRENCY_API_KEY")

# TODO imporove trycatch
def fetch_exchange_rates():
    """ 
        get current exchange rates from API and save response to PostgreSQL
    """
    url=f"{API_URL}/live"
    headers={"apikey": API_KEY}

    response = requests.get(url, headers)

    if response.status_code==200:
        data = response.json()
        fetch_raw_data(data)
        return data
    else: 
        raise Exception(f"API Error: {response.status_code} / {response.text} ")

