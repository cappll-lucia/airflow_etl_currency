import psycopg2
import os
import json

DB_HOST = 'postgres'
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def create_raw_table():
    """
        create table raw_exchange_rate in PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_exchange_rates (
                id SERIAL PRIMARY KEY,
                res_json JSONB NOT NULL,
                retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table raw_data_exchange created in PostgreSQL")
    except Exception as e:
        print(f"Error on raw data table creation: {e}")


def fetch_raw_data(data):
    """
        Save API response into raw_exchange_rates Postgres table
    """ 
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        cursor = conn.cursor()
        cursor.execute("INSERT INTO raw_exchange_rates (res_json) VALUES (%s)", [json.dumps(data)])
        conn.commit()
        cursor.close()
        conn.close()
        print("API response saved to raw table")
    except Exception as e:
        print(f"Error on raw data insert: {e}")