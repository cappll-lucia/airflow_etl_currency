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


def create_processed_table():
    """
        create table processed_exchange_rates in PostgreSQL
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
            CREATE TABLE processed_exchange_rates (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                base_currency VARCHAR(3) NOT NULL,
                target_currency VARCHAR(3) NOT NULL,
                rate NUMERIC(10, 6), 
                start_rate NUMERIC(10, 6),
                end_rate NUMERIC(10, 6),
                change NUMERIC(10, 6),
                change_pct NUMERIC(5, 2),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, base_currency, target_currency)
            );   
        """)
        conn.commit()
        conn.close()
        print("Table processed_data_exchange created in PostgreSQL")
    except Exception as e:
        print(f"Error on processed data table creation: {e}")


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


def get_lastest_raw_data():
    """
        read from raw_exchange_rates table
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
            SELECT res_json, retrieved_at
            FROM raw_exchange_rates
            WHERE retrieved_at > (SELECT COALESCE(MAX(processed_at), '2002-01-19') FROM processed_exchange_rates)
            ORDERED BY retrieved_at ASC;
        """)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(f"Error on raw data select: {e}")