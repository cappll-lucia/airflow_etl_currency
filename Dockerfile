FROM apache/airflow:2.7.2

ENV PYTHONPATH="/opt/airflow/scripts:${PYTHONPATH}"

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

USER airflow

COPY dags /opt/airflow/dags/
COPY scripts /opt/airflow/scripts/
