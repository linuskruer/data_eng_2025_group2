from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.weather_api import fetch_weather_all_cities, validate_weather_data # import your function

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="weather_data_ingestion",
    start_date=datetime(2025, 10, 25),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "weather"],
) as dag:

    fetch_weather = PythonOperator(
        task_id="fetch_weather_all_cities",
        python_callable=fetch_weather_all_cities,
        op_kwargs={
        "execution_date": "{{ ds }}",
        "file_version": "v1"
        },
    )
    validate_weather = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )

    fetch_weather >> validate_weather
