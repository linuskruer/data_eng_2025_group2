from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from utils.weather_api import fetch_weather_all_cities, validate_weather_data # import your function
from utils.clickhouse_loader import load_weather_to_clickhouse

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
            "file_version": "v1",  # can also come from upstream
            "execution_date": "{{ ds }}"  # Airflow macro
        },
    )
    validate_weather = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )

    load_to_clickhouse = PythonOperator(
        task_id="load_weather_to_clickhouse",
        python_callable=load_weather_to_clickhouse,
        op_kwargs={
            "file_version": "v1",  # can also come from upstream
            "execution_date": "{{ ds }}"  # Airflow macro
        }
    )


    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models silver+"
    )

    # --- Gold layer dbt ---
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models gold+"
    )

    # --- Run dbt tests ---
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/airflow/dags/dbt && dbt test --models gold+"
    )

    fetch_weather >> validate_weather >> load_to_clickhouse >> run_dbt_silver >> run_dbt_gold >> run_dbt_tests
