"""
Unified Pipeline DAG - Orchestrates both weather and eBay data pipelines.
Coordinates ingestion, transformation, and testing for both data sources.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.weather_api import fetch_weather_all_cities, validate_weather_data
from utils.clickhouse_loader import load_weather_to_clickhouse
from utils.ebay_ingestion import run_ebay_ingestion

default_args = {
    "owner": "data-eng-group2",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="unified_data_pipeline",
    description="Unified orchestration for weather and eBay data pipelines (Bronze → Silver → Gold)",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",  # Run daily, can trigger eBay separately
    catchup=False,
    default_args=default_args,
    tags=["unified", "weather", "ebay", "medallion", "dbt"],
) as dag:

    # ====================================================================
    # WEATHER DATA PIPELINE
    # ====================================================================
    
    # --- Weather: Fetch data from API ---
    fetch_weather = PythonOperator(
        task_id="fetch_weather_all_cities",
        python_callable=fetch_weather_all_cities,
        op_kwargs={
            "file_version": "v1",
            "execution_date": "{{ ds }}"
        },
    )

    # --- Weather: Validate data quality ---
    validate_weather = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )

    # --- Weather: Load to ClickHouse Bronze ---
    load_weather_to_clickhouse = PythonOperator(
        task_id="load_weather_to_clickhouse",
        python_callable=load_weather_to_clickhouse,
        op_kwargs={
            "file_version": "v1",
            "execution_date": "{{ ds }}"
        }
    )

    # ====================================================================
    # EBAY DATA PIPELINE
    # ====================================================================

    # --- eBay: Ingest data to ClickHouse Bronze ---
    ingest_ebay = PythonOperator(
        task_id="ingest_ebay_to_clickhouse",
        python_callable=run_ebay_ingestion,
        op_kwargs={}
    )

    # ====================================================================
    # SILVER LAYER TRANSFORMATIONS (dbt)
    # ====================================================================

    # --- Silver: Transform weather data ---
    run_dbt_silver_weather = BashOperator(
        task_id="run_dbt_silver_weather",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models silver_weather",
    )

    # --- Silver: Transform eBay data (conditional) ---
    run_dbt_silver_ebay = BashOperator(
        task_id="run_dbt_silver_ebay",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models silver_ebay_listings --vars '{\"enable_ebay_silver\": true}'",
    )

    # ====================================================================
    # GOLD LAYER TRANSFORMATIONS (dbt)
    # ====================================================================

    # --- Gold: Transform weather data ---
    run_dbt_gold_weather = BashOperator(
        task_id="run_dbt_gold_weather",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models gold.fact_weather gold.dim_*",
    )

    # --- Gold: Transform eBay data (conditional) ---
    run_dbt_gold_ebay = BashOperator(
        task_id="run_dbt_gold_ebay",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models gold.fact_listings+ --vars '{\"enable_ebay_silver\": true}'",
    )

    # ====================================================================
    # DATA QUALITY TESTS (dbt)
    # ====================================================================

    # --- Tests: Validate weather gold layer ---
    run_dbt_tests_weather = BashOperator(
        task_id="run_dbt_tests_weather",
        bash_command="cd /opt/airflow/dags/dbt && dbt test --models gold.fact_weather gold.dim_*",
    )

    # --- Tests: Validate eBay gold layer (conditional) ---
    run_dbt_tests_ebay = BashOperator(
        task_id="run_dbt_tests_ebay",
        bash_command="cd /opt/airflow/dags/dbt && dbt test --models gold.fact_listings+ --vars '{\"enable_ebay_silver\": true}'",
    )

    # ====================================================================
    # TASK DEPENDENCIES
    # ====================================================================

    # Weather pipeline: Sequential flow
    weather_pipeline = fetch_weather >> validate_weather >> load_weather_to_clickhouse >> run_dbt_silver_weather >> run_dbt_gold_weather >> run_dbt_tests_weather

    # eBay pipeline: Sequential flow
    ebay_pipeline = ingest_ebay >> run_dbt_silver_ebay >> run_dbt_gold_ebay >> run_dbt_tests_ebay

    # Both pipelines can run in parallel
    # Note: eBay pipeline may skip if no data exists (handled by enable_ebay_silver var)

