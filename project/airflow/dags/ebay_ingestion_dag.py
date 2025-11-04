"""
eBay data ingestion DAG - runs independently to collect eBay listings,
insert them into ClickHouse bronze.ebay_raw_data, and transform through
dbt silver/gold layers.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.ebay_ingestion import run_ebay_ingestion

default_args = {
    "owner": "data-eng-group2",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ebay_ingestion",
    description="Fetch eBay listings, insert into ClickHouse bronze, and run dbt transformations",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@hourly",  # Run every hour
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "ebay", "bronze", "dbt"],
) as dag:

    # --- Bronze: Ingest eBay data ---
    ingest_ebay = PythonOperator(
        task_id="ingest_ebay_to_clickhouse",
        python_callable=run_ebay_ingestion,
        op_kwargs={}
    )

    # --- Silver: Transform eBay data ---
    run_dbt_silver_ebay = BashOperator(
        task_id="run_dbt_silver_ebay",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models silver_ebay_listings --vars '{\"enable_ebay_silver\": true}'",
    )

    # --- Gold: Create dimensional model ---
    run_dbt_gold_ebay = BashOperator(
        task_id="run_dbt_gold_ebay",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --models gold.fact_listings+ --vars '{\"enable_ebay_silver\": true}'",
    )

    # --- Tests: Validate gold layer ---
    run_dbt_tests_ebay = BashOperator(
        task_id="run_dbt_tests_ebay",
        bash_command="cd /opt/airflow/dags/dbt && dbt test --models gold.fact_listings+ --vars '{\"enable_ebay_silver\": true}'",
    )

    # Define task dependencies
    ingest_ebay >> run_dbt_silver_ebay >> run_dbt_gold_ebay >> run_dbt_tests_ebay

