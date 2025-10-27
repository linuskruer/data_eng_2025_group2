from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG defaults
DEFAULT_ARGS = {
    "owner": "data-eng-group2",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Run at minute 5 every hour
with DAG(
    dag_id="ebay_ingest_to_clickhouse_hourly",
    description="Fetch eBay listings and insert into ClickHouse bronze.ebay_raw_data",
    start_date=datetime(2025, 10, 1),
    schedule_interval="5 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ebay", "bronze", "clickhouse"],
) as dag:

    # Airflow container already has the script at /opt/airflow/scripts
    run_ebay_ingest = BashOperator(
        task_id="run_ebay_ingest",
        bash_command=(
            "python /opt/airflow/scripts/all_in_one.py "
            "--items-per-product {{ var.value.get('EBAY_ITEMS_PER_PRODUCT', 200) }}"
        ),
        env={
            # eBay creds come from Kubernetes Secret as pod env vars
            "EBAY_CLIENT_ID": "{{ env.get('EBAY_CLIENT_ID') }}",
            "EBAY_CLIENT_SECRET": "{{ env.get('EBAY_CLIENT_SECRET') }}",

            # ClickHouse connectivity (local/VM where CH runs)
            "CLICKHOUSE_HOST": "{{ env.get('CLICKHOUSE_HOST', 'localhost') }}",
            "CLICKHOUSE_PORT": "{{ env.get('CLICKHOUSE_PORT', '9000') }}",
            "CLICKHOUSE_USER": "{{ env.get('CLICKHOUSE_USER', 'default') }}",
            "CLICKHOUSE_PASSWORD": "{{ env.get('CLICKHOUSE_PASSWORD', '') }}",

            # Bronze table name (optional override)
            "CH_BRONZE_TABLE": "{{ env.get('CH_BRONZE_TABLE', 'bronze.ebay_raw_data') }}",
        },
    )

    run_ebay_ingest
