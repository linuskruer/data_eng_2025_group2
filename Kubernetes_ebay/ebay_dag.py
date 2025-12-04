from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-eng-group2",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ebay_ingest_to_clickhouse_hourly",
    description="Fetch eBay listings and insert into ClickHouse bronze.ebay_raw_data",
    start_date=datetime(2025, 10, 1),
    schedule_interval="5 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ebay", "bronze", "clickhouse"],
) as dag:

    run_ebay_ingest = BashOperator(
        task_id="run_ebay_ingest",
        bash_command=(
            # 👇 Verification step before script runs
            "echo '🔍 Checking environment vars...' && "
            "echo 'EBAY_CLIENT_ID:' $EBAY_CLIENT_ID && "
            "echo 'EBAY_CLIENT_SECRET:' ${EBAY_CLIENT_SECRET:0:4}**** && "
            "echo 'CLICKHOUSE_HOST:' $CLICKHOUSE_HOST && "
            "echo 'CLICKHOUSE_USER:' $CLICKHOUSE_USER && "
            "echo 'CH_BRONZE_TABLE:' $CH_BRONZE_TABLE && "
            # 👇 Then run the actual ingestion
            "python /opt/airflow/scripts/all_in_one.py "
            f"--items-per-product {os.environ.get('EBAY_ITEMS_PER_PRODUCT', '200')}"
        ),
        env={
            "EBAY_CLIENT_ID": os.environ.get("EBAY_CLIENT_ID", ""),
            "EBAY_CLIENT_SECRET": os.environ.get("EBAY_CLIENT_SECRET", ""),
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "localhost"),
            "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "9000"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
            "CH_BRONZE_TABLE": os.environ.get("CH_BRONZE_TABLE", "bronze.ebay_raw_data"),
        },
    )

    run_ebay_ingest
