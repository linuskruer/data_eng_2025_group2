from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta
import pandas as pd
import clickhouse_connect
import os
import json

# ClickHouse connection parameters
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

def load_ebay_data_to_clickhouse(**context):
    """Load eBay data from CSV to ClickHouse Bronze layer"""
    
    # Get the data file from previous task
    ti = context['ti']
    filename = ti.xcom_pull(key='ebay_data_file', task_ids='collect_ebay_data')
    
    if not filename:
        print("❌ No data file found")
        return False
    
    # Read CSV data
    csv_path = f"/opt/airflow/data/{filename}"
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return False
    
    try:
        # Read the JSON file (from daily collection) or CSV file
        if filename.endswith('.json'):
            with open(csv_path, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data['items'])
        else:
            df = pd.read_csv(csv_path)
        
        print(f"📊 Loaded {len(df)} records from {filename}")
        
        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        
        # Prepare data for ClickHouse
        df['execution_date'] = context['execution_date'].date()
        df['data_source'] = 'ebay_api'
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Insert data into ClickHouse
        client.insert_df('bronze.ebay_raw_data', df)
        
        print(f"✅ Successfully loaded {len(df)} records to ClickHouse")
        
        # Log data quality metrics
        quality_log = {
            'log_timestamp': datetime.now(),
            'table_name': 'bronze.ebay_raw_data',
            'check_type': 'record_count',
            'check_result': 'success',
            'error_count': 0,
            'total_count': len(df),
            'details': f'Loaded from {filename}',
            'execution_id': context['dag_run'].run_id
        }
        
        client.insert('bronze.data_quality_logs', [quality_log])
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading data to ClickHouse: {e}")
        
        # Log error
        try:
            error_log = {
                'log_timestamp': datetime.now(),
                'table_name': 'bronze.ebay_raw_data',
                'check_type': 'load_error',
                'check_result': 'failed',
                'error_count': 1,
                'total_count': 0,
                'details': str(e),
                'execution_id': context['dag_run'].run_id
            }
            client.insert('bronze.data_quality_logs', [error_log])
        except:
            pass
        
        return False

def load_weather_data_to_clickhouse(**context):
    """Load weather data from CSV to ClickHouse Bronze layer"""
    
    weather_file = "/opt/airflow/weather_data/new_york_city_hourly_weather_data.csv"
    
    if not os.path.exists(weather_file):
        print(f"❌ Weather file not found: {weather_file}")
        return False
    
    try:
        # Read weather CSV
        df = pd.read_csv(weather_file, skiprows=3)  # Skip header rows
        
        print(f"🌤️ Loaded {len(df)} weather records")
        
        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        
        # Prepare data for ClickHouse
        df['data_source'] = 'open_meteo_api'
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Insert data into ClickHouse
        client.insert_df('bronze.weather_raw_data', df)
        
        print(f"✅ Successfully loaded {len(df)} weather records to ClickHouse")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading weather data to ClickHouse: {e}")
        return False

# DAG definition
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'clickhouse_data_loader',
    default_args=default_args,
    description='Load data from Airflow to ClickHouse Bronze layer',
    schedule_interval='0 7 * * *',  # Daily at 7 AM (after data collection)
    catchup=False,
    tags=['clickhouse', 'data-loading', 'bronze-layer'],
    max_active_runs=1
)

# Tasks
start = DummyOperator(
    task_id='start',
    dag=dag
)

load_ebay_data = PythonOperator(
    task_id='load_ebay_data_to_clickhouse',
    python_callable=load_ebay_data_to_clickhouse,
    provide_context=True,
    dag=dag
)

load_weather_data = PythonOperator(
    task_id='load_weather_data_to_clickhouse',
    python_callable=load_weather_data_to_clickhouse,
    provide_context=True,
    dag=dag
)

validate_bronze_data = ClickHouseOperator(
    task_id='validate_bronze_data',
    sql="""
    SELECT 
        'ebay_raw_data' as table_name,
        COUNT(*) as record_count,
        MIN(collection_timestamp) as earliest_record,
        MAX(collection_timestamp) as latest_record
    FROM bronze.ebay_raw_data
    WHERE toDate(collection_timestamp) = today()
    
    UNION ALL
    
    SELECT 
        'weather_raw_data' as table_name,
        COUNT(*) as record_count,
        MIN(time) as earliest_record,
        MAX(time) as latest_record
    FROM bronze.weather_raw_data
    WHERE toDate(time) = today()
    """,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Task dependencies
start >> [load_ebay_data, load_weather_data] >> validate_bronze_data >> end
