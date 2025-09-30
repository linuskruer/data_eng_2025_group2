from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow')

from ebay_api_client import EbayAPIClient
from data_manager import DataManager
from weather_correlation_analyzer import WeatherCorrelationAnalyzer

def collect_hourly_data(**context):
    """Stündliche Datensammlung - weniger Daten pro Run"""
    print(f"🕐 Starting HOURLY collection at {datetime.now()}")
    
    try:
        api_client = EbayAPIClient()
        analyzer = WeatherCorrelationAnalyzer(api_client)
        data_manager = DataManager()
        
        # WENIGER Daten pro stündlichem Run (50 statt 200)
        print("🌊 Collecting hourly East Coast data...")
        new_data = analyzer.collect_east_coast_data(items_per_product=50)
        
        if new_data:
            csv_file = data_manager.save_data(new_data, append=True)
            print(f"✅ {len(new_data)} hourly records collected")
            
            # Metadaten für stündliche Sammlung
            context['ti'].xcom_push(key='hourly_records', value=len(new_data))
            context['ti'].xcom_push(key='collection_hour', value=datetime.now().hour)
            
            return len(new_data)
        else:
            print("❌ No new hourly data collected.")
            return 0
            
    except Exception as e:
        print(f"❌ Error in hourly collection: {e}")
        return 0

def hourly_summary(**context):
    """Stündliche Zusammenfassung"""
    ti = context['ti']
    record_count = ti.xcom_pull(key='hourly_records', task_ids='collect_hourly_data')
    collection_hour = ti.xcom_pull(key='collection_hour', task_ids='collect_hourly_data')
    
    print(f"\n📊 HOURLY SUMMARY - Hour {collection_hour}:00")
    print(f"📈 Records collected: {record_count or 0}")
    print(f"🕒 Completed at: {datetime.now().strftime('%H:%M:%S')}")
    
    return record_count or 0

# STÜNDLICHER DAG
default_args = {
    'owner': 'ebay-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ebay_hourly_data_collection',
    default_args=default_args,
    description='HOURLY collection of eBay product data from East Coast',
    schedule_interval='0 * * * *',  # ⬅️ STÜNDLICH: Jede volle Stunde
    catchup=False,
    tags=['ebay', 'hourly', 'data-collection'],
    max_active_runs=1
) as dag:
    
    collect_hourly = PythonOperator(
        task_id='collect_hourly_data',
        python_callable=collect_hourly_data,
        provide_context=True
    )
    
    summary = PythonOperator(
        task_id='hourly_summary',
        python_callable=hourly_summary,
        provide_context=True
    )
    
    collect_hourly >> summary