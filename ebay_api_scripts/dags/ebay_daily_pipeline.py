from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os
import time
import json
from datetime import datetime

# Add path to import our ebay client
sys.path.append('/opt/airflow')

from ebay_api_client import EbayAPIClient

def collect_ebay_data(**context):
    """Collect eBay data for all product categories"""
    
    print("🚀 Starting eBay data collection...")
    
    client = EbayAPIClient()
    
    product_categories = {
        'rain_products': ['umbrella', 'rain jacket'],
        'heat_products': ['air conditioner', 'sunscreen'],
        'cold_products': ['winter coat', 'thermal gloves'],
        'seasonal_products': ['beach towel', 'snow shovel', 'outdoor furniture']
    }
    
    all_results = []
    execution_date = context['execution_date']
    
    for category, products in product_categories.items():
        print(f"\n🔍 Searching {category}...")
        
        for product in products:
            print(f"   📦 Searching: {product}")
            
            try:
                # Use your East Coast search method
                results = client.search_east_coast_only(product, limit=30)
                
                if results and 'itemSummaries' in results:
                    # Add metadata to each item
                    for item in results['itemSummaries']:
                        item['metadata'] = {
                            'product_category': category,
                            'search_term': product,
                            'collected_at': datetime.now().isoformat(),
                            'execution_date': execution_date.isoformat(),
                            'data_source': 'ebay_api'
                        }
                    
                    all_results.extend(results['itemSummaries'])
                    print(f"   ✅ Found {len(results['itemSummaries'])} items")
                else:
                    print(f"   ⚠️  No results for {product}")
                
                # Rate limiting
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error searching {product}: {e}")
                continue
    
    # Save results
    if all_results:
        filename = save_results(all_results, execution_date)
        print(f"🎯 Total items collected: {len(all_results)}")
        
        # Push filename to XCom for downstream tasks
        context['ti'].xcom_push(key='ebay_data_file', value=filename)
        return len(all_results)
    else:
        print("❌ No data collected in this run")
        return 0

def save_results(results, execution_date):
    """Save results to JSON file with timestamp"""
    
    # Create filename with date
    date_str = execution_date.strftime('%Y%m%d')
    filename = f"ebay_data_{date_str}.json"
    filepath = f"/opt/airflow/data/{filename}"
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Save with pretty printing
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'collected_at': datetime.now().isoformat(),
                'execution_date': execution_date.isoformat(),
                'total_items': len(results),
                'version': '1.0'
            },
            'items': results
        }, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Data saved to {filename}")
    return filename

def validate_data(**context):
    """Basic data validation"""
    ti = context['ti']
    filename = ti.xcom_pull(key='ebay_data_file', task_ids='collect_ebay_data')
    
    if not filename:
        print("❌ No data file found")
        return False
    
    filepath = f"/opt/airflow/data/{filename}"
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        item_count = data['metadata']['total_items']
        print(f"✅ Data validation passed: {item_count} items in {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Data validation failed: {e}")
        return False

# DAG definition
default_args = {
    'owner': 'ebay-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

dag = DAG(
    'ebay_daily_data_collection',
    default_args=default_args,
    description='Daily collection of eBay product data from East Coast',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['ebay', 'data-collection', 'production'],
    max_active_runs=1
)

# Tasks
start = DummyOperator(
    task_id='start',
    dag=dag
)

collect_data = PythonOperator(
    task_id='collect_ebay_data',
    python_callable=collect_ebay_data,
    provide_context=True,
    dag=dag
)

validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Task dependencies
start >> collect_data >> validate >> end