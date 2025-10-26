#!/usr/bin/env python3
"""
Data Loading Script for ClickHouse Medallion Architecture
Loads existing eBay and weather data into Bronze layer
"""

import pandas as pd
import clickhouse_connect
import json
import os
from datetime import datetime
from pathlib import Path

# ClickHouse connection parameters
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

def connect_to_clickhouse():
    """Connect to ClickHouse"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        print("✅ Connected to ClickHouse successfully")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return None

def load_ebay_data(client):
    """Load eBay data from CSV to ClickHouse"""
    ebay_file = Path("../ebay_data/east_coast_weather_data.csv")
    
    if not ebay_file.exists():
        print(f"❌ eBay data file not found: {ebay_file}")
        return False
    
    try:
        # Read CSV data
        df = pd.read_csv(ebay_file)
        print(f"📊 Loaded {len(df)} eBay records from CSV")
        
        # Convert timestamp column
        df['collection_timestamp'] = pd.to_datetime(df['collection_timestamp'])
        
        # Prepare data for ClickHouse
        df['execution_date'] = df['collection_timestamp'].dt.date
        df['data_source'] = 'ebay_api'
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Convert boolean columns
        df['free_shipping'] = df['free_shipping'].astype(bool)
        
        # Insert data into ClickHouse
        client.insert_df('bronze.ebay_raw_data', df)
        
        print(f"✅ Successfully loaded {len(df)} eBay records to ClickHouse")
        
        # Log data quality metrics
        quality_log = {
            'log_timestamp': datetime.now(),
            'table_name': 'bronze.ebay_raw_data',
            'check_type': 'record_count',
            'check_result': 'success',
            'error_count': 0,
            'total_count': len(df),
            'details': f'Loaded from {ebay_file.name}',
            'execution_id': f'manual_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        }
        
        client.insert('bronze.data_quality_logs', [quality_log])
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading eBay data: {e}")
        return False

def load_weather_data(client):
    """Load weather data from CSV to ClickHouse"""
    weather_file = Path("../weather_data/new_york_city_hourly_weather_data.csv")
    
    if not weather_file.exists():
        print(f"❌ Weather data file not found: {weather_file}")
        return False
    
    try:
        # Read weather CSV (skip header rows)
        df = pd.read_csv(weather_file, skiprows=3)
        print(f"🌤️ Loaded {len(df)} weather records from CSV")
        
        # Rename columns to match ClickHouse schema
        column_mapping = {
            'weather_code (wmo code)': 'weather_code_wmo_code',
            'temperature_2m (°C)': 'temperature_2m_c',
            'relative_humidity_2m (%)': 'relative_humidity_2m_percent',
            'cloudcover (%)': 'cloudcover_percent',
            'rain (mm)': 'rain_mm',
            'sunshine_duration (s)': 'sunshine_duration_s',
            'windspeed_10m (km/h)': 'windspeed_10m_kmh'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convert time column to datetime
        df['time'] = pd.to_datetime(df['time'])
        
        # Prepare data for ClickHouse
        df['data_source'] = 'open_meteo_api'
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Insert data into ClickHouse
        client.insert_df('bronze.weather_raw_data', df)
        
        print(f"✅ Successfully loaded {len(df)} weather records to ClickHouse")
        
        # Log data quality metrics
        quality_log = {
            'log_timestamp': datetime.now(),
            'table_name': 'bronze.weather_raw_data',
            'check_type': 'record_count',
            'check_result': 'success',
            'error_count': 0,
            'total_count': len(df),
            'details': f'Loaded from {weather_file.name}',
            'execution_id': f'manual_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        }
        
        client.insert('bronze.data_quality_logs', [quality_log])
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading weather data: {e}")
        return False

def validate_data_loaded(client):
    """Validate that data was loaded correctly"""
    try:
        # Check eBay data
        ebay_count = client.query("SELECT COUNT(*) FROM bronze.ebay_raw_data").result_rows[0][0]
        print(f"📊 eBay records in ClickHouse: {ebay_count}")
        
        # Check weather data
        weather_count = client.query("SELECT COUNT(*) FROM bronze.weather_raw_data").result_rows[0][0]
        print(f"🌤️ Weather records in ClickHouse: {weather_count}")
        
        # Check data quality logs
        quality_count = client.query("SELECT COUNT(*) FROM bronze.data_quality_logs").result_rows[0][0]
        print(f"📋 Data quality logs: {quality_count}")
        
        # Show sample data
        print("\n📊 Sample eBay data:")
        sample_ebay = client.query("SELECT product_type, price, weather_category FROM bronze.ebay_raw_data LIMIT 3").result_rows
        for row in sample_ebay:
            print(f"  {row[0]} - ${row[1]} - {row[2]}")
        
        print("\n🌤️ Sample weather data:")
        sample_weather = client.query("SELECT time, temperature_2m_c, rain_mm FROM bronze.weather_raw_data LIMIT 3").result_rows
        for row in sample_weather:
            print(f"  {row[0]} - {row[1]}°C - {row[2]}mm rain")
        
        return True
        
    except Exception as e:
        print(f"❌ Error validating data: {e}")
        return False

def main():
    """Main execution function"""
    print("🚀 Starting ClickHouse data loading...")
    
    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        return False
    
    # Load data
    ebay_success = load_ebay_data(client)
    weather_success = load_weather_data(client)
    
    # Validate
    if ebay_success and weather_success:
        validate_data_loaded(client)
        print("✅ Data loading completed successfully!")
        return True
    else:
        print("❌ Data loading failed")
        return False

if __name__ == "__main__":
    main()
