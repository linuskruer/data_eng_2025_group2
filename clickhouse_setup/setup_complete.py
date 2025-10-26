#!/usr/bin/env python3
"""
Complete Setup Script for ClickHouse Medallion Architecture
This script sets up the entire data pipeline from Airflow to ClickHouse with dbt
"""

import subprocess
import sys
import time
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def check_prerequisites():
    """Check if required tools are installed"""
    print("🔍 Checking prerequisites...")
    
    # Check Docker
    if not run_command("docker --version", "Checking Docker"):
        print("❌ Docker is not installed. Please install Docker first.")
        return False
    
    # Check Python
    if not run_command("python --version", "Checking Python"):
        print("❌ Python is not installed. Please install Python first.")
        return False
    
    print("✅ All prerequisites are met")
    return True

def setup_clickhouse():
    """Set up ClickHouse database"""
    print("\n🏗️ Setting up ClickHouse...")
    
    # Start ClickHouse
    if not run_command("docker-compose -f docker-compose-clickhouse-simple.yaml up -d", "Starting ClickHouse"):
        return False
    
    # Wait for ClickHouse to be ready
    print("⏳ Waiting for ClickHouse to be ready...")
    time.sleep(20)
    
    # Create databases
    if not run_command(
        'docker exec clickhouse-server clickhouse-client --query "CREATE DATABASE IF NOT EXISTS bronze; CREATE DATABASE IF NOT EXISTS silver; CREATE DATABASE IF NOT EXISTS gold;"',
        "Creating databases"
    ):
        return False
    
    # Create Bronze tables
    if not run_command(
        "Get-Content sql\\bronze_tables_updated.sql | docker exec -i clickhouse-server clickhouse-client --multiquery",
        "Creating Bronze layer tables"
    ):
        return False
    
    print("✅ ClickHouse setup completed")
    return True

def install_dependencies():
    """Install Python dependencies"""
    print("\n📚 Installing dependencies...")
    
    dependencies = [
        "clickhouse-connect",
        "pandas",
        "dbt-clickhouse"
    ]
    
    for dep in dependencies:
        if not run_command(f"pip install {dep}", f"Installing {dep}"):
            print(f"⚠️ Warning: Failed to install {dep}")
    
    print("✅ Dependencies installation completed")
    return True

def load_data():
    """Load existing data into ClickHouse"""
    print("\n📊 Loading data into ClickHouse...")
    
    if not run_command("python load_data_final.py", "Loading data"):
        return False
    
    print("✅ Data loading completed")
    return True

def setup_dbt():
    """Set up dbt project"""
    print("\n🔧 Setting up dbt...")
    
    # Check if dbt is installed
    if not run_command("dbt --version", "Checking dbt installation"):
        print("❌ dbt is not installed. Please install dbt-clickhouse first.")
        return False
    
    # Initialize dbt project (if not already done)
    dbt_dir = Path("dbt")
    if not dbt_dir.exists():
        print("📁 dbt directory not found. Please ensure dbt models are in place.")
        return False
    
    print("✅ dbt setup completed")
    return True

def verify_setup():
    """Verify the complete setup"""
    print("\n🔍 Verifying setup...")
    
    # Check ClickHouse connection
    if not run_command(
        'docker exec clickhouse-server clickhouse-client --query "SELECT version()"',
        "Testing ClickHouse connection"
    ):
        return False
    
    # Check data loading
    if not run_command(
        'docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM bronze.ebay_raw_data"',
        "Checking eBay data"
    ):
        return False
    
    if not run_command(
        'docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM bronze.weather_raw_data"',
        "Checking weather data"
    ):
        return False
    
    print("✅ Setup verification completed")
    return True

def show_next_steps():
    """Show next steps for the user"""
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next Steps:")
    print("1. 🌐 Access ClickHouse Web UI: http://localhost:8123")
    print("2. 🔧 Use ClickHouse client: docker exec -it clickhouse-server clickhouse-client")
    print("3. 📊 Run analytical queries: Get-Content sql\\analytical_queries.sql | docker exec -i clickhouse-server clickhouse-client --multiquery")
    print("4. 🔄 Set up dbt models: cd dbt && dbt run")
    print("5. 📈 Monitor data quality: Check bronze.data_quality_logs table")
    
    print("\n📚 Documentation:")
    print("- Main README: README.md")
    print("- Implementation Summary: IMPLEMENTATION_SUMMARY.md")
    print("- dbt Quick Start: dbt/DBT_QUICK_START.md")
    
    print("\n🔗 Integration with Existing Code:")
    print("- Airflow DAGs: ../ebay_api_scripts/dags/")
    print("- Data Loading: airflow_dags/clickhouse_data_loader.py")
    print("- Bronze Layer: sql/bronze_tables_updated.sql")

def main():
    """Main setup function"""
    print("🚀 Starting ClickHouse Medallion Architecture Setup")
    print("=" * 60)
    
    steps = [
        ("Prerequisites Check", check_prerequisites),
        ("ClickHouse Setup", setup_clickhouse),
        ("Dependencies Installation", install_dependencies),
        ("Data Loading", load_data),
        ("dbt Setup", setup_dbt),
        ("Setup Verification", verify_setup)
    ]
    
    for step_name, step_function in steps:
        print(f"\n📋 {step_name}")
        print("-" * 40)
        
        if not step_function():
            print(f"\n❌ Setup failed at step: {step_name}")
            print("Please check the error messages above and try again.")
            return False
    
    show_next_steps()
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
