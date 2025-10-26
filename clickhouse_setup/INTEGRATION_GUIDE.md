# Integration Guide: Airflow + ClickHouse + dbt

## 🔗 How This Connects to Your Existing Code

This ClickHouse implementation seamlessly integrates with your existing Airflow pipeline to create a complete data engineering solution.

## 📊 Data Flow Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXISTING AIRFLOW PIPELINE                    │
├─────────────────────────────────────────────────────────────────┤
│  ebay_api_scripts/dags/                                        │
│  ├── ebay_daily_collection.py     ──┐                          │
│  ├── ebay_hourly_collection.py    ──┤                          │
│  └── data_manager.py              ──┘                          │
│                                                                 │
│  ebay_api_scripts/                                              │
│  ├── ebay_api_client.py          ──┐                          │
│  ├── weather_correlation_analyzer.py ──┤                        │
│  └── weather.py                  ──┘                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    NEW CLICKHOUSE LAYER                         │
├─────────────────────────────────────────────────────────────────┤
│  clickhouse_setup/                                             │
│  ├── airflow_dags/clickhouse_data_loader.py  ← NEW DAG         │
│  ├── sql/bronze_tables_updated.sql          ← NEW TABLES       │
│  ├── dbt/models/                             ← NEW TRANSFORMS  │
│  └── load_data_final.py                     ← DATA LOADER     │
└─────────────────────────────────────────────────────────────────┘
```

## 🔄 Integration Points

### **1. Airflow DAG Integration**

#### **Existing DAGs** (in `ebay_api_scripts/dags/`)
```python
# ebay_daily_collection.py - Collects eBay data daily
# ebay_hourly_collection.py - Collects eBay data hourly
# Both save data to CSV files via data_manager.py
```

#### **New DAG** (in `clickhouse_setup/airflow_dags/`)
```python
# clickhouse_data_loader.py - Loads CSV data into ClickHouse
# Runs after daily collection (schedule: '0 7 * * *')
# Depends on ebay_daily_collection DAG completion
```

### **2. Data Storage Integration**

#### **Before (CSV Only)**
```python
# data_manager.py saves to CSV
csv_file = data_manager.save_data(new_data, append=True)
# File: ../ebay_data/east_coast_weather_data.csv
```

#### **After (CSV + ClickHouse)**
```python
# CSV still created (for backup)
csv_file = data_manager.save_data(new_data, append=True)

# NEW: ClickHouse loading
client.insert_df('bronze.ebay_raw_data', df)
# Table: bronze.ebay_raw_data (partitioned, indexed)
```

### **3. Data Transformation Integration**

#### **Before (Python Only)**
```python
# weather_correlation_analyzer.py
# Basic data processing in Python
# Limited analytical capabilities
```

#### **After (Python + dbt)**
```sql
-- dbt models for advanced transformations
-- Silver layer: Clean and standardize
-- Gold layer: Business analytics
-- Automated testing and documentation
```

## 🚀 Step-by-Step Integration

### **Step 1: Keep Existing Airflow Running**
```bash
# Your existing setup (already running)
cd ebay_api_scripts
docker-compose up -d
```

### **Step 2: Add ClickHouse Layer**
```bash
# New ClickHouse setup
cd clickhouse_setup
docker-compose -f docker-compose-clickhouse-simple.yaml up -d
```

### **Step 3: Load Existing Data**
```bash
# Load your existing CSV data into ClickHouse
python load_data_final.py
```

### **Step 4: Set Up Automated Loading**
```bash
# Copy the new DAG to your Airflow dags folder
cp airflow_dags/clickhouse_data_loader.py ../ebay_api_scripts/dags/
```

### **Step 5: Run dbt Transformations**
```bash
# Transform Bronze → Silver → Gold
cd dbt
dbt run
```

## 📋 Modified Files and New Files

### **Files You Already Have** (No Changes Needed)
```
ebay_api_scripts/
├── dags/
│   ├── ebay_daily_collection.py     ✅ Keep as-is
│   └── ebay_hourly_collection.py    ✅ Keep as-is
├── ebay_api_client.py               ✅ Keep as-is
├── data_manager.py                  ✅ Keep as-is
├── weather_correlation_analyzer.py  ✅ Keep as-is
└── docker-compose.yaml              ✅ Keep as-is
```

### **New Files Added** (ClickHouse Integration)
```
clickhouse_setup/
├── airflow_dags/
│   └── clickhouse_data_loader.py    🆕 NEW: Loads data to ClickHouse
├── sql/
│   ├── bronze_tables_updated.sql    🆕 NEW: ClickHouse schema
│   └── analytical_queries.sql       🆕 NEW: Sample analytics
├── dbt/
│   ├── models/silver/               🆕 NEW: Data cleaning
│   ├── models/gold/                 🆕 NEW: Business analytics
│   └── dbt_project.yml              🆕 NEW: dbt configuration
├── docker-compose-clickhouse-simple.yaml 🆕 NEW: ClickHouse setup
└── load_data_final.py               🆕 NEW: Data loader
```

## 🔧 Configuration Changes

### **Airflow Configuration** (Minimal Changes)
```yaml
# Add to your existing docker-compose.yaml
volumes:
  - ./clickhouse_setup/airflow_dags:/opt/airflow/dags/clickhouse
  - ./clickhouse_setup:/opt/airflow/clickhouse_setup
```

### **ClickHouse Configuration** (New)
```yaml
# clickhouse_setup/docker-compose-clickhouse-simple.yaml
services:
  clickhouse-server:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
      - "9000:9000"
```

## 📊 Data Comparison

### **Before Integration**
```
Data Sources → Airflow → CSV Files
├── eBay API data: 113 records
├── Weather data: 672 records
└── Storage: CSV files only
```

### **After Integration**
```
Data Sources → Airflow → CSV Files + ClickHouse
├── eBay API data: 113 records → 226 ClickHouse records
├── Weather data: 672 records → 672 ClickHouse records
├── Storage: CSV files (backup) + ClickHouse (analytics)
└── Transformations: Bronze → Silver → Gold layers
```

## 🔄 Automated Workflow

### **Daily Schedule**
```
06:00 AM - ebay_daily_collection.py runs
07:00 AM - clickhouse_data_loader.py runs (loads new data)
08:00 AM - dbt models run (transforms Bronze → Silver → Gold)
```

### **Hourly Schedule**
```
Every hour - ebay_hourly_collection.py runs
Every hour - Incremental ClickHouse updates
```

## 🧪 Testing the Integration

### **1. Test Data Flow**
```bash
# Check Airflow is collecting data
docker exec ebay_api_scripts-airflow-scheduler-1 airflow dags list

# Check ClickHouse has data
docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM bronze.ebay_raw_data"
```

### **2. Test Analytics**
```bash
# Run sample analytical queries
Get-Content clickhouse_setup/sql/analytical_queries.sql | docker exec -i clickhouse-server clickhouse-client --multiquery
```

### **3. Test dbt Models**
```bash
cd clickhouse_setup/dbt
dbt run --models silver
dbt test
```

## 🚨 Troubleshooting Integration

### **Common Issues**

#### **1. Airflow Can't Find ClickHouse**
```bash
# Check if ClickHouse is running
docker ps | grep clickhouse

# Check network connectivity
docker exec ebay_api_scripts-airflow-scheduler-1 ping clickhouse-server
```

#### **2. Data Not Loading**
```bash
# Check data quality logs
docker exec clickhouse-server clickhouse-client --query "SELECT * FROM bronze.data_quality_logs ORDER BY log_timestamp DESC LIMIT 5"
```

#### **3. dbt Connection Issues**
```bash
# Test dbt connection
cd clickhouse_setup/dbt
dbt debug
```

## 📈 Benefits of Integration

### **Before (CSV Only)**
- ❌ Limited analytical capabilities
- ❌ No data quality monitoring
- ❌ Manual data processing
- ❌ No incremental updates
- ❌ Poor query performance

### **After (CSV + ClickHouse + dbt)**
- ✅ Advanced analytical queries
- ✅ Automated data quality monitoring
- ✅ Automated transformations
- ✅ Incremental updates
- ✅ High-performance analytics
- ✅ Data lineage tracking
- ✅ Automated testing
- ✅ Documentation generation

## 🎯 Next Steps

1. **Deploy Integration**: Run the setup scripts
2. **Monitor Performance**: Check data quality logs
3. **Run Analytics**: Execute analytical queries
4. **Scale Up**: Add more data sources
5. **Automate**: Set up monitoring dashboards

---

**Result**: Your existing Airflow pipeline now feeds into a powerful ClickHouse data warehouse with dbt transformations, creating a complete data engineering solution! 🚀
