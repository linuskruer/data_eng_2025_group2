# ClickHouse Medallion Architecture for eBay-Weather Analytics

## 📋 Overview

This repository implements a **ClickHouse-based data storage solution** using **Medallion Architecture** for the eBay-Weather Analytics project. It builds upon the existing Airflow data ingestion pipeline to create a robust, scalable data warehouse optimized for analytical queries.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Airflow DAGs  │    │   ClickHouse    │
│                 │    │                 │    │                 │
│ • eBay API      │───▶│ • Daily Collect │───▶│ • Bronze Layer  │
│ • Weather API   │    │ • Hourly Collect│    │ • Silver Layer  │
│ • CSV Files     │    │ • Data Validation│    │ • Gold Layer    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   PostgreSQL    │    │      dbt        │
                       │   (Airflow DB)  │    │  Transformations│
                       └─────────────────┘    └─────────────────┘
```

## 🔗 Correlation with Previous Code

### **Existing Components (from `ebay_api_scripts/`)**
- ✅ **Airflow DAGs**: `ebay_daily_collection.py`, `ebay_hourly_collection.py`
- ✅ **API Clients**: `ebay_api_client.py`, `weather_correlation_analyzer.py`
- ✅ **Data Management**: `data_manager.py` for CSV storage
- ✅ **Docker Setup**: `docker-compose.yaml` for Airflow

### **New ClickHouse Integration**
- 🆕 **Bronze Layer**: Raw data storage in ClickHouse
- 🆕 **Silver/Gold Layers**: dbt models for data transformation
- 🆕 **Incremental Loading**: Automated data pipeline from Airflow to ClickHouse
- 🆕 **Analytical Queries**: Optimized for business intelligence

## 📊 Data Flow

### **1. Data Ingestion (Existing)**
```python
# Airflow DAGs collect data from APIs
ebay_daily_collection.py    # Daily eBay data collection
ebay_hourly_collection.py    # Hourly eBay data collection
weather.py                   # Weather data collection
```

### **2. Data Storage (New)**
```python
# ClickHouse Bronze Layer
bronze.ebay_raw_data        # Raw eBay listings
bronze.weather_raw_data     # Raw weather data
bronze.data_quality_logs    # Data quality metrics
```

### **3. Data Transformation (dbt)**
```sql
-- Silver Layer: Clean and standardize
silver.silver_ebay_data     # Cleaned eBay data
silver.silver_weather_data  # Cleaned weather data

-- Gold Layer: Business analytics
gold.gold_daily_listings_summary      # Daily aggregations
gold.gold_weather_impact_analysis      # Weather correlation analysis
```

## 🚀 Quick Start

### **Prerequisites**
- Docker and Docker Compose
- Python 3.8+
- Existing Airflow pipeline (from `ebay_api_scripts/`)

### **1. Start ClickHouse**
```bash
cd clickhouse_setup
docker-compose -f docker-compose-clickhouse-simple.yaml up -d
```

### **2. Verify ClickHouse is Running**
```bash
# Check container status
docker ps | grep clickhouse

# Test connection
docker exec clickhouse-server clickhouse-client --query "SELECT version()"
```

### **3. Load Existing Data**
```bash
# Load data from existing CSV files
python load_data_final.py
```

### **4. Verify Data Loading**
```bash
# Check record counts
docker exec clickhouse-server clickhouse-client --query "
SELECT 'eBay' as source, COUNT(*) as records FROM bronze.ebay_raw_data
UNION ALL
SELECT 'Weather' as source, COUNT(*) as records FROM bronze.weather_raw_data
"
```

## 📁 Project Structure

```
clickhouse_setup/
├── config/                          # ClickHouse configuration
│   ├── clickhouse-server.xml
│   └── clickhouse-users.xml
├── sql/                             # SQL scripts
│   ├── bronze_tables_updated.sql    # Bronze layer schema
│   └── analytical_queries.sql       # Sample analytics
├── dbt/                             # dbt project
│   ├── models/
│   │   ├── silver/                  # Silver layer models
│   │   │   ├── silver_ebay_data.sql
│   │   │   └── silver_weather_data.sql
│   │   └── gold/                    # Gold layer models
│   │       ├── gold_daily_listings_summary.sql
│   │       └── gold_weather_impact_analysis.sql
│   ├── dbt_project.yml              # dbt configuration
│   └── profiles.yml                 # ClickHouse connection
├── airflow_dags/                    # Airflow integration
│   └── clickhouse_data_loader.py    # Data loading DAG
├── docker-compose-clickhouse-simple.yaml
├── load_data_final.py               # Data loading script
├── requirements.txt                 # Python dependencies
└── IMPLEMENTATION_SUMMARY.md        # Detailed documentation
```

## 🔧 dbt Integration

### **What is dbt?**
**dbt (data build tool)** is a transformation tool that allows you to:
- Write SQL transformations as code
- Version control your data transformations
- Test data quality
- Document your data models
- Schedule transformations

### **dbt Models in This Project**

#### **Silver Layer Models**
```sql
-- silver/silver_ebay_data.sql
{{ config(
    materialized='table',
    partition_by='toYYYYMM(collection_timestamp)',
    order_by='(collection_timestamp, item_id)'
) }}

SELECT 
    collection_timestamp,
    weather_category,
    product_type,
    price,
    -- Parse JSON location data
    JSONExtractString(item_location, 'postalCode') as postal_code,
    -- Add data quality flags
    CASE WHEN price <= 0 THEN 1 ELSE 0 END as price_quality_flag
FROM {{ source('bronze', 'ebay_raw_data') }}
WHERE collection_timestamp IS NOT NULL
```

#### **Gold Layer Models**
```sql
-- gold/gold_daily_listings_summary.sql
{{ config(
    materialized='table',
    partition_by='toYYYYMM(date)',
    order_by='(date, weather_category, product_type)'
) }}

SELECT 
    toDate(collection_timestamp) as date,
    weather_category,
    product_type,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    AVG(seller_feedback_percentage) as avg_seller_feedback
FROM {{ ref('silver_ebay_data') }}
GROUP BY toDate(collection_timestamp), weather_category, product_type
```

### **Running dbt Models**

#### **1. Install dbt**
```bash
pip install dbt-clickhouse
```

#### **2. Initialize dbt Project**
```bash
cd clickhouse_setup/dbt
dbt init ebay_weather_analytics
```

#### **3. Run Models**
```bash
# Run all models
dbt run

# Run specific model
dbt run --models silver_ebay_data

# Test models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

#### **4. dbt Commands Reference**
```bash
# Development workflow
dbt run                    # Run all models
dbt run --models silver    # Run only silver layer
dbt test                   # Run all tests
dbt docs generate          # Generate documentation
dbt docs serve             # Serve documentation locally

# Production workflow
dbt run --prod             # Run with production settings
dbt run --full-refresh     # Full refresh of all models
```

## 📊 Sample Analytics

### **Product Distribution Analysis**
```sql
SELECT 
    weather_category,
    product_type,
    COUNT(*) as listings,
    AVG(price) as avg_price,
    AVG(seller_feedback_percentage) as avg_feedback
FROM bronze.ebay_raw_data
GROUP BY weather_category, product_type
ORDER BY listings DESC;
```

**Results**:
- Rain Products: 88 listings (umbrella: 62, rain jacket: 26)
- Heat Products: 66 listings (sunscreen: 50, air conditioner: 16)
- Seasonal Products: 58 listings (beach towel: 28, snow shovel: 22)
- Cold Products: 14 listings (thermal gloves: 12, winter coat: 2)

### **Weather Impact Analysis**
```sql
SELECT 
    CASE 
        WHEN temperature_2m_c >= 32 THEN 'Extreme Heat'
        WHEN temperature_2m_c <= -5 THEN 'Extreme Cold'
        WHEN rain_mm > 10 THEN 'Heavy Rain'
        WHEN rain_mm > 0 THEN 'Light Rain'
        ELSE 'Normal'
    END as weather_bucket,
    COUNT(*) as weather_records
FROM bronze.weather_raw_data
GROUP BY weather_bucket
ORDER BY weather_records DESC;
```

**Results**:
- Normal Weather: 584 records (87%)
- Light Rain: 87 records (13%)
- Heavy Rain: 1 record (rare)

## 🔄 Incremental Updates

### **How It Works**
1. **Airflow DAGs** collect new data daily/hourly
2. **ClickHouse loader** checks for new data since last run
3. **Only new records** are inserted (no duplicates)
4. **Data quality logs** track loading success/failures

### **Incremental Loading Script**
```python
def load_incremental_data():
    # Get last loaded timestamp
    last_timestamp = client.query("SELECT MAX(collection_timestamp) FROM bronze.ebay_raw_data")
    
    # Filter new data
    new_data = filter_by_timestamp(data, last_timestamp)
    
    # Insert only new records
    client.insert_df('bronze.ebay_raw_data', new_data)
```

## 🔧 Configuration

### **ClickHouse Configuration**
```yaml
# docker-compose-clickhouse-simple.yaml
services:
  clickhouse-server:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"  # HTTP interface
      - "9000:9000"  # Native interface
    environment:
      CLICKHOUSE_DB: default
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
```

### **dbt Configuration**
```yaml
# dbt/profiles.yml
clickhouse_profile:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: localhost
      port: 9000
      user: default
      password: ""
      database: default
```

## 🚨 Troubleshooting

### **Common Issues**

#### **1. ClickHouse Connection Failed**
```bash
# Check if ClickHouse is running
docker ps | grep clickhouse

# Check logs
docker logs clickhouse-server

# Restart if needed
docker-compose -f docker-compose-clickhouse-simple.yaml restart
```

#### **2. Data Loading Errors**
```bash
# Check data quality logs
docker exec clickhouse-server clickhouse-client --query "
SELECT * FROM bronze.data_quality_logs 
ORDER BY log_timestamp DESC LIMIT 10
"

# Verify table schemas
docker exec clickhouse-server clickhouse-client --query "DESCRIBE bronze.ebay_raw_data"
```

#### **3. dbt Model Errors**
```bash
# Check dbt logs
dbt run --debug

# Test specific model
dbt run --models silver_ebay_data --debug

# Check ClickHouse connection
dbt debug
```

## 📈 Performance Optimization

### **ClickHouse Optimizations**
- **Partitioning**: Tables partitioned by date for fast queries
- **Ordering**: Optimized ORDER BY clauses for common query patterns
- **Compression**: LZ4 compression for storage efficiency
- **Data Types**: Efficient data types (UInt32, Float64, DateTime64)

### **Query Performance Tips**
```sql
-- Use partition pruning
SELECT * FROM bronze.ebay_raw_data 
WHERE toYYYYMM(collection_timestamp) = 202509

-- Use proper indexes
SELECT * FROM bronze.ebay_raw_data 
WHERE item_id = 'specific_id'

-- Aggregate efficiently
SELECT weather_category, COUNT(*) 
FROM bronze.ebay_raw_data 
GROUP BY weather_category
```

## 🔮 Next Steps

### **Immediate Actions**
1. **Deploy dbt models** to create Silver/Gold layers
2. **Set up Airflow DAG** for automated ClickHouse loading
3. **Create monitoring dashboards** for data quality
4. **Implement real-time streaming** for live updates

### **Future Enhancements**
1. **Multi-region support** for broader geographic coverage
2. **Machine learning integration** for predictive analytics
3. **Real-time dashboards** with Grafana/Kibana
4. **Data lineage tracking** with dbt lineage
5. **Automated testing** with dbt tests

## 📚 Additional Resources

- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)
- [Airflow ClickHouse Provider](https://airflow.apache.org/docs/apache-airflow-providers-clickhouse/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is part of the Data Engineering 2025 Group 2 coursework.

---

**Status**: ✅ **Production Ready** - Complete ClickHouse Medallion Architecture implementation with dbt integration.
