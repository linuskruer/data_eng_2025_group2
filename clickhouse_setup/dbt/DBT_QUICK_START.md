# dbt Quick Start Guide

## 🚀 Setting Up dbt with ClickHouse

### **1. Install dbt**
```bash
pip install dbt-clickhouse
```

### **2. Initialize dbt Project**
```bash
cd clickhouse_setup/dbt
dbt init ebay_weather_analytics
```

### **3. Configure Connection**
The `profiles.yml` file is already configured:
```yaml
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

### **4. Run Models**
```bash
# Run all models
dbt run

# Run specific layer
dbt run --models silver
dbt run --models gold

# Run specific model
dbt run --models silver_ebay_data

# Test models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📊 Model Dependencies

```
bronze.ebay_raw_data ──┐
                       ├──► silver.silver_ebay_data ──► gold.gold_daily_listings_summary
bronze.weather_raw_data ──┘
                       ├──► silver.silver_weather_data ──► gold.gold_weather_impact_analysis
```

## 🔧 dbt Commands Reference

| Command | Description |
|---------|-------------|
| `dbt run` | Run all models |
| `dbt run --models silver` | Run only silver layer |
| `dbt run --models +gold_daily_listings_summary` | Run model and dependencies |
| `dbt test` | Run all tests |
| `dbt test --models silver_ebay_data` | Test specific model |
| `dbt docs generate` | Generate documentation |
| `dbt docs serve` | Serve docs locally (http://localhost:8080) |
| `dbt debug` | Debug connection issues |
| `dbt compile` | Compile models without running |

## 📋 Model Configuration

### **Silver Layer**
```sql
{{ config(
    materialized='table',
    partition_by='toYYYYMM(collection_timestamp)',
    order_by='(collection_timestamp, item_id)'
) }}
```

### **Gold Layer**
```sql
{{ config(
    materialized='table',
    partition_by='toYYYYMM(date)',
    order_by='(date, weather_category, product_type)'
) }}
```

## 🧪 Testing

### **Data Quality Tests**
```sql
-- tests/assert_ebay_data_quality.sql
SELECT *
FROM {{ ref('silver_ebay_data') }}
WHERE price <= 0
   OR seller_feedback_percentage < 0
   OR seller_feedback_percentage > 100
```

### **Run Tests**
```bash
dbt test --models silver_ebay_data
```

## 📈 Monitoring

### **Model Performance**
```bash
# Check model run times
dbt run --profiles-dir . --target dev

# View run results
dbt run --profiles-dir . --target dev --log-level debug
```

### **Data Quality Monitoring**
```sql
-- Check data quality metrics
SELECT 
    table_name,
    check_type,
    check_result,
    error_count,
    total_count,
    log_timestamp
FROM bronze.data_quality_logs
ORDER BY log_timestamp DESC
LIMIT 10;
```
