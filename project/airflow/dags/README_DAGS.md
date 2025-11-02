# Airflow DAGs Documentation

## Overview

This project includes **3 Airflow DAGs** for orchestrating the complete data pipeline:

1. **`weather_data_ingestion`** - Weather-only pipeline
2. **`ebay_ingestion`** - eBay-only pipeline  
3. **`unified_data_pipeline`** - Combined orchestration for both sources

All DAGs follow the **Medallion Architecture**: Bronze → Silver → Gold

---

## 1. Weather Data Ingestion DAG (`weather_data_ingestion`)

**Schedule:** `@daily`  
**Purpose:** Ingest weather data from Open-Meteo API and transform through dbt

### Pipeline Flow:
```
fetch_weather_all_cities
  ↓
validate_weather_data (Data Quality Checks)
  ↓
load_weather_to_clickhouse (Bronze Layer)
  ↓
run_dbt_silver (Silver Layer)
  ↓
run_dbt_gold (Gold Layer)
  ↓
run_dbt_tests (Data Quality Tests)
```

### Features:
- ✅ Parameterized: `execution_date`, `file_version`
- ✅ Data quality checks: nulls, duplicates, range checks
- ✅ Idempotent: File cleanup before write
- ✅ Complete dbt transformation chain

---

## 2. eBay Ingestion DAG (`ebay_ingestion`)

**Schedule:** `@hourly`  
**Purpose:** Ingest eBay listings from Browse API and transform through dbt

### Pipeline Flow:
```
ingest_ebay_to_clickhouse (Bronze Layer)
  ↓
run_dbt_silver_ebay (Silver Layer)
  ↓
run_dbt_gold_ebay (Gold Layer)
  ↓
run_dbt_tests_ebay (Data Quality Tests)
```

### Features:
- ✅ Parameterized: `items_per_product`
- ✅ Data quality: Deduplication by `(item_id, collection_timestamp)`
- ✅ Idempotent: Prevents duplicate inserts
- ✅ Complete dbt transformation chain (now automated!)

**Note:** eBay models are conditional on `enable_ebay_silver=true` variable

---

## 3. Unified Data Pipeline DAG (`unified_data_pipeline`)

**Schedule:** `@daily`  
**Purpose:** Orchestrate both weather and eBay pipelines together

### Pipeline Flow:

#### Weather Branch:
```
fetch_weather_all_cities
  ↓
validate_weather_data
  ↓
load_weather_to_clickhouse (Bronze)
  ↓
run_dbt_silver_weather (Silver)
  ↓
run_dbt_gold_weather (Gold)
  ↓
run_dbt_tests_weather (Tests)
```

#### eBay Branch (runs in parallel):
```
ingest_ebay_to_clickhouse (Bronze)
  ↓
run_dbt_silver_ebay (Silver)
  ↓
run_dbt_gold_ebay (Gold)
  ↓
run_dbt_tests_ebay (Tests)
```

### Features:
- ✅ Runs both pipelines **in parallel**
- ✅ Independent failure handling (one pipeline failure doesn't block the other)
- ✅ Complete medallion architecture for both sources
- ✅ All dbt transformations automated

---

## DAG Comparison

| Feature | Weather DAG | eBay DAG | Unified DAG |
|---------|-------------|----------|-------------|
| **Schedule** | @daily | @hourly | @daily |
| **Bronze Ingestion** | ✅ | ✅ | ✅ |
| **Silver Transformation** | ✅ | ✅ | ✅ |
| **Gold Transformation** | ✅ | ✅ | ✅ |
| **dbt Tests** | ✅ | ✅ | ✅ |
| **Data Quality Checks** | ✅ (explicit) | ✅ (inline) | ✅ (both) |
| **Idempotent** | ✅ | ✅ | ✅ |
| **Parameterized** | ✅ | ✅ | ✅ |

---

## Usage

### Running Individual DAGs

**Weather Pipeline:**
```bash
# Trigger via Airflow UI or:
airflow dags trigger weather_data_ingestion
```

**eBay Pipeline:**
```bash
# Trigger via Airflow UI or:
airflow dags trigger ebay_ingestion
```

### Running Unified Pipeline

**Unified Pipeline:**
```bash
# Trigger via Airflow UI or:
airflow dags trigger unified_data_pipeline
```

**Note:** The unified DAG will:
- Run weather pipeline fully
- Run eBay pipeline fully (if data exists)
- Both pipelines can run concurrently

---

## Configuration

### Airflow Variables

Set these in Airflow UI (`Admin > Variables`):

- `EBAY_CLIENT_ID` - eBay API client ID
- `EBAY_CLIENT_SECRET` - eBay API client secret
- `enable_ebay_silver` - Set to `true` to enable eBay dbt models (default: `false`)

### dbt Variables

eBay models use conditional execution via:
```yaml
enabled=var('enable_ebay_silver', false)
```

This allows the pipeline to skip eBay transformations when data doesn't exist.

---

## Task Dependencies

### Weather DAG
```
fetch_weather >> validate >> load >> silver >> gold >> tests
```

### eBay DAG  
```
ingest >> silver >> gold >> tests
```

### Unified DAG
```
# Weather pipeline (sequential)
weather: fetch >> validate >> load >> silver >> gold >> tests

# eBay pipeline (sequential, runs parallel to weather)
ebay: ingest >> silver >> gold >> tests

# Both pipelines run independently
```

---

## Error Handling

- **Retries:** 1 retry with 5-minute delay (configurable)
- **Failure Handling:** Each pipeline handles errors independently in unified DAG
- **Data Quality:** Validation steps will fail the pipeline if data doesn't meet quality standards

---

## Monitoring

### Success Criteria

**Weather Pipeline:**
- ✅ Data fetched and validated
- ✅ Loaded to `bronze_weather`
- ✅ Transformed to `silver_weather`
- ✅ Transformed to `fact_weather` + dimensions
- ✅ All dbt tests pass

**eBay Pipeline:**
- ✅ Data ingested to `bronze.ebay_raw_data`
- ✅ Transformed to `silver_ebay_listings`
- ✅ Transformed to `fact_listings` + dimensions
- ✅ All dbt tests pass

**Unified Pipeline:**
- ✅ Both pipelines complete successfully
- ✅ All transformations executed
- ✅ All tests pass

---

## Best Practices

1. **Start with Individual DAGs:** Test weather and eBay pipelines separately first
2. **Enable eBay Models:** Set `enable_ebay_silver=true` only when eBay data exists
3. **Monitor Unified DAG:** Use unified DAG for production orchestration
4. **Check Logs:** Review task logs in Airflow UI for detailed execution info
5. **Idempotency:** All pipelines can be re-run safely without creating duplicates

---

## Troubleshooting

### eBay DAG fails on dbt tasks:
- **Check:** Is eBay data in bronze? (`SELECT COUNT(*) FROM bronze.ebay_raw_data`)
- **Solution:** Set `enable_ebay_silver=true` in Airflow Variables or run ingestion first

### Unified DAG fails:
- **Check:** Both pipelines run independently - check individual task logs
- **Solution:** Weather pipeline failure doesn't affect eBay, and vice versa

### dbt tests fail:
- **Check:** Review test output in Airflow logs
- **Solution:** Check data quality - nulls, duplicates, or range violations

---

**Last Updated:** November 2, 2025  
**Maintained by:** Data Engineering Group 2

