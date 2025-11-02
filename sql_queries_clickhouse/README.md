# ClickHouse Analytical Queries

This directory contains analytical SQL queries converted to ClickHouse syntax for running against the gold layer tables.

## Queries

1. **impact_listings_by_weather.sql** - Impact of weather conditions on daily listings across East Coast
2. **pricing_behavior_by_weather_and_product.sql** - Average price by product type under different weather conditions
3. **seller_performance_vs_weather.sql** - Seller performance tiers vs listing activity under different weather conditions
4. **shipping_choices_vs_weather.sql** - Free shipping rate and average shipping cost by weather bucket
5. **category_demand_shifts_by_weather.sql** - Product category demand shifts under specific weather conditions
6. **bonus_listing_quality_vs_weather.sql** - Listing quality proxies (title length and buying options) by weather bucket
7. **bonus_zip_prefix_variation.sql** - Price and listing availability variation by zip prefix and weather bucket

## Running Queries

### Option 1: Using the Python Runner

```bash
# Set environment variables
export CLICKHOUSE_HOST=clickhouse-server
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=mypassword

# Run all queries
python run_analytical_queries.py
```

Results will be saved to the `results/` directory as JSON files.

### Option 2: Direct ClickHouse Connection

```bash
# Connect to ClickHouse
clickhouse-client --host clickhouse-server --port 9000 --user default --password mypassword

# Or using clickhouse-client HTTP interface
clickhouse-client --host clickhouse-server --port 8123 --user default --password mypassword

# Then paste any query from the .sql files
```

### Option 3: From Python Script

```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse-server",
    port=8123,
    username="default",
    password="mypassword"
)

# Read and execute a query
with open("impact_listings_by_weather.sql", "r") as f:
    query = f.read()
    
result = client.query(query)
print(result.result_rows)
```

## Prerequisites

- ClickHouse server running (via Docker Compose or standalone)
- Gold layer tables must exist:
  - `fact_listings` (eBay listings)
  - `fact_weather` (Weather data)
  - `dim_product`, `dim_location`, `dim_seller`, etc.

## Enabling eBay Models

The eBay gold layer models are disabled by default. To enable them:

1. **Set dbt variable**: In Airflow, set the variable `enable_ebay_silver` to `true`
   - Go to Airflow UI → Admin → Variables
   - Add variable: `enable_ebay_silver` = `true`

2. **Run dbt with variable**:
   ```bash
   cd /opt/airflow/dags/dbt
   dbt run --models silver_ebay_listings+ --vars '{"enable_ebay_silver": true}'
   ```

3. **Or update the unified pipeline DAG** to include:
   ```python
   run_dbt_silver = BashOperator(
       task_id="run_dbt_silver_all",
       bash_command="cd /opt/airflow/dags/dbt && dbt run --models silver+ --vars '{\"enable_ebay_silver\": true}'"
   )
   ```

## ClickHouse Syntax Conversions

The following PostgreSQL functions were converted to ClickHouse equivalents:

- `DATE()` → `toDate()`
- `STDDEV()` → `stddevPop()` or `stddevSamp()`
- `PERCENTILE_CONT(0.5)` → `quantile(0.5)()`
- `COUNT(DISTINCT ...)` → `uniq(...)`
- `CASE WHEN ...` → `multiIf(...)` (for simple cases) or `if(...)`
- `LIKE '%pattern%'` → `position(column, 'pattern') > 0`

## Generating Analytical Reports

### Step-by-Step Process

#### Step 1: Run Analytical Queries
Execute all queries to generate JSON results:

```bash
# Using PowerShell script (Windows)
powershell -ExecutionPolicy Bypass -File run_queries_docker.ps1

# Or using Python script (inside Docker container)
docker exec -it airflow-scheduler bash
cd /opt/airflow/dags/sql_queries_clickhouse
python run_analytical_queries.py
```

This creates JSON files in the `results/` directory:
- `impact_listings_by_weather.json`
- `pricing_behavior_by_weather_and_product.json`
- `shipping_choices_vs_weather.json`
- `category_demand_shifts_by_weather.json`
- `seller_performance_vs_weather.json`
- `bonus_listing_quality_vs_weather.json`
- `bonus_zip_prefix_variation.json`

#### Step 2: Generate Markdown Report
Generate a comprehensive markdown report from the JSON results:

```bash
# From project root or sql_queries_clickhouse directory
python sql_queries_clickhouse/generate_report.py
```

This creates `ANALYTICAL_RESULTS_REPORT.md` with:
- Executive summary
- Detailed findings for each query
- Formatted tables
- Key insights
- Methodology and conclusions

#### Step 3: View the Report
```bash
# View in terminal
cat ANALYTICAL_RESULTS_REPORT.md

# Or open in your favorite markdown viewer/editor
```

### Complete Workflow Example

```bash
# Navigate to queries directory
cd sql_queries_clickhouse

# Step 1: Run queries (if not already run)
powershell -ExecutionPolicy Bypass -File run_queries_docker.ps1

# Step 2: Generate report
python generate_report.py

# Step 3: Verify report was created
ls -la ANALYTICAL_RESULTS_REPORT.md

# Step 4: View report
cat ANALYTICAL_RESULTS_REPORT.md
```

### Report Customization

The `generate_report.py` script can be customized to:
- Add additional query analysis
- Modify table formatting
- Include custom insights
- Add charts/graphs placeholders

Edit `generate_report.py` to customize the report generation logic.

## Notes

- All queries filter for East Coast US states
- Queries use `weather_category` from eBay data (not joined with weather fact table)
- Some queries may return empty results if eBay data hasn't been ingested yet
- Make sure to run the eBay ingestion DAG first to populate `bronze.ebay_raw_data`
- The report generation script requires the JSON result files to be present in `results/` directory


