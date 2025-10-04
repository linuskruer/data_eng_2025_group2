## Project: Weather x eBay Analytics (East Coast)

### Overview
This project analyzes how US East Coast weather affects eBay marketplace activity. We join daily weather records with collected eBay listing snapshots to study listing volume, pricing, shipping choices, category demand shifts, seller behavior, and listing quality under different weather conditions.

### Business Questions
1. How do weather conditions (rain, extreme heat/cold) impact daily listings on the East Coast?
2. Does weather influence pricing? Are certain product types priced higher/lower during extreme weather?
3. What is the relationship between weather and shipping choices (free shipping, shipping cost)?
4. Are there shifts in product category demand under specific weather conditions?
5. Do seller performance metrics correlate with weather-driven listing activity?
6. (Bonus) Does weather impact listing quality (e.g., shorter titles, buying options)?
7. (Bonus) Which US East Coast zip code prefixes show the largest price/availability variation with changing weather?

## Architecture
- Data collection:
  - Weather: hourly/daily weather for selected East Coast locations.
  - eBay: listing snapshots via eBay Browse API.
- Storage: raw CSV/JSON in the repo’s data folders.
- Transform/Model: star schema in SQL warehouse.
- Analytics: parameterized SQL grouped by weather buckets and dimensions.

Artifacts:
- Diagram: `Data_Architecture/Data_Architecture.drawio.(png|pdf)`
- Star schema: `Star_Schema/Data_Eng_2025_group2.drawio.(png|pdf)`

## Repository Structure
- `ebay_api_scripts/`: eBay API client, token setup, collectors, DAGs, docker compose
  - `ebay_api_client.py`, `robust_collector.py`, `simple_auto_collector.py`, `setup_token.py`
  - `dags/`: Airflow DAGs `ebay_daily_collection.py`, `ebay_hourly_collection.py`
  - Docker assets: `Dockerfile`, `docker-compose*.yaml`
  - Config: `config.json`, `credentials.env`, `requirements.txt`
- `weather_data/`: weather dataset(s)
- `ebay_data/`: captured eBay listings datasets
- `sql_queries/`: analysis SQL files (ready to run)
- `sql_analysis/`: SQL analysis tools and testing framework
  - `run_sql_queries.py` - Main script to run individual or all SQL queries
  - `data_analysis.py` - Comprehensive data structure analysis
  - `validate_real_data_queries.py` - Validates SQL queries against real API data
  - `test_runner.py` - Orchestrates the complete test suite
  - `test_sql_pipeline.py` - Main test suite for schema validation and query execution
  - `setup_database.py` - Creates star schema and loads sample data
  - `quick_test.py` - Quick testing without database connection
  - `requirements_test.txt` - Python dependencies for testing
  - Analysis reports and documentation
- `data_dictionaries/`: machine-readable dictionaries for source fields
- `logs/`: collection logs
- `Data_Architecture/`, `Star_Schema/`: diagrams

## Data Dictionary
### Weather Data (daily snapshot)
| Column | Description | Type |
|---|---|---|
| `location` | City/region of weather record | string |
| `date` | Date of observation | date |
| `weather_code` | Weather condition code | integer |
| `temperature_2m` | Temperature at 2m (°C) | float |
| `relative_humidity_2m` | Relative humidity at 2m (%) | integer |
| `cloudcover` | Cloudcover (%) | float |
| `rain` | Rain amount (mm) | float |
| `sunshine_duration` | Duration of sunlight (seconds) | integer |
| `wind_speed_10m` | Average wind speed at 10m (m/s) | float |

### eBay Data (listing snapshot)
| Column | Description | Type |
|---|---|---|
| `collection_timestamp` | Timestamp when data was collected | datetime |
| `timezone` | Timezone of the item listing | string |
| `weather_category` | Weather at item location (optional) | string |
| `product_type` | Type of product | string |
| `price` | Item price | float |
| `currency` | Currency of price | string |
| `seller_feedback_percentage` | Seller feedback percentage | float |
| `seller_feedback_score` | Seller feedback score | integer |
| `item_location` | Location of the item | string |
| `seller_location` | Seller’s registered location | string |
| `shipping_cost` | Cost of shipping | float |
| `free_shipping` | Indicates if shipping is free | boolean |
| `condition` | Condition of the item (new/used) | string |
| `buying_options` | Available options (auction/buy it now) | string |
| `title_length` | Length of the item title | integer |
| `item_id` | Unique item identifier | string |
| `marketplace_id` | Marketplace identifier (e.g., eBay US) | string |

## Star Schema (Analytical Model)
Grain: one row per collected listing event.

- Fact: `fact_listings`
  - Degenerate: `item_id`
  - Measures: `price`, `shipping_cost`, `free_shipping`, `title_length`
  - Foreign keys: `date_key`, `location_key`, `product_key`, `seller_key`, `marketplace_key`, `currency_key`, `condition_key`, `buying_option_key`
- Dimensions:
  - `dim_date` (calendar; `date_key = DATE(collection_timestamp)`)
  - `dim_location` (city/region/state/country/zip_prefix)
  - `dim_product` (`product_type`)
  - `dim_seller` (binned `seller_feedback_score`, `seller_feedback_percentage`)
  - `dim_marketplace`, `dim_currency`, `dim_condition`, `dim_buying_option`
  - Weather daily table joined by `(date, location_key)` with fields: `temperature_2m`, `rain`, `sunshine_duration`, `wind_speed_10m`, optional `weather_category`

East Coast filter states: `ME,NH,VT,MA,RI,CT,NY,NJ,PA,DE,MD,DC,VA,NC,SC,GA,FL`.

## Environment Setup
### Prerequisites
- Python 3.10+
- pip
- PostgreSQL

### Install Python dependencies
```bash
pip install -r ebay_api_scripts/requirements.txt
```

### Configure credentials and config
- `ebay_api_scripts/credentials.env` (client ID/secret, etc.)
- `ebay_api_scripts/config.json` (API endpoints, search params, locations)

### Obtain eBay API token
```bash
python ebay_api_scripts/setup_token.py
```
This stores/refreshes a Browse API token in `ebay_api_scripts/ebay_token.*`.

## Data Collection
### Quick collectors (local)
- Run a simple collector (example):
```bash
python ebay_api_scripts/simple_auto_collector.py
```
- Robust loop with retries/scheduling:
```bash
python ebay_api_scripts/robust_collector.py
```

### Airflow (optional)
- Use `ebay_api_scripts/dags/*.py` with docker compose:
```bash
cd ebay_api_scripts
docker compose -f docker-compose.yaml up -d
```
Check the Airflow UI, ensure variables/connections align with `config.json`.

## Data Locations
- Weather CSV: `weather_data/`
- eBay listings CSV: `ebay_data/`
- Logs: `logs/`

## Warehouse Model and Loading
1. Create dimensions and fact tables per Star Schema above (DDL as per warehouse). Ensure indexes on `fact_listings(date_key, location_key)` and `weather(date, location_key)`.
2. Load weather daily table and map to `dim_location` to produce consistent `location_key`.
3. Load listing snapshots into `fact_listings`, deriving `date_key = DATE(collection_timestamp)` and `location_key` from item/seller location mapping.

## SQL Analytics

### Query Mapping to Business Questions
Each SQL query corresponds to a specific business question:

| Query # | Business Question | SQL File |
|---------|-------------------|----------|
| **1** | How do weather conditions impact daily listings on the East Coast? | `impact_listings_by_weather.sql` |
| **2** | Does weather influence pricing? Are certain product types priced higher/lower during extreme weather? | `pricing_behavior_by_weather_and_product.sql` |
| **3** | What is the relationship between weather and shipping choices (free shipping, shipping cost)? | `shipping_choices_vs_weather.sql` |
| **4** | Are there shifts in product category demand under specific weather conditions? | `category_demand_shifts_by_weather.sql` |
| **5** | Do seller performance metrics correlate with weather-driven listing activity? | `seller_performance_vs_weather.sql` |
| **6** | (Bonus) Does weather impact listing quality (e.g., shorter titles, buying options)? | `bonus_listing_quality_vs_weather.sql` |
| **7** | (Bonus) Which US East Coast zip code prefixes show the largest price/availability variation with changing weather? | `bonus_zip_prefix_variation.sql` |

### Running Queries

#### Option 1: Python Script (Recommended)
Use the provided Python script to run queries against real data:

```bash
# Run all 7 queries at once
python sql_analysis/run_sql_queries.py

# Run individual queries by number (1-7)
python sql_analysis/run_sql_queries.py 1    # Weather impact on daily listings
python sql_analysis/run_sql_queries.py 2    # Pricing behavior by weather and product
python sql_analysis/run_sql_queries.py 3    # Shipping choices vs weather
python sql_analysis/run_sql_queries.py 4    # Category demand shifts by weather
python sql_analysis/run_sql_queries.py 5    # Seller performance vs weather
python sql_analysis/run_sql_queries.py 6    # Listing quality vs weather
python sql_analysis/run_sql_queries.py 7    # Zip prefix variation
```

**Benefits of Python Script:**
-  No database setup required** - Works with CSV data files
-  **Formatted output** - Clean, readable results
-  **Error handling** - Comprehensive validation and reporting
-  **Individual query execution** - Run specific queries by number
-  **Real data analysis** - Uses actual eBay and weather data

#### Option 2: PostgreSQL (Traditional)
```bash
# Run individual SQL files directly
psql -d your_database_name -f sql_queries/impact_listings_by_weather.sql
psql -d your_database_name -f sql_queries/pricing_behavior_by_weather_and_product.sql
psql -d your_database_name -f sql_queries/shipping_choices_vs_weather.sql
psql -d your_database_name -f sql_queries/category_demand_shifts_by_weather.sql
psql -d your_database_name -f sql_queries/seller_performance_vs_weather.sql
psql -d your_database_name -f sql_queries/bonus_listing_quality_vs_weather.sql
psql -d your_database_name -f sql_queries/bonus_zip_prefix_variation.sql
```

#### Option 3: Database GUI Tools
Copy and paste SQL content into:
- **pgAdmin** (PostgreSQL)
- **DBeaver** (Universal)
- **DataGrip** (JetBrains)
- **MySQL Workbench** (MySQL)

### Weather Buckets Used
- `Precipitation-Heavy`: `rain > 10` (mm)
- `Extreme Heat`: `temperature_2m >= 32` (°C)
- `Extreme Cold`: `temperature_2m <= -5` (°C)
- Otherwise `Normal`

### Query Results Summary
All queries have been validated against real API data:
- **Total queries**: 7
- **Success rate**: 100%
- **Data coverage**: 113 eBay records, 672 weather records
- **Production ready**: ✅ All queries provide accurate business insights

### Expected Results
When you run the queries, you should see:
- **Query 1**: 4 rows showing listings by weather condition (Extreme Cold: 7, Extreme Heat: 33, Normal: 29, Precipitation-Heavy: 44)
- **Query 2**: 9 rows showing pricing by product type and weather
- **Query 3**: 4 rows showing shipping analysis (Extreme Heat has 100% free shipping)
- **Query 4**: 9 rows showing product demand shifts (Umbrellas dominate during precipitation)
- **Query 5**: 29 rows showing seller performance by weather
- **Query 6**: 8 rows showing listing quality metrics (Normal weather produces highest quality)
- **Query 7**: 20 rows showing zip code price variation

### Quick Start Example
```bash
# Test with a single query first
python sql_analysis/run_sql_queries.py 3

# If successful, run all queries
python sql_analysis/run_sql_queries.py
```

## SQL Analysis Tools

The `sql_analysis/` folder contains comprehensive tools for analyzing the weather-eBay data:

### **Core Analysis Scripts**
- `run_sql_queries.py` - Main script to run individual or all SQL queries
- `data_analysis.py` - Comprehensive data structure analysis
- `validate_real_data_queries.py` - Validates SQL queries against real API data

### **Testing Framework**
- `test_runner.py` - Orchestrates the complete test suite
- `test_sql_pipeline.py` - Main test suite for schema validation and query execution
- `setup_database.py` - Creates star schema and loads sample data
- `quick_test.py` - Quick testing without database connection

### **Data Analysis**
```bash
# Analyze data structure
python sql_analysis/data_analysis.py

# Validate queries against real data
python sql_analysis/validate_real_data_queries.py
```

### **Testing**
```bash
# Quick test (no database required)
python sql_analysis/quick_test.py

# Full test suite (requires PostgreSQL)
python sql_analysis/test_runner.py
```

### **Requirements**
Install testing dependencies:
```bash
pip install -r sql_analysis/requirements_test.txt
```

For database testing, you'll also need:
- PostgreSQL installed and running
- Database credentials configured in `.env` file

## Troubleshooting

### Common Issues and Solutions

#### **Issue: "No module named 'pandas'" or similar import errors**
```bash
# Install required dependencies
pip install pandas numpy psycopg2-binary python-dotenv
```

#### **Issue: "File not found" errors**
- Ensure you're running commands from the project root directory
- Check that data files exist in `ebay_data/` and `weather_data/` folders

#### **Issue: Empty query results**
- This is normal for some queries due to limited data overlap
- The Python script handles this gracefully and shows available results

#### **Issue: NaN values in results**
- Some statistical calculations (like standard deviation) show NaN when there's only 1 record
- This is expected behavior and documented in the analysis

#### **Issue: PostgreSQL connection errors (if using Option 2)**
- Ensure PostgreSQL is installed and running
- Check database credentials in `.env` file
- Use Option 1 (Python script) instead for easier setup

#### **Issue: Permission errors on Windows**
- Run PowerShell as Administrator
- Or use the Python script which doesn't require database permissions

### Getting Help
1. **Check the logs**: The Python script provides detailed logging
2. **Run individual queries**: Use `python sql_analysis/run_sql_queries.py [1-7]` to test specific queries
3. **Verify data**: Check that `ebay_data/` and `weather_data/` folders contain CSV files
4. **Use the analysis tools**: Run `python sql_analysis/data_analysis.py` to validate your data

### Performance Tips
- **Use Python script**: Faster and more reliable than database setup
- **Run individual queries**: Test specific queries instead of all at once
- **Check data size**: Large datasets may take longer to process

