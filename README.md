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
| **1** | How do weather conditions impact daily listings on the East Coast? | `impact_listings_by_weather_fixed.sql` |
| **2** | Does weather influence pricing? Are certain product types priced higher/lower during extreme weather? | `pricing_behavior_by_weather_and_product_fixed.sql` |
| **3** | What is the relationship between weather and shipping choices (free shipping, shipping cost)? | `shipping_choices_vs_weather_fixed.sql` |
| **4** | Are there shifts in product category demand under specific weather conditions? | `category_demand_shifts_by_weather_fixed.sql` |
| **5** | Do seller performance metrics correlate with weather-driven listing activity? | `seller_performance_vs_weather_fixed.sql` |
| **6** | (Bonus) Does weather impact listing quality (e.g., shorter titles, buying options)? | `bonus_listing_quality_vs_weather_fixed.sql` |
| **7** | (Bonus) Which US East Coast zip code prefixes show the largest price/availability variation with changing weather? | `bonus_zip_prefix_variation_fixed.sql` |

### Running Queries

#### Option 1: Python Script (Recommended)
Use the provided Python script to run queries against real data:

```bash
# Run all 7 queries
python sql_analysis/run_sql_queries.py

# Run individual queries
python sql_analysis/run_sql_queries.py 1    # Weather impact on daily listings
python sql_analysis/run_sql_queries.py 2    # Pricing behavior by weather and product
python sql_analysis/run_sql_queries.py 3    # Shipping choices vs weather
python sql_analysis/run_sql_queries.py 4    # Category demand shifts by weather
python sql_analysis/run_sql_queries.py 5    # Seller performance vs weather
python sql_analysis/run_sql_queries.py 6    # Listing quality vs weather
python sql_analysis/run_sql_queries.py 7    # Zip prefix variation
```

#### Option 2: PostgreSQL (Traditional)
```powershell
# Windows PowerShell example
psql -v ON_ERROR_STOP=1 -f "sql_queries\impact_listings_by_weather_fixed.sql"
psql -v ON_ERROR_STOP=1 -f "sql_queries\pricing_behavior_by_weather_and_product_fixed.sql"
psql -v ON_ERROR_STOP=1 -f "sql_queries\shipping_choices_vs_weather_fixed.sql"
psql -v ON_ERROR_STOP=1 -f "sql_queries\category_demand_shifts_by_weather_fixed.sql"
psql -v ON_ERROR_STOP=1 -f "sql_queries\seller_performance_vs_weather_fixed.sql"
psql -v ON_ERROR_STOP=1 -f "sql_queries\bonus_listing_quality_vs_weather_fixed.sql"
psql -v ON_ERROR_STOP=1 -f "sql_queries\bonus_zip_prefix_variation_fixed.sql"
```
Or use a GUI (DBeaver/pgAdmin) and open each file.

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

## Troubleshooting
- Column not found (e.g., `w.date`):
  - Align to actual names, e.g., use `w.date_key` if that’s weather date column.
- No `location_key`:
  - Create a lookup table mapping `location` text to a surrogate key and reference it from both weather and listings.
- Empty result sets:
  - Validate join sanity:
```sql
SELECT COUNT(*)
FROM fact_listings f
JOIN dim_location l ON f.location_key = l.location_key
JOIN weather_daily w ON w.location_key = f.location_key AND w.date = f.date_key;
```

