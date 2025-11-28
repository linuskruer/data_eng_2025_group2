## Project 3 – ClickHouse Security & Views

This folder contains all SQL needed to separate full vs. limited analytical access on top of the gold schema.

### Files
- `01_serving_views_full.sql` – creates `serving_views_full` database with rich views (`vw_listing_weather_full`, `vw_listing_kpis_full`) that expose raw business columns joined to weather context.
- `02_serving_views_masked.sql` – creates `serving_views_masked` database with views that pseudonymize at least three attributes (`price`, `seller_location`, `seller_feedback_percentage`) while keeping the same business KPIs.
- `03_roles_and_grants.sql` – defines `analyst_full` and `analyst_limited` roles, demo users, and grants. Feel free to change the sample passwords and users before deploying.
- `04_governance_demo.sql` – example queries showing the difference between full and masked views.
- `05_iceberg_s3_table.sql` – creates a ClickHouse S3 table engine to query Iceberg Parquet files directly from MinIO (read-only access).

### How to apply
```bash
# inside the ClickHouse container or via clickhouse-client
cat project3/clickhouse/01_serving_views_full.sql  | clickhouse-client --host clickhouse-server -n
cat project3/clickhouse/02_serving_views_masked.sql | clickhouse-client --host clickhouse-server -n
cat project3/clickhouse/03_roles_and_grants.sql    | clickhouse-client --host clickhouse-server -n
cat project3/clickhouse/05_iceberg_s3_table.sql     | clickhouse-client --host clickhouse-server -n
```

**Note**: The Iceberg S3 table (`05_iceberg_s3_table.sql`) requires:
1. Iceberg data to be loaded first (run `weather_data_ingestion` DAG)
2. MinIO bucket `iceberg-bronze` to exist
3. Network connectivity from ClickHouse to MinIO (`project3-minio:9000`)

### Verifying access
```sql
-- Full role sees raw data
SET ROLE analyst_full;
SELECT seller_location, price
FROM serving_views_full.vw_listing_weather_full
LIMIT 5;

-- Limited role sees masked fields
SET ROLE analyst_limited;
SELECT seller_location_masked, price_bucket
FROM serving_views_masked.vw_listing_weather_masked
LIMIT 5;
```

The limited role can produce the same KPIs (counts, averages, weather joins) but the sensitive columns remain bucketed or hashed, satisfying the Project 3 masking requirement.

