# Project 3 – Data Governance & Visualization

This README provides step-by-step instructions to run and verify Project 3 deliverables.

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available
- Git repository cloned

## Environment Variables

The project uses environment variables defined in `project3/docker/project3.env`. This file contains:
- MinIO credentials and bucket configuration
- Iceberg catalog settings
- Superset database credentials
- OpenMetadata database credentials
- ClickHouse connection settings

**Note**: The environment file is already configured with default values. No manual setup required unless you need to customize settings.

## Dependencies

All dependencies are managed through Docker containers:
- **Airflow**: Python dependencies in `project/airflow/requirements.txt`
- **Iceberg**: PyIceberg library (installed via `PythonVirtualenvOperator` in Airflow DAG)
- **ClickHouse**: Native ClickHouse server (no additional dependencies)
- **OpenMetadata**: Java-based service (no Python dependencies needed)
- **Superset**: Python dependencies included in Docker image

## Step 1: Start All Services

Start all containers (Airflow, ClickHouse, MinIO, OpenMetadata, Superset):

```powershell
docker compose --env-file project3/docker/project3.env `
  -f project/airflow/compose.yml `
  -f project3/docker/docker-compose.project3.yml up -d
```

**Wait for services to be ready** (2-3 minutes):
- Airflow: http://localhost:8080 (admin/admin)
- ClickHouse: http://localhost:18123 (default/mypassword)
- MinIO: http://localhost:9001 (project3admin/project3admin123)
- OpenMetadata: http://localhost:8585 (admin@project3.local/project3_admin)
- Superset: http://localhost:8088 (admin/admin123)

---

## Step 2: Verify Apache Iceberg Table Creation

### What is the Iceberg Table?

- **Table Name**: `project3_bronze.weather_hourly`
- **Purpose**: Serves as the bronze (landing) layer for weather data in Apache Iceberg format
- **Storage Location**: MinIO S3 bucket `iceberg-bronze` at path `warehouse/project3_bronze.db/weather_hourly/data/`
- **Format**: Parquet files stored in S3-compatible object storage

### How is it Populated?

The `weather_data_ingestion` Airflow DAG (located in `project/airflow/dags/weather_ingestion_dag.py`) automatically populates the Iceberg table:

1. **Task**: `load_weather_to_iceberg` (uses `PythonVirtualenvOperator`)
2. **Implementation**: `project/airflow/dags/utils/iceberg_loader.py` → `load_weather_to_iceberg()` function
3. **Process**:
   - Reads weather CSV files from `/opt/airflow/dags/data/`
   - Normalizes column names and data types
   - Writes to Iceberg table using PyIceberg library
   - Stores Parquet files in MinIO S3 via the Iceberg catalog

**DAG Flow**:
```
fetch_weather → validate_weather → load_weather_to_iceberg → load_weather_to_clickhouse → dbt transformations
```

### How to Query It?

ClickHouse can query Iceberg data directly (read-only) using an S3 table engine:
- **Table**: `iceberg_readonly.weather_hourly_s3` (created by `clickhouse/05_iceberg_s3_table.sql`)
- **Query Example**: `SELECT COUNT(*) FROM iceberg_readonly.weather_hourly_s3;`
- This demonstrates that Iceberg serves as archival storage while ClickHouse can query it on-demand without duplicating data

### 2.1 Check Airflow DAG Execution

1. Open Airflow UI: http://localhost:8080
2. Navigate to **DAGs** → `weather_data_ingestion`
3. Verify the `load_weather_to_iceberg` task completed successfully
4. **Evidence**: ![Airflow DAG Graph](screenshots\latest-dag.png)

### 2.2 Verify Iceberg Table in MinIO

1. Open MinIO Console: http://localhost:9001
2. Login: `project3admin / project3admin123`
3. Navigate to bucket `iceberg-bronze` → `warehouse/project3_bronze.db/weather_hourly/data/`
4. You should see `.parquet` files
5. **Evidence**: ![MinIO Iceberg Bucket](screenshots/minio-iceberg-bucket.png)

### 2.3 Verify ClickHouse Can Query Iceberg Data

Run the SQL script to create the S3 table engine:

```powershell
Get-Content project3/clickhouse/05_iceberg_s3_table.sql | docker exec -i clickhouse-server clickhouse-client --multiquery
```

Verify it works:

```powershell
docker exec -it clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM iceberg_readonly.weather_hourly_s3"
```

Expected: Returns row count > 0

---

## Step 3: Apply ClickHouse Roles and Grants

### 3.1 Create Full and Masked Views

```powershell
Get-Content project3/clickhouse/01_serving_views_full.sql | docker exec -i clickhouse-server clickhouse-client --multiquery
Get-Content project3/clickhouse/02_serving_views_masked.sql | docker exec -i clickhouse-server clickhouse-client --multiquery
```

### 3.2 Create Roles and Users

```powershell
Get-Content project3/clickhouse/03_roles_and_grants.sql | docker exec -i clickhouse-server clickhouse-client --multiquery
```

### 3.3 Verify Roles and Grants

Show roles:
```powershell
docker exec -it clickhouse-server clickhouse-client --user default --password mypassword --query "SHOW ROLES"
```

Show users:
```powershell
docker exec -it clickhouse-server clickhouse-client --user default --password mypassword --query "SHOW USERS"
```

Show grants for full user:
```powershell
docker exec -it clickhouse-server clickhouse-client --user default --password mypassword --query "SHOW GRANTS FOR analyst_full_user"
```

### 3.4 Verify Data Masking

Query full view (unmasked data):
```powershell
docker exec -it clickhouse-server clickhouse-client --user analyst_full_user --password Project3Full! --query "SELECT item_id, price, seller_feedback_percentage, seller_location, zip_prefix FROM serving_views_full.vw_listing_weather_full LIMIT 5"
```

Query masked view (pseudonymized data):
```powershell
docker exec -it clickhouse-server clickhouse-client --user analyst_limited_user --password Project3Limited! --query "SELECT listing_surrogate, price_bucket, seller_feedback_band, seller_location_masked, zip_prefix_masked FROM serving_views_masked.vw_listing_weather_masked LIMIT 5"
```

**Evidence**: 
- Full view: ![ClickHouse Full View](screenshots/clickhouse-governance-full-view.png)
- Masked view: ![ClickHouse Masked View](screenshots/clickhouse-governance-masked-view.png)
- Roles/Grants: ![ClickHouse Roles 1](screenshots/clickhouse-roles-grants-1.png) ![ClickHouse Roles 2](screenshots/clickhouse-roles-grants-2.png)

---

## Step 4: OpenMetadata Setup

### 4.1 Register ClickHouse Service

1. Open OpenMetadata UI: http://localhost:8585
2. Login: `admin@project3.local / project3_admin`
3. Navigate to **Settings** → **Services** → **Databases**
4. Click **Add Service** → Select **ClickHouse**
5. Fill in connection details:
   - **Service Name**: `clickhouse_project3`
   - **Host**: `clickhouse-server`
   - **Port**: `9000`
   - **Username**: `default`
   - **Password**: `mypassword`
   - **Database**: `default`
6. Click **Save**

**Evidence**: ![OpenMetadata ClickHouse Service](screenshots/openmetadata-clickhouse-tables-registered-1.png)

### 4.2 Run Metadata Ingestion

```powershell
docker exec -it project3-openmetadata-ingestion metadata ingest -c /metadata/clickhouse_ingestion.yml
```

This registers all ClickHouse tables and views in OpenMetadata.

**Evidence**: ![OpenMetadata Tables Registered](screenshots/openmetadata-clickhouse-tables-registered-2.png) 

### 4.3 Add Table and Column Descriptions

**Evidence**: ![Table and Column Descriptions](screenshots/fact-table-desc-column-desc.png) ![Table and Column Descriptions 1](screenshots/fact-table-desc-column-desc-1.png) ![Table and Column Descriptions 2](screenshots/fact-table-desc-column-desc-2.png) ![Table and Column Descriptions 3](screenshots/fact-table-desc-column-desc-3.png)

### 4.4 Create Data Quality Tests

**Test 1: Foreign Key Not Null**
- **Name**: `fact_listings_product_type_not_null`
- **SQL**: `SELECT item_id FROM default.fact_listings WHERE product_type IS NULL`
- **Expected**: 0 rows (PASS)

**Test 2: Surrogate Key Unique** (on `dim_product`)
- Navigate to `default.dim_product`
- **Name**: `dim_product_product_type_unique`
- **SQL**: `SELECT product_type, COUNT(*) FROM default.dim_product GROUP BY product_type HAVING COUNT(*) > 1`
- **Expected**: 0 rows (PASS)

**Test 3: Feedback Percentage Range** (on `fact_listings`)
- **Name**: `fact_listings_feedback_percentage_range`
- **SQL**: `SELECT item_id FROM default.fact_listings WHERE seller_feedback_percentage < 0 OR seller_feedback_percentage > 100`
- **Expected**: 0 rows (PASS)

- Test 1: ![Test 1 - Foreign Key Not Null](screenshots/test-1.png)
- Test 2: ![Test 2 - Surrogate Key Unique](screenshots/test2-surrogate-key-1.png) ![Test 2 - Surrogate Key Unique 1](screenshots/test2-surrogate-key-2.png) ![Test 2 - Surrogate Key Unique 2](screenshots/test2-surrogate-key-3.png)
- Test 3: ![Test 3 - Feedback Percentage Range](screenshots/test-3.png)
- All tests for fact_listings table: ![All Tests Summary](screenshots/test-fact-listings-all.png)

### 4.5 Register Superset Dashboard Service

1. Navigate to **Settings** → **Services** → **Dashboards**
2. Click **Add Service** → Select **Superset**
3. Fill in connection details:
   - **Service Name**: `superset_project3`
   - **Host**: `http://project3-superset:8088`
   - **Username**: `admin`
   - **Password**: `admin123`
   - **Provider**: `db`
   - **Verify SSL**: `no-ssl`
4. Click **Save**

**Evidence**: ![Superset Dashboard Service](screenshots/openmetadata-superset-dashboard.png)

---

## Step 5: Superset Dashboard Setup

### 5.1 Initialize Superset (if not already done)

```powershell
docker exec -it project3-superset bash -lc "superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin123 && superset db upgrade"
docker exec project3-superset superset init
docker restart project3-superset
```

### 5.2 Connect to ClickHouse

1. Open Superset UI: http://localhost:8088
2. Login: `admin / admin123`
3. Navigate to **Settings** → **Database Connections**
4. Click **+ Database** → Select **ClickHouse**
5. Fill in connection string:
   ```
   clickhouse://default:mypassword@clickhouse-server:9000/default
   ```
   Or use individual fields:
   - **Host**: `clickhouse-server`
   - **Port**: `8123`
   - **Username**: `default`
   - **Password**: `mypassword`
   - **Database**: `default`
6. Click **Connect**

**Evidence**: ![Superset ClickHouse Connection](screenshots/superset-clickhouse-connection.png)

### 5.3 Dashboard


- Dashboard view 1: ![Superset Dashboard 1](screenshots/superset-dashboard-full-1.png)
- Dashboard view 2: ![Superset Dashboard 2](screenshots/superset-dashboard-full-2.png)
- Filters active: ![Superset Filters](screenshots/superset-filters-active.png)

---

## SQL Files Reference

### ClickHouse Roles and Grants

**File**: `project3/clickhouse/03_roles_and_grants.sql`

This file creates:
- Two roles: `analyst_full` and `analyst_limited`
- Two users: `analyst_full_user` and `analyst_limited_user`
- Grants appropriate permissions to each role

**Key SQL**:
```sql
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

GRANT SELECT ON serving_views_full.* TO analyst_full;
GRANT SELECT ON default.* TO analyst_full;

GRANT SELECT ON serving_views_masked.* TO analyst_limited;
GRANT SELECT ON serving_views_full.* TO analyst_limited;
GRANT SELECT ON default.* TO analyst_limited;

CREATE USER IF NOT EXISTS analyst_full_user IDENTIFIED WITH plaintext_password BY 'Project3Full!';
CREATE USER IF NOT EXISTS analyst_limited_user IDENTIFIED WITH plaintext_password BY 'Project3Limited!';

GRANT analyst_full TO analyst_full_user;
GRANT analyst_limited TO analyst_limited_user;
```

### Data Masking Logic

**File**: `project3/clickhouse/02_serving_views_masked.sql`

This file creates masked views with pseudonymization:
- **Hashing**: `cityHash64(item_id)` for listing IDs
- **Bucketing**: `ROUND(price, -1)` for price values (rounds to nearest 10)
- **Banding**: `multiIf(...)` for feedback percentage (LOW/MODERATE/HIGH/EXCELLENT)
- **Location Masking**: `substring(hex(cityHash64(...)), 1, 12)` for seller locations
- **Zip Prefixing**: `concat('ZIP_', ...)` for zip codes

**Key SQL**:
```sql
CREATE OR REPLACE VIEW serving_views_masked.vw_listing_weather_masked AS
SELECT
    cityHash64(item_id) AS listing_surrogate,
    ROUND(price, -1) AS price_bucket,
    multiIf(
        seller_feedback_percentage < 90, 'LOW_CONFIDENCE',
        seller_feedback_percentage < 95, 'MODERATE',
        seller_feedback_percentage < 98, 'HIGH',
        'EXCELLENT'
    ) AS seller_feedback_band,
    substring(hex(cityHash64(coalesce(seller_location, 'UNKNOWN'))), 1, 12) AS seller_location_masked,
    concat('ZIP_', coalesce(zip_prefix, 'UNK')) AS zip_prefix_masked,
    ...
FROM serving_views_full.vw_listing_weather_full;
```

### Full SQL Files

- **Full Views**: `project3/clickhouse/01_serving_views_full.sql` - Unmasked analytical views
- **Masked Views**: `project3/clickhouse/02_serving_views_masked.sql` - Pseudonymized views with masking logic
- **Roles & Grants**: `project3/clickhouse/03_roles_and_grants.sql` - RBAC setup
- **Iceberg S3 Table**: `project3/clickhouse/05_iceberg_s3_table.sql` - ClickHouse S3 table engine for Iceberg

---

## Key Files Reference

- **Iceberg Loader**: `project/airflow/dags/utils/iceberg_loader.py`
- **Iceberg DAG**: `project/airflow/dags/weather_ingestion_dag.py`
- **OpenMetadata Configs**: `project3/metadata/clickhouse_ingestion.yml`, `clickhouse_fact_tests.yml`, `clickhouse_dim_tests.yml`

---

## Access Credentials

| Service | URL | Username | Password |
|---------|-----|----------|----------|
| Airflow | http://localhost:8080 | admin | admin |
| ClickHouse | http://localhost:18123 | default | mypassword |
| MinIO | http://localhost:9001 | project3admin | project3admin123 |
| OpenMetadata | http://localhost:8585 | admin@project3.local | project3_admin |
| Superset | http://localhost:8088 | admin | admin123 |
