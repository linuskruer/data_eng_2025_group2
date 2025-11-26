## Project 3 – Data Governance & Visualization

This directory isolates all new assets for the Project 3 scope so reviewers can find the governance/visualization work without digging through legacy folders.

### Layout
- `docker/` – docker-compose and helper scripts used to add MinIO (Iceberg storage), OpenMetadata, and Superset alongside Airflow/ClickHouse.
- `airflow/` – DAGs/operators/config specific to the Iceberg ingestion (bronze layer) and any Project 3 pipelines.
- `clickhouse/` – SQL for roles, grants, masking logic, and schema/view DDLs (`serving_views_full`, `serving_views_masked`).
- `metadata/` – OpenMetadata ingestion configs, data quality test definitions, and the captured screenshots/reports.
- `superset/` – dashboard export JSON (if used) plus dashboard screenshot(s).
- `docs/` – README excerpts, Report.pdf draft, and any supplemental notes for Project 3 deliverables.

> Implementation order: extend docker stack → add Iceberg ingestion → define ClickHouse roles/views → wire OpenMetadata + Superset → document and capture evidence.

### Evidence snapshots
- **Airflow – weather pipeline success**: `screenshots/airflow-weather-data-ingestion-graph.png` shows the full `weather_data_ingestion` DAG (fetch → validate → Iceberg → ClickHouse → dbt silver/gold/tests) with all tasks green in the latest run.
- **Airflow – run history overview**: `screenshots/airflow-weather-data-ingestion-runs.png` shows the run list with the most recent execution succeeded.

### ClickHouse data checkpoints Project 3
- Core gold/silver tables in `default` (from `SELECT table, sum(rows) AS row_count FROM system.parts ...`):  
  - `fact_listings` – **13,496** rows  
  - `fact_weather` – **1,200** rows  
  - `silver_ebay_listings` – **13,496** rows  
  - `silver_weather` – **1,200** rows  
  - plus fully populated dimensions (`dim_*`) and `bronze_weather`.
- Superset-facing views (from `SELECT count(*)` per view):  
  - `serving_views_full.vw_listing_weather_full` – **68,305** rows  
  - `serving_views_full.vw_listing_kpis_full` – **374** rows  
  - `serving_views_masked.vw_listing_weather_masked` – **68,305** rows  
  - `serving_views_masked.vw_listing_kpis_masked` – **472** rows  
These counts demonstrate that both full and masked schemas are populated and ready to be queried from Superset and OpenMetadata.

### Runtime notes
- Superset admin is seeded manually via:
  ```bash
  docker exec -it project3-superset bash -lc "superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin123 && superset db upgrade"
  docker exec project3-superset superset init
  docker restart project3-superset
  ```
  Credentials: `admin / admin123` (update if you change the command above).
- Airflow admin is created automatically (`admin / admin`) during `airflow-init`.
- When bringing the stack up, use the env-file flag so the Project 3 variables are available:  
  `docker compose --env-file project3/docker/project3.env -f project/airflow/compose.yml -f project3/docker/docker-compose.project3.yml up -d`.

