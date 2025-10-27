# eBay → ClickHouse (Bronze) Airflow on Kubernetes

## What this does

- Runs an **hourly Airflow DAG** that executes `/opt/airflow/scripts/all_in_one.py`.
- The script fetches **public eBay listings** and **inserts rows** into **ClickHouse** table `bronze.ebay_raw_data` (flat columns).
- Weather ingestion, dbt, and ClickHouse server are **not** part of this deployment.

## Container build & push (example: GitLab registry)

```bash
# from repo root
docker build -t gitlab.cs.ut.ee/<GROUP>/<PROJECT>/ebay-airflow:latest .
docker push gitlab.cs.ut.ee/<GROUP>/<PROJECT>/ebay-airflow:latest
```
