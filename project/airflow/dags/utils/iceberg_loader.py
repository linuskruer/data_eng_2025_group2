import os
from datetime import datetime
from pathlib import Path
from typing import Tuple

DATA_DIR = Path("/opt/airflow/dags/data")
CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "project3_catalog")
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "project3_bronze")
TABLE_NAME = os.environ.get("ICEBERG_TABLE_NAME", "weather_hourly")
TABLE_IDENTIFIER = (NAMESPACE, TABLE_NAME)

COLUMN_ORDER = [
    "event_time",
    "event_date",
    "city",
    "location_id",
    "postal_code",
    "postal_prefix",
    "weather_code",
    "temperature_2m",
    "relative_humidity_2m",
    "cloudcover",
    "rain",
    "sunshine_duration",
    "windspeed_10m",
    "ingested_at",
]

COLUMN_RENAMES = {
    "weather_code (wmo code)": "weather_code",
    "temperature_2m (°C)": "temperature_2m",
    "relative_humidity_2m (%)": "relative_humidity_2m",
    "cloudcover (%)": "cloudcover",
    "rain (mm)": "rain",
    "sunshine_duration (s)": "sunshine_duration",
    "windspeed_10m (km/h)": "windspeed_10m",
}


def load_weather_to_iceberg(file_version="v1", execution_date=None, **_: dict) -> str:
    import pandas as pd
    import pyarrow as pa

    """
    Load the most recent weather CSV into an Apache Iceberg table stored in MinIO.

    Returns the object path that was appended so downstream tasks can reference it.
    """
    csv_path = _resolve_weather_file(file_version=file_version, execution_date=execution_date)
    if not csv_path:
        raise FileNotFoundError("No weather CSV found to load into Iceberg.")

    df = pd.read_csv(csv_path)
    df = _normalize_columns(df)
    if df.empty:
        raise ValueError(f"{csv_path} contains no rows to write into Iceberg.")

    df["event_time"] = pd.to_datetime(df["time"])
    df["event_date"] = df["event_time"].dt.date
    df["ingested_at"] = datetime.utcnow()

    schema = _build_schema()
    missing = [col for col in COLUMN_ORDER if col not in df.columns]
    if missing:
        raise KeyError(f"Columns {missing} missing from weather dataset {csv_path}.")

    cleaned_df = df[COLUMN_ORDER].copy()

    # Align pandas dtypes with the Iceberg schema so PyArrow/Parquet writer
    # sees exactly the same logical types.
    cleaned_df["location_id"] = cleaned_df["location_id"].astype("int32")
    cleaned_df["weather_code"] = cleaned_df["weather_code"].astype("int32")
    cleaned_df["postal_code"] = cleaned_df["postal_code"].astype("string")
    cleaned_df["postal_prefix"] = cleaned_df["postal_prefix"].astype("string")
    for col in ["relative_humidity_2m", "cloudcover"]:
        cleaned_df[col] = cleaned_df[col].astype("float64")
    cleaned_df["event_time"] = pd.to_datetime(cleaned_df["event_time"]).astype("datetime64[us]")
    cleaned_df["ingested_at"] = pd.to_datetime(cleaned_df["ingested_at"]).astype("datetime64[us]")

    arrow_table = pa.Table.from_pandas(cleaned_df, preserve_index=False)
    # Drop pandas-specific schema metadata so it matches the Parquet writer schema
    arrow_table = arrow_table.replace_schema_metadata(None)

    catalog = _load_catalog()
    table = _ensure_table(catalog, schema)
    table.append(arrow_table)

    return str(csv_path)


def _resolve_weather_file(file_version: str, execution_date):
    if execution_date:
        expected_name = f"east_coast_weather_{file_version}_{execution_date}.csv"
        candidate = DATA_DIR / expected_name
        if candidate.exists():
            return candidate

    csv_files = sorted(
        DATA_DIR.glob(f"east_coast_weather_{file_version}_*.csv"),
        reverse=True,
    )
    return csv_files[0] if csv_files else None


def _load_catalog():
    from pyiceberg.catalog import load_catalog

    warehouse_uri = os.environ.get("ICEBERG_WAREHOUSE_URI")
    catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "sqlite:////opt/airflow/iceberg/catalog.db")
    if not warehouse_uri:
        raise EnvironmentError("ICEBERG_WAREHOUSE_URI is not configured.")

    catalog_config = {
        "type": os.environ.get("ICEBERG_CATALOG_TYPE", "sql"),
        "uri": catalog_uri,
        "warehouse": warehouse_uri,
        "s3.endpoint": os.environ.get("ICEBERG_S3_ENDPOINT", "http://minio:9000"),
        "s3.access-key-id": os.environ.get("ICEBERG_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.environ.get("ICEBERG_S3_SECRET_KEY"),
        "s3.region": os.environ.get("ICEBERG_S3_REGION", "us-east-1"),
    }

    return load_catalog(CATALOG_NAME, **catalog_config)


def _ensure_table(catalog, schema):
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, NamespaceAlreadyExistsError

    identifier: Tuple[str, str] = TABLE_IDENTIFIER

    try:
        table = catalog.load_table(identifier)
        # If the on-disk schema differs (e.g. from earlier experiments), drop
        # and recreate the table so the writer schema matches exactly.
        if table.schema != schema:
            catalog.drop_table(identifier)
            return catalog.create_table(identifier=identifier, schema=schema)
        return table
    except NoSuchTableError:
        # Ensure namespace/database exists for SqlCatalog
        try:
            catalog.create_namespace((NAMESPACE,))
        except (NoSuchNamespaceError, NamespaceAlreadyExistsError):
            # Namespace was just created or already exists; safe to ignore
            pass
        return catalog.create_table(identifier=identifier, schema=schema)


def _normalize_columns(df):
    """Align column names with the schema regardless of unit suffixes from Open-Meteo."""
    df = df.rename(columns=COLUMN_RENAMES)
    # Some CSV exports include spaces after the unit, so normalize once more.
    df.columns = [col.strip() for col in df.columns]
    return df


def _build_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DateType,
        DoubleType,
        IntegerType,
        NestedField,
        StringType,
        TimestampType,
    )

    # Make all fields nullable (required=False) so the Parquet writer schema
    # matches the DataFrame/Arrow schema and does not enforce NOT NULL.
    return Schema(
        NestedField(1, "event_time", TimestampType(), required=False),
        NestedField(2, "event_date", DateType(), required=False),
        NestedField(3, "city", StringType(), required=False),
        NestedField(4, "location_id", IntegerType(), required=False),
        NestedField(5, "postal_code", StringType(), required=False),
        NestedField(6, "postal_prefix", StringType(), required=False),
        NestedField(7, "weather_code", IntegerType(), required=False),
        NestedField(8, "temperature_2m", DoubleType(), required=False),
        NestedField(9, "relative_humidity_2m", DoubleType(), required=False),
        NestedField(10, "cloudcover", DoubleType(), required=False),
        NestedField(11, "rain", DoubleType(), required=False),
        NestedField(12, "sunshine_duration", DoubleType(), required=False),
        NestedField(13, "windspeed_10m", DoubleType(), required=False),
        NestedField(14, "ingested_at", TimestampType(), required=False),
    )

