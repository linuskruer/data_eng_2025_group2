import clickhouse_connect
import pandas as pd
import os
import glob

def load_weather_to_clickhouse(file_version: str, execution_date: str = None, **kwargs):
    """
    Load validated weather data from CSV into ClickHouse (Bronze layer).

    Parameters:
        file_version (str): Version of the file, e.g., 'v1'
        execution_date (str, optional): Date string 'YYYY-MM-DD'; if None, pick latest file
    """
    data_dir = "/opt/airflow/dags/data"

    # Build file path pattern
    if execution_date:
        pattern = os.path.join(data_dir, f"east_coast_weather_{file_version}_{execution_date}.csv")
        files = [pattern] if os.path.exists(pattern) else []
    else:
        # Pick latest CSV matching file_version
        pattern = os.path.join(data_dir, f"east_coast_weather_{file_version}_*.csv")
        files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"❌ No weather file found for version '{file_version}' in {data_dir}")

    # Pick the latest file
    file_path = max(files, key=os.path.getmtime)

    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError(f"❌ Weather data file is empty: {file_path}")

    # Rename columns to match ClickHouse table
    rename_map = {
        "weather_code (wmo code)": "weather_code",
        "temperature_2m (°C)": "temperature_2m",
        "relative_humidity_2m (%)": "relative_humidity_2m",
        "cloudcover (%)": "cloudcover",
        "rain (mm)": "rain",
        "sunshine_duration (s)": "sunshine_duration",
        "windspeed_10m (km/h)": "windspeed_10m",
        "postal_code": None,      # drop this
        "location_id": None       # drop this
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if v is not None})

    # Keep only expected columns
    expected_columns = [
        "time", "weather_code", "temperature_2m", "relative_humidity_2m",
        "cloudcover", "rain", "sunshine_duration", "windspeed_10m",
        "city", "postal_prefix"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # Add file_version column
    df["file_version"] = str(file_version)

    # Convert 'time' column to DateTime
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])

    # Convert numeric columns
    numeric_columns = [
        "weather_code", "temperature_2m", "relative_humidity_2m",
        "cloudcover", "rain", "sunshine_duration", "windspeed_10m"
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert string columns
    for col in ["city", "postal_prefix", "file_version"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host="clickhouse-server",
        port=8123,
        username="default",
        password="mypassword"
    )

    # Create Bronze table if it doesn't exist
    client.command("""
        CREATE TABLE IF NOT EXISTS bronze_weather (
            time DateTime,
            weather_code UInt16,
            temperature_2m Float32,
            relative_humidity_2m Float32,
            cloudcover Float32,
            rain Float32,
            sunshine_duration Float32,
            windspeed_10m Float32,
            city String,
            postal_prefix String,
            file_version String,
            ingestion_timestamp DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (time, city)
    """)

    # Insert into ClickHouse
    client.insert_df("bronze_weather", df)
    print(f"✅ Weather data loaded into ClickHouse from {file_path}")
