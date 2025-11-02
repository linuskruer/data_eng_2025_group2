import requests
import os
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

def fetch_weather_all_cities(execution_date=None, file_version="v1", **kwargs):
    """
    Fetch hourly weather for multiple East Coast cities and save as a single CSV.
    Adds postal_prefix and location_id for merging with eBay data.
    """
    cities = [
        {"name": "New_York", "lat": 40.71, "lon": -74.01, "postal_code": "10001", "location_id": 1},
        {"name": "Philadelphia", "lat": 39.95, "lon": -75.17, "postal_code": "19104", "location_id": 2},
        {"name": "Boston", "lat": 42.36, "lon": -71.06, "postal_code": "02108", "location_id": 3},
        {"name": "Jacksonville", "lat": 30.33, "lon": -81.65, "postal_code": "32202", "location_id": 4},
        {"name": "Miami", "lat": 25.77, "lon": -80.19, "postal_code": "33101", "location_id": 5},
    ]

    run_date = datetime.strptime(execution_date, "%Y-%m-%d") if execution_date else datetime.now()
    target_date = run_date - timedelta(days=1)

    start_date = target_date.strftime("%Y-%m-%d")
    end_date = start_date  # same day, since we only want one day's data
    
    # Use execution_date (which is the run date) for filename consistency
    file_date = run_date.strftime("%Y-%m-%d")

    print(f"Fetching weather for {start_date} (version {file_version})")

    hourly_vars = [
        "weather_code",
        "temperature_2m",
        "relative_humidity_2m",
        "cloudcover",
        "rain",
        "sunshine_duration",
        "windspeed_10m"
    ]

    data_dir = "/opt/airflow/dags/data"
    os.makedirs(data_dir, exist_ok=True)

    all_weather_data = []

    for city in cities:
        print(f"Fetching weather for {city['name']}...")
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(hourly_vars),
            "timezone": "America/New_York",
            "format": "csv"
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"❌ Failed for {city['name']}")
            continue

        text = response.text.strip().splitlines()

        # Find the header line (starts with "time,")
        header_index = next(
            (i for i, line in enumerate(text) if line.startswith("time,")), None
        )
        if header_index is None:
            print(f"⚠️ No valid CSV header found for {city['name']}.")
            print(response.text[:500])
            continue

        # Keep only the real CSV data
        csv_data = "\n".join(text[header_index:])
        df = pd.read_csv(StringIO(csv_data))

        df["postal_code"] = city["postal_code"]
        df["postal_prefix"] = city["postal_code"][:3]
        df["location_id"] = city["location_id"]
        df["city"] = city["name"]

        all_weather_data.append(df)

    if all_weather_data:
        combined_df = pd.concat(all_weather_data, ignore_index=True)

        # --- Save with version + date ---
        output_file = os.path.join(data_dir, f"east_coast_weather_{file_version}_{file_date}.csv")
        
        # Remove existing file if it exists (for idempotency)
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
                print(f"⚠️ Removed existing file: {output_file}")
            except Exception as e:
                print(f"⚠️ Warning: Could not remove existing file: {e}")
        
        # Ensure directory exists and has write permissions
        os.makedirs(data_dir, exist_ok=True)
        os.chmod(data_dir, 0o777)  # Ensure write permissions
        
        try:
            combined_df.to_csv(output_file, index=False)
            print(f"✅ Saved weather data → {output_file}")
            print(f"Combined weather data saved to {output_file}")
        except PermissionError as e:
            print(f"❌ Permission error: {e}")
            print(f"Directory permissions: {oct(os.stat(data_dir).st_mode)}")
            raise
    else:
        print("No data was fetched for any city.")

def validate_weather_data(**kwargs):
    """
    Validate the weather dataset:
    - No nulls in essential columns
    - No duplicate rows
    - Temperature range sanity check
    """
    data_dir = "/opt/airflow/dags/data"
    # find the latest file
    files = sorted(
        [f for f in os.listdir(data_dir) if f.startswith("east_coast_weather")],
        reverse=True
    )
    if not files:
        raise ValueError("No weather data files found for validation.")

    latest_file = os.path.join(data_dir, files[0])
    df = pd.read_csv(latest_file)

    # 2️⃣ Duplicate check
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        raise ValueError(f"Found {duplicate_count} duplicate rows in {latest_file}")

    # 3️⃣ Null checks in essential columns
    essential_columns = ['time', 'weather_code', 'temperature_2m', 'city']
    null_counts = {}
    for col in essential_columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                null_counts[col] = null_count
    
    if null_counts:
        raise ValueError(f"Found nulls in essential columns: {null_counts}")

    # 4️⃣ Range checks
    if 'temperature_2m' in df.columns:
        # Reasonable temperature range for East Coast US: -40°C to 50°C
        temp_outliers = df[(df['temperature_2m'] < -40) | (df['temperature_2m'] > 50)]
        if len(temp_outliers) > 0:
            raise ValueError(f"Found {len(temp_outliers)} temperature values outside valid range (-40 to 50°C)")
    
    if 'rain' in df.columns:
        # Rain should be >= 0
        negative_rain = df[df['rain'] < 0]
        if len(negative_rain) > 0:
            raise ValueError(f"Found {len(negative_rain)} negative rain values")

    print(f"✅ Data quality check passed for {latest_file} — clean and consistent.")
