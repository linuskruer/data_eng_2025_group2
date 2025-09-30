import requests

# Coordinates for New York City (Central Park)
latitude = 40.78
longitude = -73.97

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": "2025-09-01",
    "end_date": "2025-09-28",
    "hourly": [
        "weather_code",       # Temperature at 2m
        "temperature_2m", # Relative humidity %
        "relative_humidity_2m",                 # Rainfall (mm)
        "cloudcover",         # Weather condition code
        "rain",           # Total cloud cover (%)
        "sunshine_duration",        # Wind speed at 10m
        "windspeed_10m"     # Sunshine duration (seconds)
    ],
    "timezone": "America/New_York",
    "format": "csv"  # CSV output
}
params["hourly"] = ",".join(params["hourly"])

# Fetch the data
response = requests.get(url, params=params)

# Save the data to a CSV file
output_file = "new_york_city_hourly_weather_data.csv"
with open(output_file, "wb") as f:
    f.write(response.content)

print(f"Hourly weather data for New York City saved to {output_file}")
