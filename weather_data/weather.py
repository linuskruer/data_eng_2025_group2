import requests

# Coordinates for New York City (Central Park)
latitude = 40.78
longitude = -73.97

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": "2024-09-01",
    "end_date": "2025-09-28",
    "daily": [
        "weather_code",
        "temperature_2m_mean",
        "temperature_2m_max",
        "temperature_2m_min",
        "rain_sum",
        "snowfall_sum",
        "daylight_duration",
        "sunshine_duration",
        "wind_speed_10m_max" # Maximum UV index for the day
    ],
    "timezone": "America/New_York",
    "format": "xlsx"  # or "csv" if you prefer
}
params["daily"] = ",".join(params["daily"])

# Fetch the data
response = requests.get(url, params=params)

# Save the data to a file
output_file = "new_york_city_daily_weather_data.xlsx"
with open(output_file, "wb") as f:
    f.write(response.content)

print(f"Daily weather data for New York City saved to {output_file}")
