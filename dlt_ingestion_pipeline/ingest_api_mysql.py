import requests
import os
import time
import dlt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# MySQL Database Connection
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")

def fetch_city_data():
    """Fetch existing city-state pairs from MySQL using a generator."""
    with engine.connect() as connection:
        query = text("SELECT city, state, latitude, longitude FROM cities WHERE latitude IS NOT NULL")
        result = connection.execute(query)

        for row in result:
            yield row[0], row[1], row[2], row[3]

def fetch_weather_data():
    """Fetch weather data for each city-state pair using a generator."""
    for city, state, lat, lon in fetch_city_data():
        api_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relativehumidity_2m,apparent_temperature,precipitation_probability,precipitation,pressure_msl,cloudcover,visibility,windspeed_10m,winddirection_10m"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        try:
            response = requests.get(api_url, headers=headers)
            time.sleep(0.3)
            
            if response.status_code == 200:
                data = response.json()
                hourly_data = data.get("hourly", {})
                
                if hourly_data:
                    for idx in range(len(hourly_data['time'])):
                        yield {
                            'city':city,
                            'state':state,
                            'time': hourly_data['time'][idx],
                            'latitude': float(data.get('latitude', 0)) if data.get('latitude') is not None else None,
                            'longitude': float(data.get('longitude', 0)) if data.get('longitude') is not None else None,
                            'elevation': data.get('elevation', None),
                            'temperature_2m': hourly_data['temperature_2m'][idx],
                            'relativehumidity_2m': hourly_data['relativehumidity_2m'][idx],
                            'apparent_temperature': hourly_data['apparent_temperature'][idx],
                            'precipitation_probability': hourly_data['precipitation_probability'][idx],
                            'precipitation': hourly_data['precipitation'][idx],
                            'pressure_msl': hourly_data['pressure_msl'][idx],
                            'cloudcover': hourly_data['cloudcover'][idx],
                            'visibility': hourly_data['visibility'][idx],
                            'windspeed_10m': hourly_data['windspeed_10m'][idx],
                            'winddirection_10m': hourly_data['winddirection_10m'][idx],
                            'code':200
                        }
            else:
                yield {
                    'city':city,
                    'state':state,
                    'time': None,
                    'latitude': lat,
                    'longitude': lon,
                    'elevation': None,
                    'temperature_2m': None,
                    'relativehumidity_2m': None,
                    'apparent_temperature': None,
                    'precipitation_probability': None,
                    'precipitation': None,
                    'pressure_msl': None,
                    'cloudcover': None,
                    'visibility': None,
                    'windspeed_10m': None,
                    'winddirection_10m': None,
                    'code':400
                }
        except Exception as e:
            yield {
                    'city':city,
                    'state':state,
                    'time': None,
                    'latitude': lat,
                    'longitude': lon,
                    'elevation': None,
                    'temperature_2m': None,
                    'relativehumidity_2m': None,
                    'apparent_temperature': None,
                    'precipitation_probability': None,
                    'precipitation': None,
                    'pressure_msl': None,
                    'cloudcover': None,
                    'visibility': None,
                    'windspeed_10m': None,
                    'winddirection_10m': None,
                    'code':500
                }

# Define the data pipeline
pipeline = dlt.pipeline(
    pipeline_name="weather_trends",
    destination="sqlalchemy",
    dataset_name="weather_pipeline"
)


# Stream data into the pipeline
info = pipeline.run(fetch_weather_data(), table_name="weather_trends")
print("✅ Weather data loaded successfully!")