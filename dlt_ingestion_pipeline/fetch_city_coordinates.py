import requests
import json
from dotenv import load_dotenv
import os
import dlt
import time
from sqlalchemy import create_engine

load_dotenv()
TOKEN = os.getenv("COORDINATE_TOKEN")

def get_coordinates():
    with open('state_city_mapping.json','r') as f:
        mapping_data = json.load(f)
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "application/json",
        }
        
        for state in mapping_data.keys():
            for city in mapping_data[state]:
                api_url = f"https://geocode.maps.co/search?q={city}, {state}&api_key={TOKEN}"
                try:
                    response = requests.get(api_url, headers=headers)
                    time.sleep(1.2)
                    if response.status_code == 200:
                        data = response.json()

                        latitude, longitude, bounding_box  = data[0]["lat"], data[0]["lon"], data[0]['boundingbox']
                        
                        yield {
                                "city":city,
                                "state":state,
                                "latitude":latitude,
                                "longitude":longitude,
                                "boundingbox":bounding_box,
                                "code":200
                            }
                    else:
                        yield {
                                "city":city,
                                "state":state,
                                "latitude":None,
                                "longitude":None,
                                "boundingbox":None,
                                "code":400,
                            } 
                except Exception as e:
                    yield {
                                "city":city,
                                "state":state,
                                "latitude":None,
                                "longitude":None,
                                "boundingbox":None,
                                "code":500,
                            }
pipeline = dlt.pipeline(
    pipeline_name="extract_city_coordinates_from_json",
    destination="sqlalchemy",
    dataset_name="weather_pipeline",
)

data = get_coordinates()
info = pipeline.run(
    data, 
    table_name="cities", 
    write_disposition="merge",
    primary_key=["city", "state"]
)
print("✅ Coordinates data loaded successfully!")

## FINISHED