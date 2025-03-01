import dlt
import requests
import os
from dotenv import load_dotenv

load_dotenv()


MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


API_URL = "https://jsonplaceholder.typicode.com/posts"

def fetch_data():
    """Fetch data from the API"""
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()


pipeline = dlt.pipeline(
    pipeline_name="api_mysql_pipeline",
    destination="mysql",
    dataset_name="api_data",
    credentials={
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "database": MYSQL_DATABASE,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
    }
)

if __name__ == "__main__":
    data = fetch_data()
    info = pipeline.run(data, table_name="posts")
    print(info)

