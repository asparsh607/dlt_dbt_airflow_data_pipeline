import requests
import os
import time
import dlt
from dotenv import load_dotenv
from sqlalchemy import create_engine

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
        result = connection.execute("SELECT city, state, latitude, longitude FROM cities WHERE latitude IS NOT NULL")
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
    dataset_name="trending_pipeline",
    schema={
        "latitude": {"data_type": "double"},
        "longitude": {"data_type": "double"},
        "elevation": {"data_type": "double"},
        "temperature_2m": {"data_type": "double"},
        "relativehumidity_2m": {"data_type": "double"},
        "apparent_temperature": {"data_type": "double"},
        "precipitation_probability": {"data_type": "double"},
        "precipitation": {"data_type": "double"},
        "pressure_msl": {"data_type": "double"},
        "cloudcover": {"data_type": "double"},
        "visibility": {"data_type": "double"},
        "windspeed_10m": {"data_type": "double"},
        "winddirection_10m": {"data_type": "double"},
        "code": {"data_type": "int"}
    }
)


# Stream data into the pipeline
info = pipeline.run(fetch_weather_data(), table_name="weather_trends")
print("✅ Weather data loaded successfully!")



# # Section 3: PySpark Initialization

# # Creating a Spark session
# spark = SparkSession.builder.appName("WeatherTransformationApp").getOrCreate()

# # Defining the schema for the PySpark DataFrame
# schema = T.StructType(
#     [
#         T.StructField("time", T.StringType(), nullable=True),
#         T.StructField("temperature_c", T.FloatType(), nullable=True),
#         T.StructField("humidity_relative_%", T.IntegerType(), nullable=True),
#         T.StructField("temperature_apparant_c", T.FloatType(), nullable=True),
#         T.StructField("precipitation_probability_%", T.IntegerType(), nullable=True),
#         T.StructField("precipitation_mm", T.FloatType(), nullable=True),
#         T.StructField("pressure_msl_hPa", T.FloatType(), nullable=True),
#         T.StructField("cloud_cover_%", T.IntegerType(), nullable=True),
#         T.StructField("visibility_m", T.FloatType(), nullable=True),
#         T.StructField("windspeed_kmph", T.FloatType(), nullable=True),
#         T.StructField("winddirection_deg", T.IntegerType(), nullable=True)
#     ]
# )

# # Section 4: Data Extraction and Transformation

# # Getting user location and weather data
# location = get_location()
# weather = get_weather_data(location[0]["lat"], location[0]["lon"])

# # Extracting relevant data from the weather API response
# data_column_wise = [
#     weather["hourly"]["time"],  # Time column
#     weather["hourly"]["temperature_2m"],  # Temperature column
#     weather["hourly"]["relativehumidity_2m"],  # Rel_humidity Column
#     weather["hourly"]["apparent_temperature"],  # Apparent_temperature Column
#     weather["hourly"]["precipitation_probability"],  # Precipitation_probability Column
#     weather["hourly"]["precipitation"],  # Precipitation Column
#     weather["hourly"]["pressure_msl"],  # Pressure mean sea level Column
#     weather["hourly"]["cloudcover"],  # Cloud Cover Column
#     weather["hourly"]["visibility"],  # Visibility Column
#     weather["hourly"]["windspeed_10m"],  # Wind Speed Column
#     weather["hourly"]["winddirection_10m"],  # Wind Direction Column
# ]

# # Transposing data from column-wise to row-wise
# data_row_wise = list(map(list, zip(*data_column_wise)))

# # Creating a PySpark DataFrame with the transposed data and schema
# df = spark.createDataFrame(data_row_wise, schema=schema)


# # Section 5: Further Data Transformations

# df = df.withColumn("date", df.time[0:10]).withColumn("time_in_hour", df.time[12:15])

# prominant_wind_direction_df = (
#     df.groupBy(df.date)
#     .agg({"winddirection_deg": "avg"})
#     .withColumnRenamed("avg(winddirection_deg)", "winddirection_deg")
# )

# prominant_wind_direction_df = prominant_wind_direction_df.withColumn(
#     "wind_direction",
#     when((col("winddirection_deg") >= 0) & (col("winddirection_deg") < 22.5), "North")
#     .when(
#         (col("winddirection_deg") >= 22.5) & (col("winddirection_deg") < 67.5),
#         "North-East",
#     )
#     .when(
#         (col("winddirection_deg") >= 67.5) & (col("winddirection_deg") < 112.5), "East"
#     )
#     .when(
#         (col("winddirection_deg") >= 112.5) & (col("winddirection_deg") < 157.5),
#         "South-East",
#     )
#     .when(
#         (col("winddirection_deg") >= 157.5) & (col("winddirection_deg") < 202.5),
#         "South",
#     )
#     .when(
#         (col("winddirection_deg") >= 202.5) & (col("winddirection_deg") < 247.5),
#         "South-West",
#     )
#     .when(
#         (col("winddirection_deg") >= 247.5) & (col("winddirection_deg") < 292.5), "West"
#     )
#     .when(
#         (col("winddirection_deg") >= 292.5) & (col("winddirection_deg") < 337.5),
#         "North-West",
#     )
#     .when(
#         (col("winddirection_deg") >= 337.5) & (col("winddirection_deg") <= 360), "North"
#     )
# )

# df = df.drop("time").drop("time_in_hour")
# prominant_wind_direction_df = prominant_wind_direction_df.drop("winddirection_deg")

# # Continue with the data transformation steps...


# # Aggregate data: Group by columns and apply aggregation functions
# aggregated_daily_max_df = df.groupBy(df.date).agg(
#     {
#         "temperature_c": "max",
#         "temperature_apparant_c": "max",
#         "humidity_relative_%": "max",
#         "precipitation_probability_%": "max",
#         "precipitation_mm": "max",
#         "pressure_msl_hPa": "max",
#         "cloud_cover_%": "max",
#         "visibility_m": "max",
#         "windspeed_kmph": "max",
#     }
# )

# aggregated_daily_min_df = df.groupBy(df.date).agg(
#     {
#         "temperature_c": "min",
#         "temperature_apparant_c": "min",
#         "humidity_relative_%": "min",
#         "precipitation_probability_%": "min",
#         "precipitation_mm": "min",
#         "pressure_msl_hPa": "min",
#         "cloud_cover_%": "min",
#         "visibility_m": "min",
#         "windspeed_kmph": "min",
#     }
# )

# aggregated_daily_avg_df = df.groupBy(df.date).agg(
#     {
#         "temperature_c": "avg",
#         "temperature_apparant_c": "avg",
#         "humidity_relative_%": "avg",
#         "precipitation_probability_%": "avg",
#         "precipitation_mm": "avg",
#         "pressure_msl_hPa": "avg",
#         "cloud_cover_%": "avg",
#         "visibility_m": "avg",
#         "windspeed_kmph": "avg",
#     }
# )


# # Section 6: Aggregation and Result Display

# # Joining and aggregating data for visualization
# joining_wind_max_df = prominant_wind_direction_df.join(
#     aggregated_daily_max_df, "date", "inner"
# )
# joining_wind_max_min_df = joining_wind_max_df.join(
#     aggregated_daily_min_df, "date", "inner"
# )

# joining_wind_max_avg_min_df = joining_wind_max_min_df.join(
#     aggregated_daily_avg_df, "date", "inner"
# )

# # Define a dictionary for column renaming
# column_name_mapping = {
#     "wind_direction": "prominent_wind_direction",
#     "max(visibility_m)": "max_visibility_m",
#     "max(temperature_apparant_c)": "max_apparent_temp_c",
#     "max(cloud_cover_%)": "max_cloud_cover_%",
#     "max(precipitation_mm)": "max_precipitation_mm",
#     "max(windspeed_kmph)": "max_windspeed_kmph",
#     "max(temperature_c)": "max_temp_c",
#     "max(humidity_relative_%)": "max_relative_humidity_%",
#     "max(pressure_msl_hPa)": "max_pressure_hPa",
#     "max(precipitation_probability_%)": "max_precipitation_probability_%",
#     "min(visibility_m)": "min_visibility_m",
#     "min(temperature_apparant_c)": "min_apparant_temp_c",
#     "min(cloud_cover_%)": "min_cloud_cover_%",
#     "min(precipitation_mm)": "min_precipitation_mm",
#     "min(windspeed_kmph)": "min_windspeed_kmph",
#     "min(temperature_c)": "min_temp_c",
#     "min(humidity_relative_%)": "min_relative_humidity_%",
#     "min(pressure_msl_hPa)": "min_pressure_hPa",
#     "min(precipitation_probability_%)": "min_precipitation_probability_%",
#     "avg(visibility_m)": "avg_visibility_m",
#     "avg(temperature_apparant_c)": "avg_apparant_temp_c",
#     "avg(cloud_cover_%)": "avg_cloud_cover_%",
#     "avg(precipitation_mm)":"avg_precipitation_mm",
#     "avg(windspeed_kmph)": "avg_windspeed_kmph",
#     "avg(temperature_c)": "avg_temp_c",
#     "avg(humidity_relative_%)": "avg_relative_humidity_%",
#     "avg(pressure_msl_hPa)": "avg_pressure_hPa",
#     "avg(precipitation_probability_%)": "avg_precipitation_probability_%"    
# }

# # Apply column renaming using a loop
# for old_col, new_col in column_name_mapping.items():
#     joining_wind_max_avg_min_df = joining_wind_max_avg_min_df.withColumnRenamed(old_col, new_col)


# # End of the PySpark Weather Data Transformation Script


# #Saving and Deleting Scripts for DBFS

# # use to save the csv file to dbfs
# #Enter the DBFS path where you want to write the data
# dbfs_csv_path = "FileStore/tables/aggregated_weather_data.csv"
# joining_wind_max_avg_min_df_coalesced = joining_wind_max_avg_min_df.coalesce(1)
# (joining_wind_max_avg_min_df_coalesced
#     .write
#     .format("csv")
#     .mode("overwrite")
#     .option("header", "true")
#     .save(dbfs_csv_path)
# )
# print("CSV file successfully written to DBFS.")

# #use to remove unwanted directory
# dbfs_dir_path = "/FileStore/tables/aggregated_weather_data.csv"
# dbutils.fs.rm(dbfs_dir_path, True)
# print("Directory file successfully deleted from DBFS.")