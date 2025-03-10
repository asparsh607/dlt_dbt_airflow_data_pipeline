{{ config(
    materialized='incremental',
    unique_key=['row_id', 'time']
) }}

WITH stg_weather_data AS (
    SELECT 
        cast(_dlt_id as char) as row_id,
        city,
        state,
        time,
        DATE(time) AS `date`,
        TIME(time) AS `time_in_hour`,
        temperature_2m AS temperature_c,
        relativehumidity_2m AS "humidity_relative_%",
        apparent_temperature AS temperature_apparant_c,
        precipitation_probability AS "precipitation_probability_%",
        precipitation AS precipitation_mm,
        pressure_msl AS pressure_msl_hPa,
        cloudcover AS "cloud_cover_%",
        visibility AS visibility_m,
        windspeed_10m AS windspeed_kmph,
        winddirection_10m AS winddirection_deg,
        FROM_UNIXTIME(CAST(_dlt_load_id AS DECIMAL(20,3))) AS load_time
    FROM {{ source("ingestion", "weather_trends") }}

    {% if is_incremental() %}
    WHERE FROM_UNIXTIME(CAST(_dlt_load_id AS DECIMAL(20,3))) > (
        SELECT COALESCE(MAX(load_time), '1900-01-01') 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT * FROM stg_weather_data
