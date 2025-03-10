WITH stg_weather_data AS (
	SELECT
		city
		,state
		,time
		,`date`
		,`time_in_hour`
		,temperature_c
		,`humidity_relative_%`
		,temperature_apparant_c
		,`precipitation_probability_%`
		,precipitation_mm
		,pressure_msl_hPa
		,`cloud_cover_%`
		,visibility_m
		,windspeed_kmph
		,winddirection_deg
	FROM {{ ref("stg_weather") }}
),

prominant_wind_direction AS (
	SELECT date
		,state
		,city
		,AVG(winddirection_deg) AS winddirection_deg
	FROM stg_weather_data
	GROUP BY state
		,city
		,date
),

prominant_wind_direction_named AS (
	SELECT date
		,state
		,city
		,CASE 
			WHEN winddirection_deg >= 0
				AND winddirection_deg < 22.5
				THEN 'North'
			WHEN winddirection_deg >= 22.5
				AND winddirection_deg < 67.5
				THEN 'North-East'
			WHEN winddirection_deg >= 67.5
				AND winddirection_deg < 112.5
				THEN 'East'
			WHEN winddirection_deg >= 112.5
				AND winddirection_deg < 157.5
				THEN 'South-East'
			WHEN winddirection_deg >= 157.5
				AND winddirection_deg < 202.5
				THEN 'South'
			WHEN winddirection_deg >= 202.5
				AND winddirection_deg < 247.5
				THEN 'South-West'
			WHEN winddirection_deg >= 247.5
				AND winddirection_deg < 292.5
				THEN 'West'
			WHEN winddirection_deg >= 292.5
				AND winddirection_deg < 337.5
				THEN 'North-West'
			WHEN winddirection_deg >= 337.5
				AND winddirection_deg < 360
				THEN 'North'
			ELSE NULL
			END AS wind_direction
	FROM prominant_wind_direction
),

aggregated_weather_data AS (
	SELECT
		date
		,state
		,city
		,max(temperature_c) AS max_temp_c
		,min(temperature_c) AS min_temp_c
		,round(avg(temperature_c), 3) AS avg_temp_c
		,max(temperature_apparant_c) AS max_apparent_temp_c
		,min(temperature_apparant_c) AS min_apparent_temp_c
		,round(avg(temperature_apparant_c), 3) AS avg_apparent_temp_c
		,max(`humidity_relative_%`) AS "max_relative_humidity_%"
		,min(`humidity_relative_%`) AS "min_relative_humidity_%"
		,round(avg(`humidity_relative_%`), 3) AS "avg_relative_humidity_%"
		,max(`precipitation_probability_%`) AS "max_precipitation_probability_%"
		,min(`precipitation_probability_%`) AS "min_precipitation_probability_%"
		,round(avg(`precipitation_probability_%`), 3) AS "avg_precipitation_probability_%"
		,max(precipitation_mm) AS max_precipitation_mm
		,min(precipitation_mm) AS min_precipitation_mm
		,round(avg(precipitation_mm), 3) AS avg_precipitation_mm
		,max(pressure_msl_hPa) AS max_pressure_hPa
		,min(pressure_msl_hPa) AS min_pressure_hPa
		,round(avg(pressure_msl_hPa), 3) AS avg_pressure_hPa
		,max(`cloud_cover_%`) AS "max_cloud_cover_%"
		,min(`cloud_cover_%`) AS "min_cloud_cover_%"
		,round(avg(`cloud_cover_%`), 3) AS "avg_cloud_cover_%"
		,max(visibility_m) AS max_visibility_m
		,min(visibility_m) AS min_visibility_m
		,round(avg(visibility_m), 3) AS avg_visibility_m
		,max(windspeed_kmph) AS max_windspeed_kmph
		,min(windspeed_kmph) AS min_windspeed_kmph
		,round(avg(windspeed_kmph), 3) AS avg_windspeed_kmph
	FROM stg_weather_data
	GROUP BY STATE
		,city
		,date
),

aggregated_weather_data_joined AS (
	SELECT a.*
		,p.wind_direction
	FROM aggregated_weather_data a
	INNER JOIN prominant_wind_direction_named p ON a.STATE = p.STATE
		AND a.city = p.city
		AND a.DATE = p.DATE
)

SELECT *
FROM aggregated_weather_data_joined
