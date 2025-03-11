SELECT
    date
	,state
	,city
	,max_temp_c
	,min_temp_c
	,avg_temp_c
	,max_apparent_temp_c
	,min_apparent_temp_c
	,avg_apparent_temp_c
	,`max_relative_humidity_%` as max_relative_humidity_pct
	,`min_relative_humidity_%` as min_relative_humidity_pct
	,`avg_relative_humidity_%` as avg_relative_humidity_pct
	,`max_precipitation_probability_%` as max_precipitation_probability_pct
	,`min_precipitation_probability_%` as min_precipitation_probability_pct
	,`avg_precipitation_probability_%` as avg_precipitation_probability_pct
	,max_precipitation_mm
	,min_precipitation_mm
	,avg_precipitation_mm
	,max_pressure_hPa
	,min_pressure_hPa
	,avg_pressure_hPa
	,`max_cloud_cover_%` as max_cloud_cover_pct
	,`min_cloud_cover_%` as min_cloud_cover_pct
	,`avg_cloud_cover_%` as avg_cloud_cover_pct
	,max_visibility_m
	,min_visibility_m
	,avg_visibility_m
	,max_windspeed_kmph
	,min_windspeed_kmph
	,avg_windspeed_kmph
    ,wind_direction
FROM
    {{ ref("int_weather") }}