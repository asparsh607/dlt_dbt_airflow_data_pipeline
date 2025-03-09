select 

cast(city as char) as city,
cast(`state` as char) as `state`,
round(cast(latitude as double),5) as latitude,
round(cast(longitude as double),5) as longitude,
cast(code as signed) run_code,
FROM_UNIXTIME(CAST(_dlt_load_id AS DECIMAL(20,3))) as ingestion_timestamp,
cast(_dlt_id as char) as row_id

from {{ source("ingestion","cities") }}