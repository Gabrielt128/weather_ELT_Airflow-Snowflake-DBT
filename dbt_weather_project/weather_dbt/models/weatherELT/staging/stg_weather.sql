-- models/weatherETL/staging/stg_weather.sql
{{
  config(
    materialized='view'
  )
}}

WITH hourly_data AS (
  SELECT 
    DATE_TRUNC('DAY', date) AS date_day,
    date AS date_hour,
    apparent_temperature,
    temperature_2m,
    precipitation,
    ROW_NUMBER() OVER (
      PARTITION BY DATE_TRUNC('DAY', date)
      ORDER BY date ASC
    ) AS hour_rank
  FROM {{ source('raw_data', 'raw_weather_data') }}  
)

SELECT 
  date_hour,
  date_day,
  apparent_temperature,
  temperature_2m,
  precipitation
FROM hourly_data
WHERE hour_rank <= 24