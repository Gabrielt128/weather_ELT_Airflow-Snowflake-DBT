-- models/weatherETL/intermediate/itm_weather.sql
{{
  config(
    materialized='view'
  )
}}

SELECT 
  date_day,
  AVG(apparent_temperature) AS avg_atemp,
  AVG(temperature_2m) AS avg_temp,
  SUM(precipitation) AS sum_prec
FROM {{ref('stg_weather')}}
GROUP BY date_day