-- models/weatherETL/marts/daily_weather_summary.sql
{{ 
  config(
    materialized='table',
    unique_key='date_day'
)
}}

SELECT
    date_day,
    avg_atemp AS avg_apparent_temperature,
    avg_temp AS avg_temperature,
    sum_prec AS total_precipitation,
    CASE
      WHEN sum_prec > 0 THEN 'Rainy'
      ELSE 'Dry'
    END AS weather_condition
FROM {{ ref('itm_weather') }}