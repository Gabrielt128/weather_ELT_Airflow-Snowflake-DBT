-- models/marts/daily_avg_weather.sql
{{
  config(
    materialized='table',
    unique_key='date_day'
  )
}}

SELECT
  date_day,
  AVG(apparent_temperature) AS avg_apparent_temperature,
  COUNT(*) AS records_count
FROM {{ ref('stg_weather') }}
GROUP BY date_day