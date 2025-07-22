-- models/marts/dim_dates.sql
{{ config(materialized='table') }}

WITH date_series AS (
  SELECT generate_series(
    (SELECT MIN(media_date) FROM {{ ref('stg_telegram_messages') }}),
    (SELECT MAX(media_date) FROM {{ ref('stg_telegram_messages') }}),
    interval '1 day'
  ) AS date_day
)
SELECT
  date_day::date AS date,
  EXTRACT(DAY FROM date_day) AS day,
  EXTRACT(MONTH FROM date_day) AS month,
  EXTRACT(YEAR FROM date_day) AS year,
  TO_CHAR(date_day, 'Day') AS weekday
FROM date_series
