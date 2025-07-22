-- models/marts/fct_messages.sql
{{ config(materialized='table') }}

SELECT
  m.message_id,
  c.channel_id,
  d.date AS media_date,
  m.has_image
FROM {{ ref('stg_telegram_messages') }} m
LEFT JOIN {{ ref('dim_channels') }} c
  ON m.channel_username = c.channel_username
LEFT JOIN {{ ref('dim_dates') }} d
  ON m.media_date::date = d.date
