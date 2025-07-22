-- models/marts/dim_channels.sql
{{ config(materialized='table') }}

SELECT DISTINCT
  channel_username,
  MD5(channel_username) AS channel_id -- surrogate key
FROM {{ ref('stg_telegram_messages') }}
