-- models/marts/fct_messages.sql
{{ config(materialized='table') }}

SELECT
  m.message_id,
  md.detected_object_class,
  md.confidence_score
FROM {{ ref('stg_telegram_messages') }} m
INNER JOIN {{ ref('image_detections') }} md
  ON m.file_path = md.file_path
