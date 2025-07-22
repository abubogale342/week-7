-- models/staging/stg_telegram_messages.sql
{{ config(materialized='view') }}

SELECT
  id AS message_uuid,
  channel_username,
  media_date::timestamp,
  
  -- JSONB extracted fields
  (media_data->>'message_id')::int AS message_id,
  (media_data->>'media_id')::bigint AS media_id,
  (media_data->>'media_type') AS media_type,
  (media_data->>'file_path') AS file_path,
  (media_data->>'access_hash')::bigint AS access_hash,
  (media_data->>'download_success')::boolean AS download_success,
  
  -- Derived fields
  (media_data->>'media_type') = 'photo' AS has_image,
  loaded_at::timestamp
FROM {{ source('raw', 'telegram_media') }}
