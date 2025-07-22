import os
import psycopg2
from telethon.sync import TelegramClient
from telethon.tl.types import PeerChannel
from config.config import config

# TELEGRAM API
api_id = config.get_telegram_config()['api_id']
api_hash = config.get_telegram_config()['api_hash']

# DB CONNECTION
conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
cursor = conn.cursor()

# 1. Get message_ids and channel_ids to update
cursor.execute("""
    SELECT message_id, channel_id
    FROM telegram_schema.fct_messages
    WHERE message_text IS NULL
""")
rows = cursor.fetchall()

channel_map = {
    '9c8e1e57054ce9826cb986f55b25016d': 'lobelia4cosmetics',
    '13d619c52e5db90ef6a786b69ba3c978': 'CheMed123',
    'f6e7cc642365e9c68c2066ff71d8de76': 'tikvahpharma'
}

with TelegramClient('session', api_id, api_hash) as client:
    for message_id, channel_id in rows:
        try:
            username = channel_map.get(channel_id)
            if not username:
                print(f"Channel ID {channel_id} not in map.")
                continue

            message = client.get_messages(username, ids=int(message_id))

            if message and message.text:
                cursor.execute("""
                    UPDATE telegram_schema.fct_messages
                    SET message_text = %s
                    WHERE message_id = %s AND channel_id = %s
                """, (message.text, message_id, channel_id))
                print(f"Updated message {message_id}")
        except Exception as e:
            print(f"Failed for {message_id}: {e}")

# Commit updates
conn.commit()
cursor.close()
conn.close()
