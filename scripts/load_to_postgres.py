# scripts/load_to_postgres.py
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import glob

from dotenv import load_dotenv
load_dotenv()

def get_channel_name_from_path(file_path):
    """
    Extract channel name from file path.
    Expected path format: .../telegram_messages/YYYY-MM-DD/channel_name/messages_*.json
    """
    try:
        # Split the path into parts
        parts = file_path.split(os.sep)
        
        # Find the index of 'telegram_messages' in the path
        try:
            base_idx = parts.index('telegram_messages')
        except ValueError:
            # If 'telegram_messages' not found, try to find the first date-like part
            date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
            for i, part in enumerate(parts):
                if date_pattern.match(part) and i + 1 < len(parts):
                    return parts[i + 1]
            return 'unknown'
        
        # The channel name should be two directories after 'telegram_messages'
        if base_idx + 2 < len(parts):
            return parts[base_idx + 2]
            
        return 'unknown'
    except Exception as e:
        logger.error(f"Error extracting channel name from {file_path}: {e}")
        return 'unknown'


def load_json_files(conn, base_dir):
    """Load JSON files from directory into PostgreSQL"""
    cur = conn.cursor()
    
    # Create raw schema if not exists
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        id BIGSERIAL PRIMARY KEY,
        channel_username TEXT,
        message_date TIMESTAMPTZ,
        message_data JSONB,
        loaded_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS raw.telegram_media (
        id BIGSERIAL PRIMARY KEY,
        channel_username TEXT,
        media_date TIMESTAMPTZ,
        media_data JSONB,
        loaded_at TIMESTAMPTZ DEFAULT NOW()
    );
    """)
    
    # Process message files
    message_files = glob.glob(f"{base_dir}/telegram_messages/**/messages_*.json", recursive=True)
    for file_path in message_files:
        channel = file_path.split('/')[-3]  # Extract channel name from path
        with open(file_path, 'r', encoding='utf-8') as f:
            messages = json.load(f)
            for msg in messages:
                cur.execute("""
                    INSERT INTO raw.telegram_messages (channel_username, message_date, message_data)
                    VALUES (%s, %s, %s)
                """, (channel, msg.get('date'), json.dumps(msg)))
    
    # Process media info files
    media_files = glob.glob(f"{base_dir}/telegram_messages/**/media_info_*.json", recursive=True)
    for file_path in media_files:
        channel = get_channel_name_from_path(file_path)
        with open(file_path, 'r', encoding='utf-8') as f:
            media_items = json.load(f)
            for media in media_items:
                cur.execute("""
                    INSERT INTO raw.telegram_media (channel_username, media_date, media_data)
                    VALUES (%s, %s, %s)
                """, (channel, media.get('date'), json.dumps(media)))
    
    conn.commit()
    cur.close()

if __name__ == "__main__":
    # Database connection
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    
    # Base directory containing the data
    base_dir = "data/raw"
    
    try:
        load_json_files(conn, base_dir)
        print("Data loaded successfully!")
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        conn.close()
