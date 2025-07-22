from sqlalchemy.orm import Session
from sqlalchemy import text

def get_channel_activity(db: Session, channel_id: str):
    query = text("""
        SELECT DATE(media_date) AS date, COUNT(*) AS message_count
        FROM telegram_schema.fct_messages
        WHERE channel_id = :channel
        GROUP BY DATE(media_date)
        ORDER BY DATE(media_date)
        limit 10
    """)
    result = db.execute(query, {"channel": channel_id})
    return [{"date": row.date, "message_count": row.message_count} for row in result]

def get_messages(db: Session, query: str):
    print(f"Searching for query: {query}")
    sql = """
        SELECT 
            message_id,
            channel_id,
            message_text,
            media_date,
            has_image
        FROM telegram_schema.fct_messages
        WHERE message_text IS NOT NULL
        AND message_text ILIKE :query
        ORDER BY media_date DESC
        LIMIT 50
    """
    print(f"Executing SQL: {sql}")
    result = db.execute(text(sql), {"query": f"%{query}%"})
    rows = list(result)
    print(f"Found {len(rows)} matching messages")
    
    return [{
        "message_id": row.message_id, 
        "channel_id": row.channel_id, 
        "message_text": row.message_text,
        "media_date": row.media_date,
        "has_image": row.has_image
    } for row in rows]
