from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional

class ChannelActivity(BaseModel):
    date: date
    message_count: int

class MessageBase(BaseModel):
    message_id: int
    channel_id: str
    message_text: str
    media_date: Optional[datetime] = None
    has_image: Optional[bool] = None

class MessageSearchResult(BaseModel):
    success: bool = True
    count: int
    results: list[MessageBase]
    query: str
