from fastapi import FastAPI
from fastapi import Depends
from sqlalchemy.orm import Session
from schemas import ChannelActivity, MessageSearchResult
from crud import get_channel_activity, get_messages
from database import get_db
from fastapi import HTTPException

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/top-products")
def read_top_products(limit: int = 10):

    return {"top_products": "top_products", "limit": limit}

channel_map = {
    'lobelia4cosmetics': '9c8e1e57054ce9826cb986f55b25016d',
    'CheMed123': '13d619c52e5db90ef6a786b69ba3c978',
    'tikvahpharma': 'f6e7cc642365e9c68c2066ff71d8de76'
}

@app.get("/api/channels/{channel_name}/activity", response_model=list[ChannelActivity])
def get_activity(channel_name: str, db: Session = Depends(get_db)):
    channel_id = channel_map.get(channel_name)
    if not channel_id:
        raise HTTPException(status_code=404, detail="Channel not found")
    return get_channel_activity(db, channel_id)
    
@app.get("/api/search/messages", response_model=MessageSearchResult)
async def search_messages(
    query: str,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Search for messages containing the given query string.
    
    - **query**: Search term to look for in message texts
    - **limit**: Maximum number of results to return (default: 50, max: 100)
    - **offset**: Number of results to skip for pagination (default: 0)
    """
    print(f"API Request - Search query: '{query}', limit: {limit}, offset: {offset}")
    
    # Validate limit
    limit = min(max(1, limit), 100)  # Ensure limit is between 1 and 100
    
    try:
        results = get_messages(db, query)
        print(f"API Response - Found {len(results)} results")
        
        return {
            "success": True,
            "count": len(results),
            "results": results[offset:offset + limit],
            "query": query
        }
        
    except Exception as e:
        error_msg = f"Error searching messages: {str(e)}"
        print(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)