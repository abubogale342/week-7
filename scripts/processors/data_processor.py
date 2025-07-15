import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from config.config import config
from utils.logger import default_logger as logger

class DataProcessor:
    """Handles processing and saving of scraped data."""
    
    @staticmethod
    def get_output_path(channel_name: str) -> Path:
        """Get the output path for a channel's messages."""
        today = datetime.now().strftime("%Y-%m-%d")
        channel_dir = config.MESSAGES_DIR / today / channel_name
        channel_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return channel_dir / f"{timestamp}.json"
    
    @staticmethod
    def save_messages(
        messages: List[Dict[str, Any]], 
        channel_name: str
    ) -> str:
        """Save messages to a JSON file."""
        try:
            output_path = DataProcessor.get_output_path(channel_name)
            
            # Prepare data for saving
            data = {
                'channel': channel_name,
                'scrape_time': datetime.now().isoformat(),
                'message_count': len(messages),
                'messages': messages
            }
            
            # Save to file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved {len(messages)} messages to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error saving messages: {str(e)}")
            raise
    
    @staticmethod
    def process_message(
        message_data: Dict[str, Any], 
        media_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process a single message and its media."""
        processed = message_data.copy()
        
        if media_info:
            processed['media'] = media_info
            
        return processed
