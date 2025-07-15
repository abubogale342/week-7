import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any

# Set up logger
logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for the Telegram scraper."""
    
    def __init__(self):
        # Load environment variables
        env_path = Path(__file__).parent.parent.parent / '.env'
        load_dotenv(dotenv_path=env_path)
        
        # Telegram API credentials
        self.API_ID = os.getenv('TELEGRAM_APP_ID')
        self.API_HASH = os.getenv('TELEGRAM_API_HASH')
        self.PHONE = os.getenv('TELEGRAM_PHONE')
        
        # Scraper settings
        self.BASE_DIR = Path(__file__).parent.parent.parent
        self.DATA_DIR = self.BASE_DIR / 'data'
        self.SESSION_NAME = str(self.DATA_DIR / 'sessions' / 'telegram_session')
        self.RAW_DATA_DIR = self.DATA_DIR / 'raw'
        self.MESSAGES_DIR = self.RAW_DATA_DIR / 'telegram_messages'
        self.MEDIA_DIR = self.RAW_DATA_DIR / 'telegram_media'
        
        # Create necessary directories
        self._create_directories()
        
        # Validate configuration
        self._validate()
    
    def _create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        # Create all required directories
        directories = [
            self.DATA_DIR / 'sessions',  # For session files
            self.MESSAGES_DIR,            # For message JSON files
            self.MEDIA_DIR               # For downloaded media
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def _validate(self) -> None:
        """Validate the configuration."""
        if not all([self.API_ID, self.API_HASH, self.PHONE]):
            raise ValueError(
                "Missing required environment variables. "
                "Please set TELEGRAM_APP_ID, TELEGRAM_API_HASH, and TELEGRAM_PHONE in .env file"
            )
    
    def get_telegram_config(self) -> Dict[str, str]:
        """Get Telegram API configuration."""
        return {
            'api_id': self.API_ID,
            'api_hash': self.API_HASH,
            'phone': self.PHONE
        }

# Create a global config instance
config = Config()
