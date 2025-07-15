#!/usr/bin/env python3
"""
Telegram Scraper

This script scrapes messages and media from specified Telegram channels.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from pathlib import Path

from config.config import config
from services.telegram_service import TelegramService
from processors.data_processor import DataProcessor
from utils.logger import default_logger as logger

class TelegramScraper:
    """Main scraper class that orchestrates the scraping process."""
    
    def __init__(self):
        self.telegram = TelegramService()
        self.data_processor = DataProcessor()
    
    async def scrape_channel(
        self,
        channel_username: str,
        limit: int = 100000,
        min_id: int = 0,
        max_id: int = 0,
        offset_id: int = 0,
        offset_date: Optional[datetime] = None,
        add_offset: int = 0,
        search: str = "",
        filter: str = "",
        from_user: str = "",
        wait_time: int = 1,
        min_wait: int = 1,
        max_wait: int = 5,
        max_retries: int = 3,
        reverse: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Scrape messages and media from a Telegram channel.
        
        This method performs two main tasks:
        1. Fetches and saves all messages from the channel
        2. Fetches and saves all media files from the channel separately
        """
        result = {
            'channel': channel_username,
            'status': 'failed',
            'message_count': 0,
            'media_count': 0,
            'messages_file': None,
            'media_files': [],
            'error': None
        }
        
        try:
            # Get channel info
            try:
                channel_info = await self.telegram.get_channel_info(channel_username)
                logger.info(f"Scraping channel: {channel_info.get('title', channel_username)}")
            except Exception as e:
                result['error'] = f"Failed to get channel info: {str(e)}"
                logger.error(result['error'])
                return result
            
            # Create directory for this channel's data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            channel_dir = os.path.join(
                config.DATA_DIR,
                'raw',
                'telegram_messages',
                datetime.now().strftime('%Y-%m-%d'),
                channel_username
            )
            os.makedirs(channel_dir, exist_ok=True)
            
            # 1. Create directories for messages and media
            # Media will be saved in a separate directory structure
            media_base_dir = os.path.join('data', 'raw', 'telegram_media')
            media_dir = os.path.join(media_base_dir, channel_username, timestamp)
            os.makedirs(media_dir, exist_ok=True)
            logger.info(f"Media files will be saved to: {os.path.abspath(media_dir)}")
            
            # 2. First, collect and save messages (without media)
            messages = []
            message_count = 0
            
            logger.info(f"Starting to collect up to {limit} messages from {channel_username}")
            
            async for message in self.telegram.get_messages(channel_username, limit=limit):
                try:
                    message_dict = message.to_dict() if hasattr(message, 'to_dict') else {}
                    
                    # Remove media from message to keep it clean
                    if 'media' in message_dict:
                        del message_dict['media']
                        
                    messages.append(message_dict)
                    message_count += 1
                    
                    # Log progress every 100 messages and save periodically
                    if message_count % 100 == 0:
                        logger.info(f"Collected {message_count} messages...")
                        
                    # Save progress every 1000 messages to prevent data loss
                    if message_count % 1000 == 0:
                        messages_file = os.path.join(channel_dir, f"messages_{timestamp}_partial_{message_count}.json")
                        with open(messages_file, 'w', encoding='utf-8') as f:
                            json.dump(messages, f, ensure_ascii=False, indent=2)
                        logger.info(f"Saved partial progress at {message_count} messages")
                        
                except Exception as e:
                    logger.error(f"Error processing message {message_count}: {e}", exc_info=True)
                    continue
                    
            # Save final messages to file
            messages_file = os.path.join(channel_dir, f"messages_{timestamp}.json")
            with open(messages_file, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            
            # Store the result
            result['messages_file'] = messages_file
            result['message_count'] = len(messages)
            logger.info(f"Saved {len(messages)} messages to {messages_file}")
            
            # 3. Now, collect all media files separately
            media_files = []
            media_count = 0
            
            logger.info(f"Starting to collect media files from {channel_username}")
            try:
                # Pass the media directory for downloading files
                try:
                    async for media_info in self.telegram.get_channel_media(
                        channel_username, 
                        limit=limit,
                        download_path=media_dir  # This will trigger file downloads
                    ):
                        if media_info:
                            media_files.append(media_info)
                            media_count += 1
                            
                            # Log progress every 10 media files
                            if media_count % 10 == 0:
                                logger.info(f"Collected {media_count} media files...")
                except Exception as e:
                    logger.error(f"Error processing media: {str(e)}", exc_info=True)
                
                # Save media info to file in the messages directory
                if media_files:
                    media_info_file = os.path.join(channel_dir, f'media_info_{timestamp}.json')
                    with open(media_info_file, 'w', encoding='utf-8') as f:
                        # Update file paths in media info to be relative to the project root
                        for media in media_files:
                            if 'file_path' in media:
                                media['file_path'] = os.path.relpath(media['file_path'], os.getcwd())
                        json.dump(media_files, f, ensure_ascii=False, indent=2)
                    
                    result['media_info_file'] = media_info_file
                    result['media_count'] = len(media_files)
                    logger.info(f"Saved info for {len(media_files)} media files to {media_info_file}")
                    logger.info(f"Media files saved to: {os.path.abspath(media_dir)}")
                    logger.info(f"Saved info for {len(media_files)} media files to {media_info_file}")
                
                # Update status
                result['status'] = 'success'
                
            except Exception as e:
                result['error'] = f"Failed to fetch media: {str(e)}"
                logger.error(result['error'])
            
            return result
            
        except Exception as e:
            result['error'] = f"Unexpected error: {str(e)}"
            logger.error(f"Error scraping channel {channel_username}: {str(e)}", exc_info=True)
            return result

async def main():
    """Main function to run the scraper."""
    # List of channels to scrape
    channels = [
        'CheMed123',
        'lobelia4cosmetics',
        'tikvahpharma'
    ]
    
    # Initialize scraper
    scraper = TelegramScraper()
    
    # Ensure we're connected
    try:
        await scraper.telegram.connect()
        if not scraper.telegram._is_connected:
            logger.error("Failed to connect to Telegram. Please run auth_telegram.py first to authenticate.")
            print("\nPlease run the authentication script first:")
            print("python scripts/auth_telegram.py\n")
            return
    except Exception as e:
        logger.error(f"Failed to connect to Telegram: {str(e)}")
        print("\nAuthentication required. Please run:")
        print("python scripts/auth_telegram.py\n")
        return
    
    # Process each channel
    for channel in channels:
        logger.info(f"\n{'='*50}")
        logger.info(f"Starting scrape for: {channel}")
        logger.info(f"{'='*50}")
        
        try:
            # Scrape the channel
            result = await scraper.scrape_channel(channel, limit=100000)
            
            # Log results
            if result['status'] == 'success':
                logger.info(f"Successfully scraped {result['message_count']} messages")
                logger.info(f"Downloaded {result['media_count']} media files")
                if result.get('messages_file'):
                    logger.info(f"Messages saved to: {result['messages_file']}")
                if result.get('media_info_file'):
                    logger.info(f"Media info saved to: {result['media_info_file']}")
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Failed to scrape {channel}: {error_msg}")
                
        except Exception as e:
            logger.error(f"Error processing channel {channel}: {str(e)}", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)