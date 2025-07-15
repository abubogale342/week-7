import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, AsyncGenerator

from telethon import TelegramClient, errors
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument

from config.config import config
from utils.logger import default_logger as logger

class TelegramService:
    """Service for interacting with Telegram API."""
    
    def __init__(self):
        """Initialize the Telegram service."""
        self.client = None
        self._is_connected = False
        
        # Get the absolute path for the session file
        self.session_path = os.path.abspath(config.SESSION_NAME)
        
        # Ensure the session directory exists
        session_dir = os.path.dirname(self.session_path)
        if session_dir:  # Only create directory if SESSION_NAME includes a path
            os.makedirs(session_dir, exist_ok=True)
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
    
    async def connect(self) -> None:
        """Connect to Telegram with proper error handling and retry logic."""
        max_retries = 3
        retry_delay = 5  # seconds
        
        if self._is_connected and self.client and self.client.is_connected():
            return
            
        telegram_config = config.get_telegram_config()
        
        for attempt in range(max_retries):
            try:
                if self.client is None:
                    self.client = TelegramClient(
                        self.session_path,  # Use the absolute path
                        int(telegram_config['api_id']),
                        telegram_config['api_hash'],
                        device_model="Telegram Scraper",
                        app_version="1.0.0",
                        system_version="Linux",
                        system_lang_code="en"
                    )
                
                if not self.client.is_connected():
                    await self.client.connect()
                    
                    # Check if we need to sign in
                    if not await self.client.is_user_authorized():
                        # Send code if needed
                        await self.client.send_code_request(telegram_config['phone'])
                        # For now, we'll just log that we need to handle the code
                        logger.warning("Phone authorization required. Please check your messages for a verification code.")
                        # In a real-world scenario, you'd want to implement code input handling here
                        return
                
                self._is_connected = True
                logger.info("Successfully connected to Telegram")
                return
                
            except Exception as e:
                self._is_connected = False
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Connection attempt {attempt + 1} failed: {str(e)}. "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("Failed to connect to Telegram after multiple attempts")
                    raise
    
    async def disconnect(self) -> None:
        """Disconnect from Telegram."""
        if self._is_connected and self.client:
            await self.client.disconnect()
            self._is_connected = False
            logger.info("Disconnected from Telegram")
    
    async def get_channel_info(self, channel_username: str) -> Dict[str, Any]:
        """Get information about a channel."""
        try:
            # Ensure client is connected
            if not self._is_connected or not self.client or not self.client.is_connected():
                await self.connect()
                
            # Get the entity
            try:
                entity = await self.client.get_entity(channel_username)
            except (ValueError, TypeError) as e:
                # If we get a ValueError, the channel might not exist or we don't have access
                logger.error(f"Could not find channel {channel_username}. Make sure the username is correct and you have access to it.")
                raise ValueError(f"Channel {channel_username} not found or access denied") from e
                
            return {
                'id': entity.id,
                'title': getattr(entity, 'title', 'Unknown'),
                'username': getattr(entity, 'username', None),
                'participants_count': getattr(entity, 'participants_count', 0),
                'verified': getattr(entity, 'verified', False),
                'scam': getattr(entity, 'scam', False),
                'description': getattr(entity, 'about', '')
            }
        except Exception as e:
            logger.error(f"Error getting channel info for {channel_username}: {str(e)}", exc_info=True)
            raise
    
    async def get_channel_media(
        self,
        channel_username: str,
        limit: int = 100000,  # Increased default limit to 50,000
        file_types: Optional[List[str]] = None,
        download_path: Optional[str] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Get all media files from a channel and optionally download them.
        
        Args:
            channel_username: Username or ID of the channel
            limit: Maximum number of media files to fetch
            file_types: List of file types to include (e.g. ['photo', 'document', 'video'])
                      If None, all media types will be included
            download_path: Directory where to save the media files. If None, files won't be downloaded.
                      
        Yields:
            Dict containing media information and local file path if downloaded
        """
        if file_types is None:
            file_types = ['photo', 'document', 'video']
            
        try:
            # Ensure client is connected
            if not self._is_connected or not self.client or not self.client.is_connected():
                await self.connect()
                
            logger.info(f"Fetching up to {limit} media files from {channel_username}")
            
            # Get the channel entity
            channel = await self.client.get_entity(channel_username)
            logger.info(f"Channel info: ID={channel.id}, Title={getattr(channel, 'title', 'N/A')}")
            
            # Create download directory if it doesn't exist
            if download_path:
                os.makedirs(download_path, exist_ok=True)
                logger.info(f"Media files will be saved to: {os.path.abspath(download_path)}")
            
            media_count = 0
            total_processed = 0
            
            # Use a larger limit to account for non-media messages
            fetch_limit = min(limit * 3, 100000)  # Cap at 100,000 messages to prevent memory issues
            
            async for message in self.client.iter_messages(channel, limit=fetch_limit):
                if media_count >= limit:
                    break
                    
                total_processed += 1
                
                # Log progress every 100 messages processed
                if total_processed % 100 == 0:
                    logger.info(f"Scanned {total_processed} messages, found {media_count} media files...")
                
                try:
                    media_info = None
                    
                    # Check for different media types
                    if hasattr(message, 'photo') and 'photo' in file_types:
                        media_info = await self._process_media(message, 'photo', download_path)
                    elif hasattr(message, 'document') and 'document' in file_types:
                        # Skip non-media documents (like PDFs, DOCs) if needed
                        mime_type = getattr(message.document, 'mime_type', '').lower()
                        if not mime_type or any(x in mime_type for x in ['image/', 'video/']):
                            media_info = await self._process_media(message, 'document', download_path)
                    elif hasattr(message, 'video') and 'video' in file_types:
                        media_info = await self._process_media(message, 'video', download_path)
                    
                    if media_info:
                        media_count += 1
                        yield media_info
                        
                        # Log progress every 10 media files
                        if media_count % 10 == 0:
                            logger.info(f"Downloaded {media_count} media files...")
                            
                except Exception as e:
                    logger.error(f"Error processing media from message {getattr(message, 'id', 'unknown')}: {str(e)}", exc_info=True)
                    continue
                    
        except Exception as e:
            logger.error(f"Error fetching media from {channel_username}: {str(e)}", exc_info=True)
            raise

    async def _process_media(self, message, media_type: str, download_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Process a media message and return its information.
        
        Args:
            message: The message containing media
            media_type: Type of media ('photo', 'document', 'video')
            download_path: Optional path to save the media file
            
        Returns:
            Dict containing media information or None if processing fails
        """
        try:
            media = getattr(message, media_type, None)
            if not media:
                return None
                
            # Create base media info
            media_info = {
                'message_id': message.id,
                'date': message.date.isoformat() if hasattr(message, 'date') else None,
                'media_type': media_type,
                'file_path': None
            }
            
            # Add media-specific attributes
            if hasattr(media, 'id'):
                media_info['media_id'] = media.id
            if hasattr(media, 'access_hash'):
                media_info['access_hash'] = media.access_hash
                
            # Handle document attributes
            if hasattr(media, 'mime_type'):
                media_info['mime_type'] = media.mime_type
            if hasattr(media, 'size'):
                media_info['file_size'] = media.size
            if hasattr(media, 'file_name'):
                media_info['file_name'] = media.file_name
                
            # Download the file if download_path is provided
            if download_path and hasattr(message, 'download_media'):
                try:
                    # Create the download directory if it doesn't exist
                    os.makedirs(download_path, exist_ok=True)
                    
                    # Download the media file directly using the message's download_media method
                    # This will automatically handle the file naming and downloading
                    file_path = await message.download_media(file=download_path)
                    
                    if file_path and os.path.exists(file_path):
                        media_info['file_path'] = os.path.abspath(file_path)
                        media_info['download_success'] = True
                        logger.info(f"Downloaded media to: {file_path}")
                    else:
                        media_info['download_success'] = False
                        logger.warning("Failed to download media file")
                        
                except Exception as e:
                    media_info['download_error'] = str(e)
                    media_info['download_success'] = False
                    logger.error(f"Error downloading media: {e}", exc_info=True)
            
            return media_info
            
        except Exception as e:
            logger.error(f"Error processing {media_type} media: {e}", exc_info=True)
            return None
            
    def _get_file_extension(self, media_type: str, mime_type: str = None) -> str:
        """Get appropriate file extension based on media type and mime type."""
        if media_type == 'photo':
            return '.jpg'
        elif media_type == 'video':
            return '.mp4'
        elif media_type == 'document' and mime_type:
            # Extract extension from mime type if available
            ext_map = {
                'image/jpeg': '.jpg',
                'image/png': '.png',
                'application/pdf': '.pdf',
                'application/msword': '.doc',
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
            }
            return ext_map.get(mime_type, '.bin')
        return '.bin'

    async def get_messages(
        self, 
        channel_username: str, 
        limit: int = 50000  # Increased default limit to 50,000
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Get messages from a channel."""
        try:
            # Ensure client is connected
            if not self._is_connected or not self.client or not self.client.is_connected():
                await self.connect()
                
            logger.info(f"Fetching up to {limit} messages from {channel_username}")
            
            # Get the channel entity
            channel = await self.client.get_entity(channel_username)
            logger.info(f"Channel info: ID={channel.id}, Title={getattr(channel, 'title', 'N/A')}")
            
            message_count = 0
            async for message in self.client.iter_messages(channel, limit=limit):
                message_count += 1
                
                # Log detailed info for first 10 messages
                if message_count <= 10:  # Check more messages for media
                    logger.info(f"\n--- Message {message_count} (ID: {message.id}) ---")
                    logger.info(f"Date: {message.date}")
                    logger.info(f"Text: {getattr(message, 'text', 'N/A')}")
                    
                    # Log all message attributes for debugging
                    msg_attrs = [attr for attr in dir(message) if not attr.startswith('_')]
                    logger.debug(f"Message attributes: {', '.join(msg_attrs)}")
                    
                    # Check for media in different ways
                    has_media = False
                    
                    # Check message.media
                    if hasattr(message, 'media') and message.media is not None:
                        has_media = True
                        logger.info("MEDIA DETECTED: message.media is present")
                        media_attrs = [attr for attr in dir(message.media) if not attr.startswith('_')]
                        logger.info(f"Media attributes: {', '.join(media_attrs)}")
                        
                        # Check for document
                        if hasattr(message.media, 'document'):
                            doc = message.media.document
                            logger.info("Document found in message.media.document")
                            logger.info(f"Document ID: {getattr(doc, 'id', 'N/A')}")
                            logger.info(f"Document MIME: {getattr(doc, 'mime_type', 'N/A')}")
                            logger.info(f"Document size: {getattr(doc, 'size', 'N/A')} bytes")
                        
                        # Check for photo
                        if hasattr(message.media, 'photo'):
                            logger.info("Photo found in message.media.photo")
                            
                    # Check for photo attribute directly on message
                    if hasattr(message, 'photo') and message.photo:
                        has_media = True
                        logger.info("MEDIA DETECTED: message.photo is present")
                        logger.info(f"Photo ID: {getattr(message.photo, 'id', 'N/A')}")
                        
                    # Check for document attribute directly on message
                    if hasattr(message, 'document') and message.document:
                        has_media = True
                        doc = message.document
                        logger.info("DOCUMENT DETECTED: message.document is present")
                        logger.info(f"Document ID: {getattr(doc, 'id', 'N/A')}")
                        logger.info(f"Document MIME: {getattr(doc, 'mime_type', 'N/A')}")
                        
                    if not has_media:
                        logger.info("NO MEDIA DETECTED in this message")
                
                try:
                    message_data = {
                        'id': message.id,
                        'date': message.date.isoformat() if message.date else None,
                        'message': message.message,
                        'views': getattr(message, 'views', None),
                        'forwards': getattr(message, 'forwards', None),
                        'media': None
                    }
                    yield message_data
                except Exception as e:
                    logger.error(f"Error processing message {getattr(message, 'id', 'unknown')}: {str(e)}")
                    continue
        except Exception as e:
            logger.error(f"Error fetching messages from {channel_username}: {str(e)}")
            raise
    
    async def download_media(self, message: Any, channel_name: str) -> Optional[Dict[str, Any]]:
        """Download media from a message."""
        file_path = None
        try:
            logger.info(f"Starting media download for message ID: {getattr(message, 'id', 'unknown')}")
            
            # Check for different media locations
            media = None
            media_type = 'unknown'
            file_ext = ''
            
            # 1. Check message.media first
            if hasattr(message, 'media') and message.media:
                logger.info("Found media in message.media")
                media = message.media
                
                if hasattr(media, 'document'):
                    doc = media.document
                    mime_type = getattr(doc, 'mime_type', '').lower()
                    logger.info(f"Document MIME type: {mime_type}")
                    
                    if 'image/' in mime_type:
                        media_type = 'image'
                        file_ext = '.' + mime_type.split('/')[-1].split(';')[0]  # Handle cases like 'image/jpeg;'
                    elif 'video/' in mime_type:
                        media_type = 'video'
                        file_ext = '.' + mime_type.split('/')[-1].split(';')[0]
                    elif 'pdf' in mime_type:
                        media_type = 'document'
                        file_ext = '.pdf'
                    else:
                        # Try to get extension from document attributes
                        media_type = 'file'
                        file_ext = '.bin'
                        if hasattr(doc, 'attributes'):
                            for attr in doc.attributes:
                                if hasattr(attr, 'file_name') and attr.file_name:
                                    file_ext = os.path.splitext(attr.file_name)[1]
                                    break
                
                elif hasattr(media, 'photo'):
                    media_type = 'photo'
                    file_ext = '.jpg'
                    logger.info("Found photo in message.media")
            
            # 2. Check for direct document/photo attributes
            elif hasattr(message, 'document') and message.document:
                logger.info("Found document directly in message")
                media = message.document
                media_type = 'document'
                file_ext = '.bin'  # Default extension
                if hasattr(media, 'mime_type') and media.mime_type:
                    mime_type = media.mime_type.lower()
                    if 'pdf' in mime_type:
                        file_ext = '.pdf'
                    elif 'word' in mime_type:
                        file_ext = '.docx'
            
            elif hasattr(message, 'photo') and message.photo:
                logger.info("Found photo directly in message")
                media = message.photo
                media_type = 'photo'
                file_ext = '.jpg'
            
            if not media:
                logger.info("No downloadable media found in message")
                return None
            
            # Create media directory
            media_dir = os.path.join(self.base_dir, 'media', channel_name)
            os.makedirs(media_dir, exist_ok=True)
            logger.info(f"Saving media to directory: {media_dir}")
            
            # Create unique filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{media_type}_{timestamp}_{getattr(message, 'id', 'unknown')}{file_ext}"
            file_path = os.path.join(media_dir, filename)
            
            logger.info(f"Downloading media to: {file_path}")
            
            # Download the file
            try:
                if hasattr(media, 'download'):
                    await media.download(file=file_path)
                else:
                    await self.client.download_media(message, file=file_path)
                
                if not os.path.exists(file_path):
                    logger.error(f"Download failed: {file_path} does not exist after download")
                    return None
                    
                file_size = os.path.getsize(file_path)
                logger.info(f"Successfully downloaded {media_type} ({file_size} bytes) to {file_path}")
                
                return {
                    'type': media_type,
                    'path': str(file_path),
                    'url': f"/media/{channel_name}/{filename}",
                    'filename': filename,
                    'size': file_size,
                    'mime_type': getattr(media, 'mime_type', '')
                }
                
            except Exception as download_error:
                logger.error(f"Error during download: {str(download_error)}")
                # Try to clean up partially downloaded file
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.error(f"Error cleaning up file {file_path}: {str(e)}")
                return None
            
        except Exception as e:
            logger.error(f"Error in download_media: {str(e)}", exc_info=True)
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as cleanup_error:
                    logger.error(f"Error during cleanup: {str(cleanup_error)}")
            return None
