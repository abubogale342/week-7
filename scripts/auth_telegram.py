#!/usr/bin/env python3
"""
Telegram Authentication Script

This script handles the initial authentication with Telegram's servers.
It will prompt for the verification code when needed.
"""

import asyncio
import os
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from config.config import config
from utils.logger import default_logger as logger

async def authenticate():
    """Authenticate with Telegram servers."""
    client = None
    try:
        # Get Telegram config
        telegram_config = config.get_telegram_config()
        
        # Get the absolute path for the session file
        session_path = os.path.abspath(config.SESSION_NAME)
        
        # Initialize client
        client = TelegramClient(
            session_path,
            int(telegram_config['api_id']),
            telegram_config['api_hash'],
            device_model="Telegram Scraper",
            app_version="1.0.0",
            system_version="Linux",
            system_lang_code="en"
        )
        
        # Connect to Telegram
        await client.connect()
        
        # Check if we're already authorized
        if not await client.is_user_authorized():
            # Send code request
            phone = telegram_config['phone']
            await client.send_code_request(phone)
        
            # Ask for the code
            print("\nA verification code has been sent to your Telegram account.")
            print("Please enter the code you received (format: 1 2 3 4 5): ")
            code = input("Code: ").strip()
        
            try:
                # Sign in with the code
                await client.sign_in(phone, code)
                print("Successfully signed in!")
            except SessionPasswordNeededError:
                # 2FA is enabled, ask for password
                password = input("\nEnter your 2FA password: ").strip()
                await client.sign_in(password=password)
                print("Successfully signed in with 2FA!")
        else:
            print("Already authorized. No need to sign in again.")
        
        # Get and display some basic info
        me = await client.get_me()
        print(f"\nLogged in as: {me.first_name} ({me.phone})")
        
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}", exc_info=True)
        return False
    finally:
        if client:
            await client.disconnect()
    
    return True

if __name__ == "__main__":
    try:
        # Create necessary directories
        session_dir = os.path.dirname(os.path.abspath(config.SESSION_NAME))
        if session_dir:  # Only create directory if SESSION_NAME includes a path
            os.makedirs(session_dir, exist_ok=True)
        
        # Run the authentication
        asyncio.run(authenticate())
    except KeyboardInterrupt:
        print("\nAuthentication cancelled by user.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
