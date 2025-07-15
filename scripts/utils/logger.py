import logging
import os
from pathlib import Path
from typing import Optional

def setup_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with both file and console handlers.
    
    Args:
        name: Name of the logger
        log_level: Logging level (default: logging.INFO)
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Prevent adding handlers multiple times in case of module reloading
    if logger.handlers:
        return logger
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # File handler for logging to a file
    file_handler = logging.FileHandler(log_dir / 'telegram_scraper.log')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    
    # Console handler for logging to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create a default logger instance
default_logger = setup_logger(__name__)
