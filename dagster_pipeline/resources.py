from dagster import resource
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@resource
class PostgresResource:
    def __init__(self, init_context):
        self.config = {
            "db_host": os.getenv("POSTGRES_HOST", "localhost"),
            "db_port": os.getenv("POSTGRES_PORT", "5432"),
            "db_name": os.getenv("POSTGRES_DB", "telegram"),
            "db_user": os.getenv("POSTGRES_USER", "postgres"),
            "db_password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }

@resource
class TelegramResource:
    def __init__(self, init_context):
        self.config = {
            "api_id": os.getenv("TELEGRAM_APP_ID"),
            "api_hash": os.getenv("TELEGRAM_API_HASH"),
            "phone": os.getenv("TELEGRAM_PHONE"),
        }

# Resource instances for Dagster
postgres_resource = PostgresResource(None)
telegram_resource = TelegramResource(None)