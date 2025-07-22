from dagster import Definitions, ScheduleDefinition
from .jobs.telegram_pipeline import telegram_job
from .resources import postgres_resource, telegram_resource

# Define a daily schedule for the pipeline
daily_telegram_schedule = ScheduleDefinition(
    job=telegram_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight UTC
    execution_timezone="UTC"
)

defs = Definitions(
    jobs=[telegram_job],
    schedules=[daily_telegram_schedule],
    resources={
        "postgres": postgres_resource,
        "telegram": telegram_resource,
    },
)