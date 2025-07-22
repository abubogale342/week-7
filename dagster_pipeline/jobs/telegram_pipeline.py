from dagster import job, op, get_dagster_logger, graph
from pathlib import Path
import subprocess
import os
from datetime import datetime
from typing import Dict, Any

# Import resources
from ..resources import postgres_resource, telegram_resource

@op(required_resource_keys={"telegram"})
def scrape_telegram_data(context):
    """Scrape data from Telegram channels."""
    logger = get_dagster_logger()
    logger.info("Starting Telegram data scraping...")
    
    # Run your scraping script
    script_path = Path(__file__).parent.parent.parent / "scripts" / "scraping.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Scraping script not found at {script_path}")
    
    result = subprocess.run(
        ["python", str(script_path)], 
        capture_output=True, 
        text=True,
        env=dict(os.environ, **{
            "TELEGRAM_APP_ID": context.resources.telegram["api_id"],
            "TELEGRAM_API_HASH": context.resources.telegram["api_hash"],
            "TELEGRAM_PHONE": context.resources.telegram["phone"]
        })
    )
    
    if result.returncode != 0:
        logger.error(f"Scraping failed: {result.stderr}")
        raise Exception(f"Scraping failed: {result.stderr}")
    
    logger.info("Successfully scraped Telegram data")
    return {"status": "success", "timestamp": datetime.utcnow().isoformat()}

@op(required_resource_keys={"postgres"})
def load_raw_to_postgres(context, previous_result):
    """Load raw data into PostgreSQL."""
    logger = get_dagster_logger()
    logger.info("Loading raw data to PostgreSQL...")
    
    script_path = Path(__file__).parent.parent.parent / "scripts" / "load_to_postgres.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Load script not found at {script_path}")
    
    result = subprocess.run(
        ["python", str(script_path)], 
        capture_output=True, 
        text=True,
        env=dict(os.environ, **{
            "POSTGRES_HOST": context.resources.postgres["db_host"],
            "POSTGRES_PORT": context.resources.postgres["db_port"],
            "POSTGRES_DB": context.resources.postgres["db_name"],
            "POSTGRES_USER": context.resources.postgres["db_user"],
            "POSTGRES_PASSWORD": context.resources.postgres["db_password"]
        })
    )
    
    if result.returncode != 0:
        logger.error(f"Loading to PostgreSQL failed: {result.stderr}")
        raise Exception(f"Loading to PostgreSQL failed: {result.stderr}")
    
    logger.info("Successfully loaded data to PostgreSQL")
    return {"status": "success", "timestamp": datetime.utcnow().isoformat()}

@op(required_resource_keys={"postgres"})
def run_dbt_transformations(context, previous_result):
    """Run dbt transformations."""
    logger = get_dagster_logger()
    logger.info("Running dbt transformations...")
    
    # Change to your dbt project directory
    dbt_project = Path(__file__).parent.parent.parent / "telegram_data"
    if not dbt_project.exists():
        raise FileNotFoundError(f"dbt project not found at {dbt_project}")
    
    env = dict(os.environ, **{
        "DBT_HOST": context.resources.postgres["db_host"],
        "DBT_PORT": context.resources.postgres["db_port"],
        "DBT_DATABASE": context.resources.postgres["db_name"],
        "DBT_USER": context.resources.postgres["db_user"],
        "DBT_PASSWORD": context.resources.postgres["db_password"]
    })
    
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", str(dbt_project)], 
        cwd=str(dbt_project),
        capture_output=True, 
        text=True,
        env=env
    )
    
    if result.returncode != 0:
        logger.error(f"dbt transformations failed: {result.stderr}")
        raise Exception(f"dbt transformations failed: {result.stderr}")
    
    logger.info("Successfully ran dbt transformations")
    return {"status": "success", "timestamp": datetime.utcnow().isoformat()}

@op(required_resource_keys={"postgres"})
def run_yolo_enrichment(context, previous_result):
    """Run YOLO object detection on images."""
    logger = get_dagster_logger()
    logger.info("Running YOLO enrichment...")
    
    script_path = Path(__file__).parent.parent.parent / "scripts" / "detect_objects.py"
    if not script_path.exists():
        logger.warning(f"YOLO script not found at {script_path}, skipping...")
        return {"status": "skipped", "reason": "YOLO script not found", "timestamp": datetime.utcnow().isoformat()}
    
    result = subprocess.run(
        ["python", str(script_path)], 
        capture_output=True, 
        text=True,
        env=dict(os.environ, **{
            "POSTGRES_HOST": context.resources.postgres["db_host"],
            "POSTGRES_PORT": context.resources.postgres["db_port"],
            "POSTGRES_DB": context.resources.postgres["db_name"],
            "POSTGRES_USER": context.resources.postgres["db_user"],
            "POSTGRES_PASSWORD": context.resources.postgres["db_password"]
        })
    )
    
    if result.returncode != 0:
        logger.error(f"YOLO enrichment failed: {result.stderr}")
        raise Exception(f"YOLO enrichment failed: {result.stderr}")
    
    logger.info("Successfully ran YOLO enrichment")
    return {"status": "success", "timestamp": datetime.utcnow().isoformat()}

@graph
def telegram_pipeline():
    """Main pipeline for Telegram data processing."""
    # Define the execution order
    scraped = scrape_telegram_data()
    loaded = load_raw_to_postgres(scraped)
    transformed = run_dbt_transformations(loaded)
    run_yolo_enrichment(transformed)

# Create a job from the graph
telegram_job = telegram_pipeline.to_job(
    name="telegram_pipeline_job",
    resource_defs={
        "postgres": postgres_resource,
        "telegram": telegram_resource
    }
)
