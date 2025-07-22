#!/usr/bin/env python3
"""
Run the Dagster pipeline for the Telegram data processing workflow.

This script provides a simple way to run the Dagster pipeline with proper
environment variable loading.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add the current directory to the path so we can import from dagster_pipeline
sys.path.append(str(Path(__file__).parent))

from dagster_pipeline.jobs.telegram_pipeline import telegram_job
from dagster import DagsterInstance

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

def main():
    """Run the Dagster pipeline."""
    print("Starting Telegram data pipeline...")
    
    # Execute the job in-process (simpler than using execute_job with ReconstructableJob)
    result = telegram_job.execute_in_process(
        run_config={
            "execution": {
                "config": {
                    "multiprocess": {
                        "config": {
                            "start_method": {"forkserver": {"enabled": True}},
                            "max_concurrent": 4
                        }
                    }
                }
            }
        }
    )
    
    if result.success:
        print("\n✅ Pipeline completed successfully!")
    else:
        print("\n❌ Pipeline failed!")
        print("Check the logs for more details.")
    
    return result

if __name__ == "__main__":
    main()
