import os
import logging
from dotenv import load_dotenv
from src.api_connector import GeminiAPIConnector # <-- Use the centralized connector

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='INFO: %(message)s')
load_dotenv()

def cleanup_all_batch_jobs():
    """
    Lists all batch jobs and attempts to cancel any that are not in a
    final state, using the centralized API connector.
    """
    try:
        # Initialize the Gemini API client
        connector = GeminiAPIConnector()
        if not connector.client:
            logging.error("API connector could not be initialized. Please check your API key.")
            return

        logging.info("Fetching all batch jobs...")
        
        # Use the connector method to list jobs
        all_jobs = connector.list_batch_jobs()
        
        if not all_jobs:
            logging.info("No batch jobs found.")
            return

        logging.info(f"Found {len(all_jobs)} total jobs. Attempting to cancel active jobs...")
        
        for job in all_jobs:
            # These are the states where a job is considered "finished"
            terminal_states = {
                'JOB_STATE_SUCCEEDED',
                'JOB_STATE_FAILED',
                'JOB_STATE_CANCELLED',
                'JOB_STATE_EXPIRED'
            }

            if job.state.name in terminal_states:
                logging.info(f"  - Skipping job {job.name} (Status: {job.state.name})")
            else:
                try:
                    logging.info(f"  - Attempting to cancel job {job.name} (Status: {job.state.name})...")
                    # Use the connector method to cancel the job
                    connector.cancel_batch_job(job_id=job.name)
                    logging.info(f"    ... Successfully cancelled.")
                except Exception as e:
                    logging.error(f"    ... Could not cancel job {job.name}: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error("Please ensure your GEMINI_API_KEY is set correctly in the .env file.")

if __name__ == "__main__":
    cleanup_all_batch_jobs()
    logging.info("\n--- Cleanup process completed ---")