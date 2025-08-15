import os
import logging
from dotenv import load_dotenv
from google.genai.client import Client
from google.genai.errors import APIError

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='INFO: %(message)s')
load_dotenv()

def cleanup_all_batch_jobs():
    """
    Lists all batch jobs and attempts to cancel any that are not in a
    final state.
    """
    try:
        # Initialize the Gemini API client
        client = Client()
        logging.info("Fetching all batch jobs...")
        
        # Get a list of all batch jobs
        all_jobs = client.batches.list()
        
        # Convert the iterator to a list to check if it's empty
        job_list = list(all_jobs)

        if not job_list:
            logging.info("No batch jobs found.")
            return

        logging.info(f"Found {len(job_list)} total jobs. Attempting to cancel active jobs...")
        
        for job in job_list:
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
                    # Cancel the job
                    client.batches.cancel(name=job.name)
                    logging.info(f"    ... Successfully cancelled.")
                except APIError as e:
                    logging.error(f"    ... Could not cancel job {job.name}: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error("Please ensure your GEMINI_API_KEY is set correctly in the .env file.")

if __name__ == "__main__":
    cleanup_all_batch_jobs()
    logging.info("\n--- Cleanup process completed ---")