import os
import logging
from dotenv import load_dotenv
from src.api_connector import GeminiAPIConnector # <-- Use the centralized connector

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='INFO: %(message)s')
load_dotenv()

# These are the states where a job is considered "finished"

ongoing_states = set([
    'JOB_STATE_PENDING',
    'JOB_STATE_RUNNING',
])
    

completed_states = set([
    'JOB_STATE_SUCCEEDED',
    'JOB_STATE_FAILED',
    'JOB_STATE_CANCELLED',
    'JOB_STATE_EXPIRED',
])
    
class BatchCleaner:

    def __init__(self):
        self.connector = _connect_to_gemini()
        self.batch_jobs_list = _get_all_batch_jobs_list(self)

    def _connect_to_gemini():
        """
        Instantiates the connection and passes the connection handler
        """
        try:
            # Initialize the Gemini API client
            connector = GeminiAPIConnector()
            if not connector.client:
                logging.error("API connector could not be initialized. Please check your API key.")
                return
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            logging.error("Please ensure your GEMINI_API_KEY is set correctly in the .env file.")
        return connector    

    def _get_all_batch_jobs_list(self):
        """
        List all the batch job in the server, regarding of the status they are
        """
        
        logging.info("Fetching all batch jobs...")
        
        # Use the connector method to list jobs
        all_jobs = self.connector.list_batch_jobs()
        
        if not all_jobs:
            logging.info("No batch jobs found.")
            return

        logging.info(f"Found {len(all_jobs)} total jobs.")
        
        return all_jobs

    def _delete_completed_batch_jobs(self):
        """
        Delete the completed jobs form 
        """
        for batch_job in self.batch_job_list:

            if batch_job.state.name in completed_states:
                try:
                    logging.info(f"  - Attempting to delete file for job {batch_job.name} (Status: {batch_job.state.name})...")
                    self.connector.delete_job_file(batch_job.name)
                    logging.info(f"    ... Successfully deleted.")
                except Exception as e:
                    logging.error(f"    ... Could not delete job {batch_job.name}: {e}")
    
    def _cancel_batch_jobs(self):
        """
        """
        for batch_job in self.batch_job_list:

            if batch_job.state.name in ongoing_states:
                try:
                    logging.info(f"  - Attempting to cancel job {batch_job.name} (Status: {batch_job.state.name})...")
                    # Use the connector method to cancel the job
                    self.connector.cancel_batch_job(batch_job_id=batch_job.name)
                    logging.info(f"    ... Successfully cancelled.")
                except Exception as e:
                    logging.error(f"    ... Could not cancel job {batch_job.name}: {e}")



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
        all_jobs = _get_all_batch_jobs_list()
        
        if not all_jobs:
            logging.info("No batch jobs found.")
            return

        logging.info(f"Attempting to cancel active jobs...")
        
        for job in all_jobs:

            if job.state.name in completed_states:
                try:
                    logging.info(f"  - Attempting to delete file for job {job.name} (Status: {job.state.name})...")
                    connector.delete_job_file(job.name)
                    logging.info(f"    ... Successfully deleted.")
                    except Exception as e:
                    logging.error(f"    ... Could not delete job {job.name}: {e}")
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