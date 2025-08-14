# src/utils.py
# This file contains shared helper functions for the application.

import logging

logger = logging.getLogger(__name__)

def get_job_status_summary(status_list):
    """
    Analyzes a list of job status strings and returns a user-friendly
    summary message and a boolean indicating if all jobs succeeded.
    """
    total_jobs = len(status_list)
    succeeded_jobs = status_list.count("JOB_STATE_SUCCEEDED")
    failed_jobs = sum(1 for s in status_list if s in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"])
    
    if failed_jobs > 0:
        summary = f"  - {failed_jobs}/{total_jobs} job(s) failed. Please check the job logs in the Google Cloud Console."
        return summary, False # Not all succeeded

    if succeeded_jobs == total_jobs:
        # This message is now logged from the orchestrator, so we can be brief here.
        return f"  - All {total_jobs}/{total_jobs} jobs succeeded.", True # All succeeded

    # If we reach here, it means some jobs are still pending or running
    pending_jobs = total_jobs - succeeded_jobs
    summary = f"  - {succeeded_jobs}/{total_jobs} jobs have succeeded. Still waiting for {pending_jobs} job(s) to complete."
    return summary, False # Not all succeeded yet

def get_batch_status_report(statuses):
    """
    Analyzes a list of job statuses and returns a comprehensive report.

    Args:
        statuses (list): A list of job status strings from the API.

    Returns:
        tuple: A tuple containing:
            - all_succeeded (bool): True if all jobs have succeeded, False otherwise.
            - summary_message (str): A user-friendly summary of the statuses.
    """
    if not statuses:
        return False, "No job statuses found to report."

    total_jobs = len(statuses)
    succeeded_count = statuses.count("JOB_STATE_SUCCEEDED")
    failed_count = sum(1 for s in statuses if s in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"])
    
    # Case 1: One or more jobs have failed permanently
    if failed_count > 0:
        summary = f"{failed_count}/{total_jobs} job(s) have failed. Please check the job logs in the Google Cloud Console."
        return False, summary

    # Case 2: All jobs have succeeded
    if succeeded_count == total_jobs:
        summary = f"All {total_jobs}/{total_jobs} jobs have succeeded."
        return True, summary

    # Case 3: Jobs are still in progress
    pending_count = total_jobs - succeeded_count
    summary = f"{succeeded_count}/{total_jobs} jobs have succeeded. Still waiting for {pending_count} job(s) to complete."
    return False, summary


def get_performance_estimate():
    """Reads the performance log and returns the average seconds per post."""
    try:
        with open('config/performance_log.json', 'r') as f:
            log = json.load(f)
        return log.get('average_seconds_per_post', 5.0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 5.0

def update_performance_log(job_duration_seconds, num_posts):
    """Updates the performance log with data from a completed job."""
    try:
        try:
            with open('config/performance_log.json', 'r') as f:
                log = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            log = {"total_posts_processed": 0, "total_time_seconds": 0}

        log['total_posts_processed'] += num_posts
        log['total_time_seconds'] += job_duration_seconds
        
        if log['total_posts_processed'] > 0:
            log['average_seconds_per_post'] = round(log['total_time_seconds'] / log['total_posts_processed'], 2)

        with open('config/performance_log.json', 'w') as f:
            json.dump(log, f, indent=4)
        logger.info("Performance log updated.")
    except (IOError, TypeError) as e:
        logger.warning(f"Could not update performance log: {e}")