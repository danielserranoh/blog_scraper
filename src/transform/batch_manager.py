# src/transform/batch_manager.py
# This file contains the high-level logic for managing Gemini Batch jobs.

import os
import json
import logging
import asyncio
import time
import csv
from datetime import datetime

# Import live enrichment and other helpers
from . import live
from src import utils
from src.state_management.state_manager import StateManager
from src.api_connector import GeminiAPIConnector

logger = logging.getLogger(__name__)

class BatchJobManager:
    """
    Manages the entire lifecycle of one or more Gemini Batch jobs, from
    submission to result processing.
    """
    def __init__(self, app_config):
        self.api_connector = GeminiAPIConnector()
        self.state_manager = StateManager(app_config)
    
    async def submit_new_jobs(self, competitor, posts, batch_model, app_config, source_raw_filepath, wait):
        """
        Chunks posts, submits them as jobs, and renames files transactionally.
        """
        competitor_name = competitor['name']
        workspace_folder = os.path.join('workspace', competitor_name)
        
        post_chunks = self._split_posts_into_chunks(posts)

        if len(post_chunks) > 1:
            logger.info(f"Job for '{competitor_name}' is large and has been split into {len(post_chunks)} chunks.")

        job_tracking_list = []
        for i, chunk in enumerate(post_chunks):
            logger.info(f"Submitting chunk {i+1}/{len(post_chunks)}...")
            job_id = self.api_connector.create_batch_job(chunk, competitor_name, batch_model)
            
            if job_id:
                unsubmitted_path = self._save_raw_posts(chunk, competitor_name, chunk_num=i+1)
                if not unsubmitted_path: continue
                submitted_path = os.path.join(workspace_folder, f"temp_posts_chunk_{i+1}.jsonl")
                os.rename(unsubmitted_path, submitted_path)
                
                job_tracking_list.append({
                    "job_id": job_id,
                    "raw_posts_file": os.path.basename(submitted_path),
                    "num_posts": len(chunk)
                })
            else:
                logger.error(f"Failed to submit chunk {i+1}. The unsubmitted file has been left in the workspace for the next run.")

        if job_tracking_list:
            self._save_pending_jobs(competitor_name, job_tracking_list, source_raw_filepath)
            if wait:
                await self.check_and_load_results(competitor, app_config)
            else:
                await self._prompt_to_wait_for_job(competitor, len(posts), app_config)


    async def check_and_load_results(self, competitor, app_config):
        """
        Orchestrates the checking of jobs and the loading of results.
        """
        name = competitor['name']
        workspace_folder = os.path.join('workspace', name)
        jobs_file_path = os.path.join(workspace_folder, "pending_jobs.json")
        source_raw_filepath = None

        if not os.path.exists(jobs_file_path):
            logger.info(f" ℹ️ No pending jobs found for '{name}'.")
            return
            
        try:
            with open(jobs_file_path, "r") as f:
                jobs_data = json.load(f)
                pending_jobs = jobs_data.get('jobs', [])
                source_raw_filepath = jobs_data.get('source_raw_filepath')
        except (FileNotFoundError, json.JSONDecodeError):
            logger.error(f"Could not read pending jobs file for '{name}'. Skipping.")
            return

        statuses = self._poll_job_statuses(pending_jobs)
        summary_message, all_succeeded = utils.get_job_status_summary(statuses)
        
        logger.info(f"--- Status for '{name}': {len(pending_jobs)} job(s) ---")
        logger.info(summary_message)

        if all_succeeded:
            try:
                logger.info(f"--- All jobs for '{name}' succeeded! Consolidating and updating state... ---")
                await self._consolidate_and_save_results(competitor, pending_jobs, app_config, source_raw_filepath)
                
                self._cleanup_workspace(competitor, pending_jobs)
            except Exception as e:
                logger.error(f"A critical error occurred during result processing for '{name}': {e}")
                logger.error("Temporary workspace files have been preserved for manual inspection.")
        else:
            logger.info("--- Not all jobs have finished processing. Please check again later. ---")


    # --- Internal Helper Methods (moved from orchestrator.py) ---
    def _save_raw_posts(self, posts, competitor_name, chunk_num=None):
        """Saves a list of posts to a temporary JSONL file, with chunk number if provided."""
        try:
            workspace_folder = os.path.join('workspace', competitor_name)
            os.makedirs(workspace_folder, exist_ok=True)
            
            filename = f"unsubmitted_posts_chunk_{chunk_num}.jsonl" if chunk_num else "unsubmitted_posts.jsonl"
            raw_posts_file_path = os.path.join(workspace_folder, filename)
            
            with open(raw_posts_file_path, "w") as f:
                for post in posts:
                    f.write(json.dumps(post) + "\n")
            logger.info(f"Saved {len(posts)} raw posts to '{os.path.basename(raw_posts_file_path)}' for later processing.")
            return raw_posts_file_path
        except IOError as e:
            logger.error(f"Could not save raw posts to file: {e}")
            return None

    def _split_posts_into_chunks(self, posts, max_size_mb=95):
        """
        Splits a list of posts into chunks, ensuring the estimated size of each
        chunk's JSONL file is below the max_size_mb limit.
        """
        max_size_bytes = max_size_mb * 1024 * 1024
        chunks = []
        current_chunk = []
        current_size = 0

        for post in posts:
            post_size = len(json.dumps(post).encode('utf-8')) + 1

            if current_size + post_size > max_size_bytes and current_chunk:
                chunks.append(current_chunk)
                current_chunk = [post]
                current_size = post_size
            else:
                current_chunk.append(post)
                current_size += post_size

        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

    def _save_pending_jobs(self, competitor_name, job_tracking_list, source_raw_filepath):
        """Saves a list of pending job details to a JSON file."""
        try:
            workspace_folder = os.path.join('workspace', competitor_name)
            os.makedirs(workspace_folder, exist_ok=True)
            jobs_file_path = os.path.join(workspace_folder, "pending_jobs.json")
            
            data_to_save = {
                "source_raw_filepath": source_raw_filepath,
                "jobs": job_tracking_list
            }

            with open(jobs_file_path, "w") as f:
                json.dump(data_to_save, f, indent=4)
            logger.info(f"Saved {len(job_tracking_list)} pending job(s) to '{jobs_file_path}'")
        except IOError as e:
            logger.error(f"Could not save pending jobs file: {e}")

    async def _prompt_to_wait_for_job(self, competitor, num_posts, app_config):
        """Asks the user if they want to wait for a submitted batch job."""
        avg_speed = utils.get_performance_estimate()
        if avg_speed > 0:
            estimated_seconds = avg_speed * num_posts
            estimated_minutes = estimated_seconds / 60
            logger.info(f"Based on previous jobs, the estimated completion time is ~{estimated_minutes:.1f} minutes.")
        
        try:
            choice = input("? Do you want to start polling for the results now? (y/n): ").lower()
            if choice == 'y':
                await self.check_and_load_results(competitor, app_config)
            else:
                logger.info("Exiting. You can check the job status later with the --check-job flag.")
        except (KeyboardInterrupt, EOFError):
            logger.info("\nExiting.")

    def _poll_job_statuses(self, pending_jobs):
        """Polls the API for the status of each job in the list."""
        statuses = []
        for job_info in pending_jobs:
            status = self.api_connector.check_batch_job(job_info['job_id'], verbose=False)
            statuses.append(status)
        return statuses

    def _cleanup_workspace(self, competitor, pending_jobs):
        """Deletes all temporary files after processing is complete."""
        name = competitor['name']
        workspace_folder = os.path.join('workspace', name)
        jobs_file_path = os.path.join(workspace_folder, "pending_jobs.json")

        for job_info in pending_jobs:
            raw_posts_file_path = os.path.join(workspace_folder, job_info['raw_posts_file'])
            if os.path.exists(raw_posts_file_path):
                os.remove(raw_posts_file_path)
        
        os.remove(jobs_file_path)
        logger.info(f"Cleaned up all temporary files for '{name}'.")


    async def _consolidate_and_save_results(self, competitor, pending_jobs, app_config, source_raw_filepath):
        """Downloads results for all successful jobs, consolidates them, and updates the state file."""
        name = competitor['name']
        workspace_folder = os.path.join('workspace', name)
        
        all_enriched_posts = []
        total_posts = sum(job.get('num_posts', 0) for job in pending_jobs)
        start_time = time.time()

        for job_info in pending_jobs:
            job_id = job_info['job_id']
            raw_posts_file_path = os.path.join(workspace_folder, job_info['raw_posts_file'])
            
            original_posts_chunk = None
            if os.path.exists(raw_posts_file_path):
                # --- UPDATED: Handle both JSON and CSV files ---
                if raw_posts_file_path.endswith('.jsonl'):
                    with open(raw_posts_file_path, "r") as f:
                        original_posts_chunk = [json.loads(line) for line in f]
                elif raw_posts_file_path.endswith('.csv'):
                    with open(raw_posts_file_path, mode='r', newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        original_posts_chunk = list(reader)

            chunk_results = self.api_connector.download_batch_results(job_id, original_posts_chunk)
            all_enriched_posts.extend(chunk_results)

        job_duration = time.time() - start_time
        utils.update_performance_log(job_duration, total_posts)

        if all_enriched_posts:
            # Load the original raw data from the saved source file
            original_posts_map = {}
            original_posts_from_file = []
            
            if source_raw_filepath and os.path.exists(source_raw_filepath):
                # --- UPDATED: Handle both JSON and CSV files ---
                if source_raw_filepath.endswith('.json'):
                    with open(source_raw_filepath, 'r', encoding='utf-8') as f:
                        original_posts_from_file = json.load(f)
                elif source_raw_filepath.endswith('.csv'):
                    with open(source_raw_filepath, mode='r', newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        original_posts_from_file = list(reader)

                for post in original_posts_from_file:
                    original_posts_map[post['url']] = post
            else:
                logger.warning(f"Could not find the original source file at {source_raw_filepath}. Reconstructing data.")
                # If the source file is missing, reconstruct the original posts map
                for post in all_enriched_posts:
                    try:
                        # Use the metadata from the API request to reconstruct
                        original_posts_map[post['url']] = post
                    except KeyError:
                        logger.error(f"Post is missing a 'url' key, cannot be properly merged: {post}")
                        continue
            
            # Now, merge the enriched data with the original data
            for enriched_post in all_enriched_posts:
                original_url = enriched_post.get('url')
                if original_url in original_posts_map:
                    original_posts_map[original_url].update({
                        'summary': enriched_post.get('summary', 'N/A'),
                        'seo_keywords': enriched_post.get('seo_keywords', 'N/A'),
                        'funnel_stage': enriched_post.get('funnel_stage', 'N/A')
                    })
            
            final_posts = list(original_posts_map.values())

            # Use the StateManager to save the processed data
            self.state_manager.save_processed_data(final_posts, name, os.path.basename(source_raw_filepath))