# src/orchestrator.py
# This file contains the core application logic and workflow orchestration.

import os
import json
import logging
import asyncio
import random
import time
import csv
from datetime import datetime

from .extract import extract_posts_in_batches
from .transform import create_gemini_batch_job, check_gemini_batch_job, download_gemini_batch_results, transform_posts_live
from .state_management import get_storage_adapter
from .load import exporters
from .load.file_saver import save_export_file
from . import utils

logger = logging.getLogger(__name__)

# --- Helper Functions ---
def _split_posts_into_chunks(posts, max_size_mb=95):
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

def _save_pending_jobs(competitor_name, job_tracking_list):
    """Saves a list of pending job details to a JSON file."""
    try:
        workspace_folder = os.path.join('workspace', competitor_name)
        os.makedirs(workspace_folder, exist_ok=True)
        jobs_file_path = os.path.join(workspace_folder, "pending_jobs.json")
        with open(jobs_file_path, "w") as f:
            json.dump(job_tracking_list, f, indent=4)
        logger.info(f"Saved {len(job_tracking_list)} pending job(s) to '{jobs_file_path}'")
    except IOError as e:
        logger.error(f"Could not save pending jobs file: {e}")


def _save_raw_posts(posts, competitor_name, chunk_num=None):
    """Saves a list of posts to a temporary JSONL file, with chunk number if provided."""
    try:
        workspace_folder = os.path.join('workspace', competitor_name)
        os.makedirs(workspace_folder, exist_ok=True)
        
        filename = f"temp_posts_chunk_{chunk_num}.jsonl" if chunk_num else "temp_posts.jsonl"
        raw_posts_file_path = os.path.join(workspace_folder, filename)
        
        with open(raw_posts_file_path, "w") as f:
            for post in posts:
                f.write(json.dumps(post) + "\n")
        logger.info(f"Saved {len(posts)} raw posts to '{os.path.basename(raw_posts_file_path)}' for later processing.")
        return raw_posts_file_path
    except IOError as e:
        logger.error(f"Could not save raw posts to file: {e}")
        return None

def _load_configuration():
    """Loads and returns the application and competitor configurations."""
    try:
        with open('config/config.json', 'r') as f:
            app_config = json.load(f)
        with open('config/competitor_seed_data.json', 'r') as f:
            competitor_config = json.load(f)
        return app_config, competitor_config
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return None, None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing configuration file: {e}")
        return None, None

def _get_competitors_to_process(competitor_config, selected_competitor_name):
    """Filters and returns the list of competitors to be processed."""
    all_competitors = competitor_config.get('competitors', [])
    if not selected_competitor_name:
        return all_competitors
    for comp in all_competitors:
        if comp['name'].lower() == selected_competitor_name.lower():
            return [comp]
    logger.error(f"Competitor '{selected_competitor_name}' not found.")
    return []



# --- Main Workflow Functions ---

async def run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold, live_model, batch_model, app_config):
    """Scrapes the blog, chunks if necessary, and submits jobs."""
    name = competitor['name']
    all_posts = []
    async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all):
        all_posts.extend(batch)
    if not all_posts:
        return
    # This ensures the state file is created before any enrichment happens.
    if all_posts:
        storage_adapter = get_storage_adapter(app_config)
        storage_adapter.save(all_posts, name, mode='append')

    if len(all_posts) < batch_threshold and not scrape_all:
        logger.info(f"Number of new posts ({len(all_posts)}) is below threshold. Using live processing.")
        
        transformed_posts = await transform_posts_live(all_posts, live_model)
        storage_adapter = get_storage_adapter(app_config)
        storage_adapter.save(transformed_posts, name, mode='overwrite')
    else:
        await _submit_chunks_for_processing(competitor, all_posts, batch_model, app_config)


async def check_and_load_results(competitor, app_config, num_posts=0):
    """Checks all pending jobs for a competitor and loads results when all are complete."""
    name = competitor['name']
    workspace_folder = os.path.join('workspace', name)
    jobs_file_path = os.path.join(workspace_folder, "pending_jobs.json")

    with open(jobs_file_path, "r") as f:
        pending_jobs = json.load(f)

    logger.info(f"--- Status for '{name}': Found {len(pending_jobs)} pending job(s) ---")
    
    all_succeeded = True
    total_posts = 0
    """
    status_summary = {}

    for job_info in pending_jobs:
        job_id = job_info['job_id']
        status = check_gemini_batch_job(job_id, verbose=False)
        status_summary[job_id] = status
        total_posts += job_info['num_posts']
        if status != "JOB_STATE_SUCCEEDED":
            all_succeeded = False
    
    # Print summary
    for job_id, status in status_summary.items():
        logger.info(f"  - Job {job_id}: {status}")
    """
    statuses = []
    total_posts = 0
    for job_info in pending_jobs:
        job_id = job_info['job_id']
        status = check_gemini_batch_job(job_id, verbose=False)
        statuses.append(status)
        total_posts += job_info.get('num_posts', 0)
    
    # --- REFACTORED: Use the new summary helper ---
    summary_message, all_succeeded = utils.get_job_status_summary(statuses)
    logger.info(summary_message) # Print the clear, user-friendly summary

    if not all_succeeded:
        logger.info("--- Not all jobs have succeeded. Please check again later. ---")
        return

    # If all jobs succeeded, process the results
    logger.info(f"--- All {len(pending_jobs)} jobs for '{name}' succeeded! Consolidating results... ---")
    all_transformed_posts = []
    start_time = time.time()

    for job_info in pending_jobs:
        job_id = job_info['job_id']
        raw_posts_file_path = os.path.join(workspace_folder, job_info['raw_posts_file'])
        
        original_posts = None
        if os.path.exists(raw_posts_file_path):
            with open(raw_posts_file_path, "r") as f:
                original_posts = [json.loads(line) for line in f]
        
        chunk_results = download_gemini_batch_results(job_id, original_posts)
        all_transformed_posts.extend(chunk_results)
        
        # Clean up individual chunk files as we go
        if os.path.exists(raw_posts_file_path):
            os.remove(raw_posts_file_path)

    job_duration = time.time() - start_time
    _utils.utils.update_performance_log(job_duration, total_posts)

    if all_transformed_posts:
        storage_adapter = get_storage_adapter(app_config)
        storage_adapter.save(all_transformed_posts, name, mode='append')
    
    os.remove(jobs_file_path)
    logger.info(f"Cleaned up all temporary files for '{name}'.")

async def run_enrichment_process(competitor, batch_threshold, live_model, batch_model, app_config, all_posts_from_file, posts_to_enrich):
    """Enriches posts, chunks if necessary, and submits jobs."""
    if len(posts_to_enrich) < batch_threshold:
        enriched_posts = await transform_posts_live(posts_to_enrich, live_model)
        if enriched_posts:
            enriched_map = {post['url']: post for post in enriched_posts}
            final_posts = [enriched_map.get(post['url'], post) for post in all_posts_from_file]
            storage_adapter = get_storage_adapter(app_config)
            storage_adapter.save(final_posts, competitor['name'], mode='overwrite')
    else:
        await _submit_chunks_for_processing(competitor, posts_to_enrich, batch_model, app_config)

async def _submit_chunks_for_processing(competitor, posts, batch_model, app_config):
    """Helper to chunk posts, submit jobs, and prompt the user."""
    competitor_name = competitor['name']
    post_chunks = _split_posts_into_chunks(posts)

    if len(post_chunks) > 1:
        logger.info(f"Job for '{competitor_name}' is too large and has been split into {len(post_chunks)} chunks.")

    job_tracking_list = []
    for i, chunk in enumerate(post_chunks):
        logger.info(f"Submitting chunk {i+1}/{len(post_chunks)} with {len(chunk)} posts...")
        raw_posts_path = _save_raw_posts(chunk, competitor_name, chunk_num=i+1)
        if not raw_posts_path: continue

        job_id = create_gemini_batch_job(chunk, competitor_name, batch_model)
        if job_id:
            job_tracking_list.append({
                "job_id": job_id,
                "raw_posts_file": os.path.basename(raw_posts_path),
                "num_posts": len(chunk)
            })
        else:
            logger.error(f"Failed to submit chunk {i+1}. It will be skipped.")
            os.remove(raw_posts_path)
    
    if job_tracking_list:
        _save_pending_jobs(competitor_name, job_tracking_list)
        await _prompt_to_wait_for_job(competitor, len(posts), app_config)

def run_export_process(competitors_to_export, export_format, app_config):
    """Finds the latest CSV for each competitor and exports the combined data."""
    logger.info(f"--- Starting export process to {export_format.upper()} ---")
    
    all_posts_to_export = []
    
    for competitor in competitors_to_export:
        competitor_name = competitor['name']

        state_folder = os.path.join("state", competitor_name)
        state_filepath = os.path.join(state_folder, f"{competitor_name}_state.csv")

        if not os.path.isdir(state_folder):
            logger.warning(f"No data directory found for '{competitor_name}'. Skipping.")
            continue

        csv_files = [f for f in os.listdir(state_folder) if f.endswith('.csv') and f.startswith(competitor_name)]
        if not csv_files:
            logger.warning(f"No CSV file found for '{competitor_name}'. Skipping.")
            continue
            
        logger.info(f"Reading latest data for '{competitor_name}' from: {os.path.basename(state_filepath)}")
        
        with open(state_filepath,  mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for post in reader:
                post['competitor'] = competitor_name
                all_posts_to_export.append(post)

    if not all_posts_to_export:
        logger.warning("No data found to export.")
        return

    try:
        formatted_data = exporters.export_data(all_posts_to_export, export_format, app_config)
    except ValueError as e:
        logger.error(e)
        return
    # For gsheets, the returned data is a success message, not file content
    if export_format == 'gsheets':
        # For Google Sheets, the return value is a status message, so we just log it.
        logger.info(formatted_data)
    else:
        # For all other formats, call our new dedicated saver function.
        save_export_file(formatted_data, export_format, competitors_to_export)

async def _prompt_to_wait_for_job(competitor, num_posts, app_config):
    """Asks the user if they want to wait for a submitted batch job."""
    avg_speed = utils.get_performance_estimate()
    if avg_speed > 0:
        estimated_seconds = avg_speed * num_posts
        estimated_minutes = estimated_seconds / 60
        logger.info(f"Based on previous jobs, the estimated completion time is ~{estimated_minutes:.1f} minutes.")
    
    try:
        choice = input("? Do you want to start polling for the results now? (y/n): ").lower()
        if choice == 'y':
            await check_and_load_results(competitor, app_config, num_posts)
        else:
            logger.info("Exiting. You can check the job status later with the --check-job flag.")
    except (KeyboardInterrupt, EOFError):
        logger.info("\nExiting.")

async def _run_job_check_phase(competitors_to_process, app_config):
    """Discovers, summarizes, and executes checks for any pending batch jobs."""
    logger.info("--- Checking for any pending batch jobs... ---")
    all_jobs_to_check = []
    
    for competitor in competitors_to_process:
        jobs_file_path = os.path.join("workspace", competitor['name'], "pending_jobs.json")
        if os.path.exists(jobs_file_path):
            all_jobs_to_check.append(competitor)
        else:
            logger.info(f"No pending jobs found for: {competitor['name']}")
    
    if all_jobs_to_check:
        logger.info(f"Found pending jobs for: {', '.join([c['name'] for c in all_jobs_to_check])}. Processing them now.")
        for competitor in all_jobs_to_check:
            await check_and_load_results(competitor, app_config)

async def run_pipeline(args):
    """The primary orchestration function that executes the ETL workflow."""
    app_config, competitor_config = _load_configuration()
    if not app_config or not competitor_config:
        return
        
    batch_threshold = app_config.get('batch_threshold', 10)
    live_model = app_config.get('models', {}).get('live', 'gemini-2.0-flash')
    batch_model = app_config.get('models', {}).get('batch', 'gemini-2.0-flash-lite')

    competitors_to_process = _get_competitors_to_process(competitor_config, args.competitor)
    if not competitors_to_process:
        return

    if args.check_job:
        await _run_job_check_phase(competitors_to_process, app_config)
            
    elif args.export:
        await _run_job_check_phase(competitors_to_process, app_config)
        run_export_process(competitors_to_process, args.export, app_config)

    elif args.enrich:
        logger.info("Discovering posts to enrich...")
        enrichment_plan = []
        for competitor in competitors_to_process:
            state_folder = os.path.join("state", competitor['name'])
            if not os.path.isdir(state_folder):
                continue

            # In enrichment, we always look for the single state file
            state_filepath = os.path.join(state_folder, f"{competitor['name']}_state.csv")
            if not os.path.exists(state_filepath):
                continue

            all_posts, to_enrich = [], []
            with open(state_filepath, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for post in reader:
                    all_posts.append(post)
                    if post.get('summary') == 'N/A' or post.get('seo_keywords') == 'N/A':
                        to_enrich.append(post)
            
            if to_enrich:
                enrichment_plan.append({
                    "competitor": competitor,
                    "posts_to_enrich": to_enrich,
                    "all_posts_from_file": all_posts
                })

        if not enrichment_plan:
            logger.info("No posts found that require enrichment.")
        else:
            logger.info("Enrichment Plan:")
            for item in enrichment_plan:
                logger.info(f"  - Will enrich {len(item['posts_to_enrich'])} posts for '{item['competitor']['name']}'.")

            for item in enrichment_plan:
                await run_enrichment_process(
                    item['competitor'], batch_threshold, live_model, batch_model, app_config,
                    item['all_posts_from_file'], item['posts_to_enrich']
                )
    else:
        for competitor in competitors_to_process:
            await run_scrape_and_submit(competitor, args.days, args.all, batch_threshold, live_model, batch_model, app_config)
            
    if args.enrich:
        process_name = "Enrichment"
    elif args.check_job:
        process_name = "Job Check"
    elif args.export:
        process_name = f"Export to {args.export.upper()}"
    else:
        process_name = "Scraping"

    logger.info(f"\n--- {process_name} process completed ---")