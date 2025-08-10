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
from .load import get_storage_adapter
from . import exporters

logger = logging.getLogger(__name__)

# --- Helper Functions ---
def _save_job_id(competitor_name, job_id):
    """Saves a batch job ID to a text file in the workspace directory."""
    try:
        workspace_folder = os.path.join('workspace', competitor_name)
        os.makedirs(workspace_folder, exist_ok=True)
        job_id_file_path = os.path.join(workspace_folder, "batch_job_id.txt")
        with open(job_id_file_path, "w") as f:
            f.write(job_id)
        logger.info(f"Submitted Gemini batch job: {job_id}. Job ID saved to '{job_id_file_path}'")
    except IOError as e:
        logger.error(f"Could not save job ID {job_id} to file: {e}")

def _save_raw_posts(posts, competitor_name):
    """Saves a list of posts to a temporary JSONL file in the workspace."""
    try:
        workspace_folder = os.path.join('workspace', competitor_name)
        os.makedirs(workspace_folder, exist_ok=True)
        raw_posts_file_path = os.path.join(workspace_folder, "temp_posts.jsonl")
        with open(raw_posts_file_path, "w") as f:
            for post in posts:
                f.write(json.dumps(post) + "\n")
        logger.info(f"Saved {len(posts)} raw posts to '{raw_posts_file_path}' for later processing.")
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

def _get_performance_estimate():
    """Reads the performance log and returns the average seconds per post."""
    try:
        with open('config/performance_log.json', 'r') as f:
            log = json.load(f)
        return log.get('average_seconds_per_post', 5.0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 5.0

def _update_performance_log(job_duration_seconds, num_posts):
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

# --- Main Workflow Functions ---

async def run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold, live_model, batch_model, app_config):
    """Scrapes the blog and decides whether to use live or batch processing."""
    name = competitor['name']
    all_posts = []
    async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all):
        all_posts.extend(batch)
    if not all_posts:
        return

    if len(all_posts) < batch_threshold and not scrape_all:
        transformed_posts = await transform_posts_live(all_posts, live_model)
        storage_adapter = get_storage_adapter(app_config)
        storage_adapter.save(transformed_posts, name)
    else:
        raw_posts_file_path = _save_raw_posts(all_posts, name)
        if not raw_posts_file_path:
            return
        job_id = create_gemini_batch_job(all_posts, name, batch_model)
        if job_id:
            _save_job_id(name, job_id)
            await _prompt_to_wait_for_job(competitor, len(all_posts), app_config)
        else:
            os.remove(raw_posts_file_path)

async def check_and_load_results(competitor, app_config, num_posts=0):
    """Checks for a saved job ID, polls the API, and loads results."""
    name = competitor['name']
    workspace_folder = os.path.join('workspace', name)
    job_id_file_path = os.path.join(workspace_folder, "batch_job_id.txt")
    raw_posts_file_path = os.path.join(workspace_folder, "temp_posts.jsonl")
    
    with open(job_id_file_path, "r") as f:
        job_id = f.read().strip()
    
    initial_delay, max_delay, multiplier, start_time = 30, 300, 2, time.time()
    delay = initial_delay
    
    while True:
        status = check_gemini_batch_job(job_id)
        if status not in ["JOB_STATE_PENDING", "JOB_STATE_RUNNING"]:
            break
        jitter = random.uniform(0, 5)
        wait_time = min(delay, max_delay) + jitter
        logger.info(f"Job for '{name}' is {status}. Waiting for approximately {int(wait_time)} seconds...")
        await asyncio.sleep(wait_time)
        delay *= multiplier
    
    if status == "JOB_STATE_SUCCEEDED":
        job_duration = time.time() - start_time
        logger.info(f"Gemini batch job '{job_id}' succeeded in {job_duration:.2f} seconds!")
        if num_posts > 0:
            _update_performance_log(job_duration, num_posts)

        original_posts = None
        if os.path.exists(raw_posts_file_path):
            with open(raw_posts_file_path, "r") as f:
                original_posts = [json.loads(line) for line in f]
        else:
            logger.warning(f"Original raw posts file not found: '{raw_posts_file_path}'. Recovering from download.")
        
        transformed_posts = download_gemini_batch_results(job_id, original_posts)
        if transformed_posts:
            storage_adapter = get_storage_adapter(app_config)
            storage_adapter.save(transformed_posts, name)
        else:
            logger.warning(f"No processed posts could be recovered for {name}.")

        os.remove(job_id_file_path)
        if os.path.exists(raw_posts_file_path):
            os.remove(raw_posts_file_path)
        logger.info(f"Cleaned up temporary files for job '{job_id}'")
    else:
        logger.error(f"Gemini batch job '{job_id}' failed or has an unknown status: {status}")

async def run_enrichment_process(competitor, batch_threshold, live_model, batch_model, app_config, all_posts_from_file, posts_to_enrich):
    """Executes the enrichment for a single competitor, assuming discovery is done."""
    if len(posts_to_enrich) < batch_threshold:
        enriched_posts = await transform_posts_live(posts_to_enrich, live_model)
        
        if enriched_posts:
            enriched_map = {post['url']: post for post in enriched_posts}
            final_posts = [enriched_map.get(post['url'], post) for post in all_posts_from_file]
            storage_adapter = get_storage_adapter(app_config)
            storage_adapter.save(final_posts, competitor['name'])
            logger.info(f"Successfully enriched {len(enriched_posts)} posts for '{competitor['name']}'.")
        else:
            logger.warning(f"Enrichment process failed for {competitor['name']}.")
    else:
        raw_posts_file_path = _save_raw_posts(posts_to_enrich, competitor['name'])
        if not raw_posts_file_path:
            return
        job_id = create_gemini_batch_job(posts_to_enrich, competitor['name'], batch_model)
        if job_id:
            _save_job_id(competitor['name'], job_id)
            await _prompt_to_wait_for_job(competitor, len(posts_to_enrich), app_config)
        else:
            logger.error(f"Failed to submit Gemini batch job for {competitor['name']}.")
            os.remove(raw_posts_file_path)

def run_export_process(competitors_to_export, export_format):
    """Finds the latest CSV for each competitor and exports the combined data."""
    logger.info(f"--- Starting export process to {export_format.upper()} ---")
    
    all_posts_to_export = []
    
    for competitor in competitors_to_export:
        competitor_name = competitor['name']
        state_folder = os.path.join("state", competitor_name)
        if not os.path.isdir(state_folder):
            logger.warning(f"No data directory found for '{competitor_name}'. Skipping.")
            continue

        csv_files = [f for f in os.listdir(state_folder) if f.endswith('.csv') and f.startswith(competitor_name)]
        if not csv_files:
            logger.warning(f"No CSV file found for '{competitor_name}'. Skipping.")
            continue
            
        latest_csv_path = os.path.join(state_folder, max(csv_files))
        logger.info(f"Reading latest data for '{competitor_name}' from: {os.path.basename(latest_csv_path)}")
        
        with open(latest_csv_path, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for post in reader:
                post['competitor'] = competitor_name
                all_posts_to_export.append(post)

    if not all_posts_to_export:
        logger.warning("No data found to export.")
        return

    try:
        formatted_data = exporters.export_data(all_posts_to_export, export_format)
    except ValueError as e:
        logger.error(e)
        return

    try:
        if len(competitors_to_export) > 1:
            base_filename = f"all_competitors-{datetime.now().strftime('%y%m%d')}"
        else:
            base_filename = f"{competitors_to_export[0]['name']}-{datetime.now().strftime('%y%m%d')}"
        
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        output_filepath = os.path.join(export_dir, f"{base_filename}.{export_format}")

        with open(output_filepath, "w", encoding="utf-8") as f:
            f.write(formatted_data)
        logger.info(f"Successfully exported {len(all_posts_to_export)} total posts to: {output_filepath}")
    except IOError as e:
        logger.error(f"Failed to write export file: {e}")

async def _prompt_to_wait_for_job(competitor, num_posts, app_config):
    """Asks the user if they want to wait for a submitted batch job."""
    avg_speed = _get_performance_estimate()
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
    logger.info("--- Checking for any pending batch jobs before proceeding... ---")
    
    jobs_to_check, no_jobs_found = [], []
    for competitor in competitors_to_process:
        job_id_file_path = os.path.join("workspace", competitor['name'], "batch_job_id.txt")
        if os.path.exists(job_id_file_path):
            jobs_to_check.append(competitor)
        else:
            no_jobs_found.append(competitor['name'])
    
    if jobs_to_check:
        logger.info(f"Found pending jobs for: {', '.join([c['name'] for c in jobs_to_check])}. Processing them now.")
    if no_jobs_found:
        logger.info(f"No pending jobs found for: {', '.join(no_jobs_found)}")
    
    for competitor in jobs_to_check:
        await check_and_load_results(competitor, app_config)

async def run_pipeline(args):
    """The primary orchestration function that executes the ETL workflow."""
    app_config, competitor_config = _load_configuration()
    if not app_config or not competitor_config:
        return
        
    batch_threshold = app_config.get('batch_threshold', 10)
    live_model = app_config.get('models', {}).get('live', 'gemini-2.0-flash')
    batch_model = app_config.get('models', {}).get('batch', 'gemini-2.5-flash')

    competitors_to_process = _get_competitors_to_process(competitor_config, args.competitor)
    if not competitors_to_process:
        return

    if args.check_job:
        await _run_job_check_phase(competitors_to_process, app_config)
            
    elif args.export:
        await _run_job_check_phase(competitors_to_process, app_config)
        run_export_process(competitors_to_process, args.export)

    elif args.enrich:
        logger.info("Discovering posts to enrich...")
        enrichment_plan = []
        for competitor in competitors_to_process:
            state_folder = os.path.join("state", competitor['name'])
            if not os.path.isdir(state_folder):
                continue

            csv_files = [f for f in os.listdir(state_folder) if f.endswith('.csv') and f.startswith(competitor['name'])]
            if not csv_files:
                continue

            latest_file = os.path.join(state_folder, max(csv_files))
            all_posts, to_enrich = [], []
            with open(latest_file, mode='r', newline='', encoding='utf-8') as f:
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