# main.py
# This file serves as the main orchestrator for the ETL pipeline.

from dotenv import load_dotenv
import os
import json
import argparse
import logging
from termcolor import colored
import asyncio
import random
import httpx
import csv

# --- Logger Setup ---
class ColorFormatter(logging.Formatter):
    # Removed 'SUCCESS' from the color map
    COLORS = {
        'INFO': 'blue',
        'WARNING': 'yellow',
        'ERROR': 'red'
    }
    def format(self, record):
        log_message = super().format(record)
        log_color = self.COLORS.get(record.levelname)
        # Bolding the log level for better visibility
        log_level = colored(f"{record.levelname}:", color=log_color, attrs=['bold'])
        return f"{log_level} {log_message}"

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter('%(message)s'))

if root_logger.hasHandlers():
    root_logger.handlers.clear()
root_logger.addHandler(console_handler)


logger = logging.getLogger(__name__)
load_dotenv() 

from src.extract import extract_posts_in_batches
from src.transform import create_gemini_batch_job, check_gemini_batch_job, download_gemini_batch_results, transform_posts_live
from src.load import load_posts

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
    

def _save_job_id(competitor_name, job_id):
    """Saves a batch job ID to a text file for later retrieval."""
    try:
        job_id_file_path = os.path.join("scraped", competitor_name, "batch_job_id.txt")
        os.makedirs(os.path.dirname(job_id_file_path), exist_ok=True)
        with open(job_id_file_path, "w") as f:
            f.write(job_id)
        logger.info(f"Submitted Gemini batch job: {job_id}. Job ID saved to '{job_id_file_path}'")
        logger.info("Use --check-job to retrieve results later.")
    except IOError as e:
        logger.error(f"Could not save job ID {job_id} to file: {e}")


def _save_raw_posts(posts, competitor_name):
    """Saves a list of posts to a temporary JSONL file for later processing."""
    try:
        raw_posts_file_path = os.path.join("scraped", competitor_name, "temp_posts.jsonl")
        os.makedirs(os.path.dirname(raw_posts_file_path), exist_ok=True)
        with open(raw_posts_file_path, "w") as f:
            for post in posts:
                f.write(json.dumps(post) + "\n")
        logger.info(f"Saved {len(posts)} raw posts to '{raw_posts_file_path}' for later processing.")
        return raw_posts_file_path
    except IOError as e:
        logger.error(f"Could not save raw posts to file: {e}")
        return None

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

async def run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold, live_model, batch_model):
    """Scrapes the blog and decides whether to use live or batch processing."""
    name = competitor['name']
    
    all_posts = []
    async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all):
        all_posts.extend(batch)
    
    if not all_posts:
        logger.info(f"No new posts found for {name}. No API job will be submitted.")
        return

    if len(all_posts) < batch_threshold and not scrape_all:
        logger.info(f"Number of new posts ({len(all_posts)}) is below threshold. Using live processing.")
        transformed_posts = await transform_posts_live(all_posts, live_model)
        load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
    else:
        logger.info(f"Number of new posts ({len(all_posts)}) is above threshold. Submitting a batch job.")
        raw_posts_file_path = _save_raw_posts(all_posts, name)
        
        job_id = create_gemini_batch_job(all_posts, name, batch_model)

        if job_id:
            _save_job_id(name, job_id)
        else:
            logger.error(f"Failed to submit Gemini batch job for {name}. No results will be processed.")
            os.remove(raw_posts_file_path)

async def check_and_load_results(competitor):
    """Checks for a saved job ID, polls the Gemini API for results, and loads them."""
    name = competitor['name']
    job_id_file_path = os.path.join("scraped", name, "batch_job_id.txt")
    raw_posts_file_path = os.path.join("scraped", name, "temp_posts.jsonl")

    if not os.path.exists(job_id_file_path):
        logger.warning(f"No saved Gemini batch job ID found for {name}. Nothing to check.")
        return

    with open(job_id_file_path, "r") as f:
        job_id = f.read().strip()
    
    logger.info(f"Checking status of Gemini batch job: {job_id}")

    initial_delay, max_delay, multiplier = 30, 300, 2
    delay = initial_delay
    
    while True:
        status = check_gemini_batch_job(job_id)
        if status not in ["JOB_STATE_PENDING", "JOB_STATE_RUNNING"]:
            break
        jitter = random.uniform(0, 5)
        wait_time = min(delay, max_delay) + jitter
        logger.info(f"Job is {status}. Waiting for approximately {int(wait_time)} seconds...")
        await asyncio.sleep(wait_time)
        delay *= multiplier
    
    if status == "JOB_STATE_SUCCEEDED":
        logger.info(f"Gemini batch job '{job_id}' succeeded!")
        
        original_posts = None # Default to None
        if not os.path.exists(raw_posts_file_path):
            # Change the error to a warning
            logger.warning(f"Original raw posts file not found: '{raw_posts_file_path}'. Will attempt to recover results from download.")
        else:
            with open(raw_posts_file_path, "r") as f:
                original_posts = [json.loads(line) for line in f]
        
        # Call the download function. It will handle the case where original_posts is None.
        transformed_posts = download_gemini_batch_results(job_id, original_posts)

        if transformed_posts:
            # Save the recovered data
            load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
        else:
            logger.warning(f"No processed posts could be recovered for {name}. No files will be created.")

        # Clean up the job ID file. If the raw posts file exists, clean it up too.
        os.remove(job_id_file_path)
        if os.path.exists(raw_posts_file_path):
            os.remove(raw_posts_file_path)
        logger.info(f"Cleaned up temporary files for job '{job_id}'")
        
    else:
        logger.error(f"Gemini batch job '{job_id}' failed or has an unknown status: {status}")

async def run_enrichment_process(competitor, batch_threshold, live_model, batch_model):
    """Finds and enriches posts with 'N/A' values for a given competitor."""
    logger.info(f"--- Starting enrichment process for {competitor['name']} ---")
    
    output_folder = os.path.join("scraped", competitor['name'])
    if not os.path.isdir(output_folder):
        logger.warning(f"No scraped data found for {competitor['name']}. Cannot enrich.")
        return

    files = os.listdir(output_folder)
    csv_files = [f for f in files if f.endswith('.csv') and f.startswith(competitor['name'])]
    if not csv_files:
        logger.warning(f"No CSV file found for {competitor['name']}. Cannot enrich.")
        return

    latest_file = os.path.join(output_folder, max(csv_files))
    all_posts_from_file, posts_to_enrich = [], []
    with open(latest_file, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for post in reader:
            all_posts_from_file.append(post)
            if post.get('summary') == 'N/A' or post.get('seo_keywords') == 'N/A':
                posts_to_enrich.append(post)
    
    if not posts_to_enrich:
        logger.info(f"No posts found for {competitor['name']} that need enrichment.")
        return
        
    if len(posts_to_enrich) < batch_threshold:
        logger.info(f"Found {len(posts_to_enrich)} posts to enrich. Using live processing.")
        enriched_posts = await transform_posts_live(posts_to_enrich, live_model)
        
        if enriched_posts:
            enriched_map = {post['url']: post for post in enriched_posts}
            final_posts = [enriched_map.get(post['url'], post) for post in all_posts_from_file]
            load_posts(final_posts, filename_prefix=f"{competitor['name']}_blog_posts")
            # Changed logger.success to logger.info
            logger.info(f"Successfully enriched {len(enriched_posts)} posts.")
        else:
            logger.warning(f"Enrichment process failed for {competitor['name']}.")

    else:
        logger.info(f"Found {len(posts_to_enrich)} posts to enrich. Submitting a batch job.")
        raw_posts_file_path = _save_raw_posts(posts_to_enrich, competitor['name'])
        if not raw_posts_file_path:
            return
        job_id = create_gemini_batch_job(posts_to_enrich, competitor['name'], batch_model)
        if job_id:
            _save_job_id(competitor['name'], job_id)
        else:
            logger.error(f"Failed to submit Gemini batch job for {competitor['name']}.")

async def main():
    parser = argparse.ArgumentParser(
        description="Scrape blog posts from competitor websites.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # Argument parser setup
    parser.add_argument('days', nargs='?', type=int, default=30, help='Days to look back for posts. Defaults to 30.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help='Scrape all posts, regardless of date.')
    group.add_argument('--check-job', '-j', action='store_true', help='Check status of a batch job.')
    group.add_argument('--enrich', '-e', action='store_true', help='Enrich existing posts missing Gemini data.')
    parser.add_argument('--competitor', '-c', type=str, help='Specify a single competitor to process.')
    
    args = parser.parse_args()

    # Argument validation
    if args.check_job and (args.days != 30 or args.all or args.enrich):
        logger.error("--check-job cannot be used with scraping-specific arguments.")
        return
    if args.enrich and (args.days != 30 or args.all):
        logger.error("--enrich cannot be used with --days or --all.")
        return

    # Configuration loading
    app_config, competitor_config = _load_configuration()
    if not app_config or not competitor_config:
        return
        
    batch_threshold = app_config.get('batch_threshold', 10)
    live_model = app_config.get('models', {}).get('live', 'gemini-2.0-flash')
    batch_model = app_config.get('models', {}).get('batch', 'gemini-2.5-flash')

    # Competitor selection
    competitors_to_process = _get_competitors_to_process(competitor_config, args.competitor)
    if not competitors_to_process:
        return

    # Main processing loop
    for competitor in competitors_to_process:
        if args.check_job:
            await check_and_load_results(competitor)
        elif args.enrich:
            await run_enrichment_process(competitor, batch_threshold, live_model, batch_model)
        else:
            await run_scrape_and_submit(competitor, args.days, args.all, batch_threshold, live_model, batch_model)
            
    # Changed logger.success to logger.info
    logger.info("\n--- ETL process completed ---")

if __name__ == "__main__":
    asyncio.run(main())