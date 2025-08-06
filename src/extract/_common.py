# main.py
# This file serves as the main orchestrator for the ETL pipeline.

from dotenv import load_dotenv
import os
import json
import argparse
import logging
from termcolor import colored
import asyncio
import httpx

# Configure a custom logger with colorized output
class ColorFormatter(logging.Formatter):
    COLORS = {
        'INFO': 'blue',
        'SUCCESS': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red'
    }
    def format(self, record):
        log_message = super().format(record)
        return colored(log_message, self.COLORS.get(record.levelname))

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter('%(levelname)s: %(message)s'))

if root_logger.hasHandlers():
    root_logger.handlers.clear()
root_logger.addHandler(console_handler)

def success(self, message, *args, **kwargs):
    if self.isEnabledFor(25):
        self._log(25, message, args, **kwargs)
logging.Logger.success = success

logger = logging.getLogger(__name__)

load_dotenv() 

from src.extract import extract_posts_in_batches
from src.transform import create_gemini_batch_job, check_gemini_batch_job, download_gemini_batch_results
from src.load import load_posts

async def run_scrape_and_submit(competitor, days_to_scrape, scrape_all):
    """
    Scrapes the blog, saves the raw data, and submits a new batch job.
    """
    name = competitor['name']
    
    # 1. EXTRACT: Scrape the blog and collect all posts
    all_posts = []
    async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all):
        all_posts.extend(batch)
    
    if not all_posts:
        logger.info(f"No new posts found for {name}. No API job will be submitted.")
        return

    # Save raw posts to a temporary file for state management
    raw_posts_file_path = os.path.join("scraped", name, "temp_posts.jsonl")
    os.makedirs(os.path.dirname(raw_posts_file_path), exist_ok=True)
    with open(raw_posts_file_path, "w") as f:
        for post in all_posts:
            f.write(json.dumps(post) + "\n")
    logger.info(f"Saved {len(all_posts)} raw posts to '{raw_posts_file_path}'")

    # 2. TRANSFORM: Create and submit the Gemini batch job
    job_id = await create_gemini_batch_job(all_posts, name)

    if job_id:
        # Save job ID to a file for later retrieval
        job_id_file_path = os.path.join("scraped", name, "batch_job_id.txt")
        with open(job_id_file_path, "w") as f:
            f.write(job_id)
        logger.info(f"Submitted Gemini batch job: {job_id}. Job ID saved to '{job_id_file_path}'")
        logger.info("The job will be processed in the background. You can check its status later with the --check-job flag.")
    else:
        logger.error(f"Failed to submit Gemini batch job for {name}. No results will be processed.")
        # Clean up temp file on failure
        os.remove(raw_posts_file_path)

async def check_and_load_results(competitor):
    """
    Checks for a saved job ID, polls the Gemini API for results, and loads them.
    """
    name = competitor['name']
    job_id_file_path = os.path.join("scraped", name, "batch_job_id.txt")
    raw_posts_file_path = os.path.join("scraped", name, "temp_posts.jsonl")

    if not os.path.exists(job_id_file_path):
        logger.warning(f"No saved Gemini batch job ID found for {name}. Nothing to check.")
        return

    with open(job_id_file_path, "r") as f:
        job_id = f.read().strip()
    
    logger.info(f"Checking status of Gemini batch job: {job_id}")

    # Poll the API until the job is complete or it fails
    status = await check_gemini_batch_job(job_id)
    while status in ["PENDING", "RUNNING"]:
        logger.info(f"Job is {status}. Waiting 60 seconds before checking again.")
        await asyncio.sleep(60)
        status = await check_gemini_batch_job(job_id)
    
    if status == "SUCCEEDED":
        logger.success(f"Gemini batch job '{job_id}' succeeded!")
        
        # Load the original raw posts
        if not os.path.exists(raw_posts_file_path):
            logger.error(f"Original raw posts file not found: '{raw_posts_file_path}'. Cannot combine results.")
            return

        with open(raw_posts_file_path, "r") as f:
            original_posts = [json.loads(line) for line in f]
        
        # Download and process results
        transformed_posts = await download_gemini_batch_results(job_id, original_posts)

        if transformed_posts:
            load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
        else:
            logger.warning(f"No processed posts found for {name}. No files will be created.")

        # Clean up temporary files
        os.remove(job_id_file_path)
        os.remove(raw_posts_file_path)
        logger.info(f"Cleaned up temporary files for job '{job_id}'")
        
    else:
        logger.error(f"Gemini batch job '{job_id}' failed or has an unknown status: {status}")


async def main():
    parser = argparse.ArgumentParser(
        description="Scrape blog posts from competitor websites. Use --check-job to resume a previously submitted job.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        'days',
        nargs='?',
        type=int,
        default=30,
        help='The number of days to look back for recent posts. Defaults to 30.'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Scrape all available posts, regardless of publication date.'
    )
    parser.add_argument(
        '--competitor',
        '-c',
        type=str,
        help='Specify a single competitor to scrape (e.g., "terminalfour", "modern campus", "squiz").'
    )
    parser.add_argument(
        '--check-job',
        '-j',
        action='store_true',
        help='Check for an existing Gemini batch job ID and retrieve its results.'
    )
    
    args = parser.parse_args()
    
    if args.check_job and (args.days != 30 or args.all or args.competitor):
        logger.error("--check-job cannot be used with other scraping arguments (--days, --all, --competitor). Please use it alone.")
        return

    days_to_scrape = args.days
    scrape_all = args.all
    selected_competitor = args.competitor

    try:
        with open('config/competitor_seed_data.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("Error: 'config/competitor_seed_data.json' not found. Please ensure the file exists.")
        return

    competitors_to_process = []
    if selected_competitor:
        found = False
        for comp in config['competitors']:
            if comp['name'].lower() == selected_competitor.lower():
                competitors_to_process.append(comp)
                found = True
                break
        if not found:
            logger.error(f"Error: Competitor '{selected_competitor}' not found in 'config/competitor_seed_data.json'.")
            return
    else:
        competitors_to_process = config['competitors']

    for competitor in competitors_to_process:
        if args.check_job:
            await check_and_load_results(competitor)
        else:
            await run_scrape_and_submit(competitor, days_to_scrape, scrape_all)
            
    logger.success("\n--- ETL process completed ---")

if __name__ == "__main__":
    asyncio.run(main())
