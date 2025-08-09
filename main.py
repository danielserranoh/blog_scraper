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
import csv

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
#logging.logger.info = success

logger = logging.getLogger(__name__)

load_dotenv() 

from src.extract import extract_posts_in_batches
from src.transform import create_gemini_batch_job, check_gemini_batch_job, download_gemini_batch_results, transform_posts_live
from src.load import load_posts

async def run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold):
    """
    Scrapes the blog and decides whether to use live or batch processing.
    """
    name = competitor['name']
    
    all_posts = []
    async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all):
        all_posts.extend(batch)
    
    if not all_posts:
        logger.info(f"No new posts found for {name}. No API job will be submitted.")
        return

    if len(all_posts) < batch_threshold and not scrape_all:
        logger.info(f"Number of new posts ({len(all_posts)}) is below threshold. Using live processing.")
        transformed_posts = await transform_posts_live(all_posts)
        load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
    else:
        logger.info(f"Number of new posts ({len(all_posts)}) is above threshold. Submitting a batch job.")
        raw_posts_file_path = os.path.join("scraped", name, "temp_posts.jsonl")
        os.makedirs(os.path.dirname(raw_posts_file_path), exist_ok=True)
        with open(raw_posts_file_path, "w") as f:
            for post in all_posts:
                f.write(json.dumps(post) + "\n")
        logger.info(f"Saved {len(all_posts)} raw posts to '{raw_posts_file_path}'")
        
        job_id = create_gemini_batch_job(all_posts, name)

        if job_id:
            job_id_file_path = os.path.join("scraped", name, "batch_job_id.txt")
            with open(job_id_file_path, "w") as f:
                f.write(job_id)
            logger.info(f"Submitted Gemini batch job: {job_id}. Job ID saved to '{job_id_file_path}'")
            logger.info("The job will be processed in the background. You can check its status later with the --check-job flag.")
        else:
            logger.error(f"Failed to submit Gemini batch job for {name}. No results will be processed.")
            os.remove(raw_posts_file_path)

# In main.py

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

    # 1. Define parameters for our backoff strategy
    initial_delay = 30  # Start with a 30-second wait
    max_delay = 300     # Don't wait more than 5 minutes between checks
    multiplier = 2      # Double the delay each time
    
    delay = initial_delay
    
    while True:
        status = check_gemini_batch_job(job_id)

        # 2. Check for the terminal states (success or failure)
        if status not in ["JOB_STATE_PENDING", "JOB_STATE_RUNNING"]:
            break # Exit the loop if the job is done (succeeded, failed, etc.)

        # 3. If still running, wait using our backoff strategy
        jitter = random.uniform(0, 5) # Add a random wait of 0-5 seconds
        wait_time = min(delay, max_delay) + jitter
        
        logger.info(f"Job is {status}. Waiting for approximately {int(wait_time)} seconds before checking again.")
        await asyncio.sleep(wait_time)
        
        # Increase the delay for the next iteration
        delay *= multiplier
    
    # This check is now correct after our last fix
    if status == "JOB_STATE_SUCCEEDED":
        logger.info(f"Gemini batch job '{job_id}' succeeded!")
        
        if not os.path.exists(raw_posts_file_path):
            logger.error(f"Original raw posts file not found: '{raw_posts_file_path}'. Cannot combine results.")
            return

        with open(raw_posts_file_path, "r") as f:
            original_posts = [json.loads(line) for line in f]
        
        transformed_posts = download_gemini_batch_results(job_id, original_posts)

        if transformed_posts:
            load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
        else:
            logger.warning(f"No processed posts found for {name}. No files will be created.")

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
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--all',
        action='store_true',
        help='Scrape all available posts, regardless of publication date.'
    )
    group.add_argument(
        '--check-job',
        '-j',
        action='store_true',
        help='Check for an existing Gemini batch job ID and retrieve its results.'
    )
    group.add_argument(
        '--enrich',
        '-e',
        action='store_true',
        help='Enrich existing posts in the scraped CSV file that are missing Gemini API data.'
    )

    parser.add_argument(
        '--competitor',
        '-c',
        type=str,
        help='Specify a single competitor to scrape (e.g., "terminalfour", "modern campus", "squiz").\nIf omitted, all competitors will be scraped.'
    )
    
    args = parser.parse_args()
    
    if args.check_job and (args.days != 30 or args.enrich):
        logger.error("--check-job cannot be used with other scraping arguments (--days, --competitor, --enrich). Please use it alone.")
        return
    
    if args.enrich and (args.days != 30):
        logger.error("--enrich cannot be used with --days or --all. Please specify a competitor with -c.")
        return

    days_to_scrape = args.days
    scrape_all = args.all
    selected_competitor = args.competitor
    enrich_posts = args.enrich
    batch_threshold = 10

    try:
        # Load the main application config
        with open('config/config.json', 'r') as f:
            app_config = json.load(f)
        
        # Load the competitor data
        with open('config/competitor_seed_data.json', 'r') as f:
            competitor_config = json.load(f)

    except FileNotFoundError as e:
        logger.error(f"Error: Configuration file not found. Please ensure both config.json and competitor_seed_data.json exist in the 'config/' directory. Details: {e}")
        return
    except json.JSONDecodeError as e:
        logger.error(f"Error: Could not parse a configuration file. Please check for syntax errors (e.g., trailing commas). Details: {e}")
        return

    # Get settings from the app_config
    batch_threshold = app_config.get('batch_threshold', 10)
    live_model = app_config.get('models', {}).get('live', 'gemini-2.5-flash')
    batch_model = app_config.get('models', {}).get('batch', 'gemini-2.5-flash-lite')

    # Get competitors from the competitor_config
    all_competitors = competitor_config.get('competitors', [])
    if not all_competitors:
        logger.warning("No competitors found in 'config/competitor_seed_data.json'.")
        return

    competitors_to_process = []
    if selected_competitor:
        found = False
        for comp in all_competitors:
            if comp['name'].lower() == selected_competitor.lower():
                competitors_to_process.append(comp)
                found = True
                break
        if not found:
            logger.error(f"Error: Competitor '{selected_competitor}' not found in 'config/competitor_seed_data.json'.")
            return
    else:
        competitors_to_process = all_competitors

    for competitor in competitors_to_process:
        if args.check_job:
            await check_and_load_results(competitor)
        elif args.enrich:
            logger.info(f"--- Starting enrichment process for {competitor['name']} ---")
            posts_to_enrich = []
            
            output_folder = os.path.join("scraped", competitor['name'])
            if not os.path.isdir(output_folder):
                logger.warning(f"No scraped data found for {competitor['name']}. Cannot enrich.")
                continue

            files = os.listdir(output_folder)
            csv_files = [f for f in files if f.endswith('.csv') and f.startswith(competitor['name'])]
            if not csv_files:
                logger.warning(f"No CSV file found for {competitor['name']}. Cannot enrich.")
                continue

            latest_file = os.path.join(output_folder, max(csv_files))
            all_posts_from_file = []
            with open(latest_file, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for post in reader:
                    all_posts_from_file.append(post)
                    if post.get('summary') == 'N/A' or post.get('seo_keywords') == 'N/A':
                        posts_to_enrich.append(post)
            
            if posts_to_enrich:
                if len(posts_to_enrich) < batch_threshold:
                    logger.info(f"Found {len(posts_to_enrich)} posts to enrich. Using live processing.")
                    # --- FIX: Pass the 'live_model' argument ---
                    enriched_posts = await transform_posts_live(posts_to_enrich, live_model)
                else:
                    logger.info(f"Found {len(posts_to_enrich)} posts to enrich. Submitting a batch job.")
                    # --- FIX: Pass the 'batch_model' argument ---
                    job_id = create_gemini_batch_job(posts_to_enrich, competitor['name'], batch_model)

                    if job_id:
                        logger.info(f"Submitted Gemini batch job: {job_id}. Use --check-job to retrieve results later.")
                        continue
                    else:
                        logger.error(f"Failed to submit Gemini batch job for {competitor['name']}. No results will be processed.")
                        continue
                
                if enriched_posts:
                    enriched_map = {post['url']: post for post in enriched_posts}
                    final_posts = []
                    for post in all_posts_from_file:
                        final_posts.append(enriched_map.get(post['url'], post))
                        
                    load_posts(final_posts, filename_prefix=f"{competitor['name']}_blog_posts")
                    logger.info(f"Successfully enriched {len(enriched_posts)} posts.")
                else:
                    logger.warning(f"Enrichment process failed for {competitor['name']}.")
            else:
                logger.info(f"No posts found for {competitor['name']} that need enrichment.")
                
        else:
            await run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold)
            
    logger.info("\n--- ETL process completed ---")

if __name__ == "__main__":
    asyncio.run(main())
