# main.py
# This file serves as the main orchestrator for the ETL pipeline.

from dotenv import load_dotenv
import os
import json
import argparse
import logging
from termcolor import colored
import asyncio # Import asyncio for asynchronous operations

# Configure a custom logger with colorized output
class ColorFormatter(logging.Formatter):
    COLORS = {
        'INFO': 'blue',
        'SUCCESS': 'green', # Custom level name
        'WARNING': 'yellow',
        'ERROR': 'red'
    }

    def format(self, record):
        log_message = super().format(record)
        return colored(log_message, self.COLORS.get(record.levelname))

# Setup logging for the entire application
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

# Load environment variables from .env file
load_dotenv() 

# Import functions from the new src package structure
from src.extract import extract_posts_in_batches
from src.transform import transform_posts
from src.load import load_posts

async def main(): # main is now an async function
    """
    Main asynchronous function to run the ETL pipeline for all competitors.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Scrape blog posts from competitor websites.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Optional positional argument for days, defaults to 30
    parser.add_argument(
        'days',
        nargs='?', # Makes the argument optional
        type=int,
        default=30,
        help='The number of days to look back for recent posts. Defaults to 30.\nExample: python main.py 7'
    )
    
    # Optional flag to scrape all posts
    parser.add_argument(
        '--all',
        action='store_true',
        help='Scrape all available posts, regardless of publication date.\nExample: python main.py --all'
    )

    # New optional argument to select a specific competitor
    parser.add_argument(
        '--competitor',
        '-c', # Short alias for competitor
        type=str,
        help='Specify a single competitor to scrape (e.g., "terminalfour", "modern campus", "squiz").\nIf omitted, all competitors will be scraped.'
    )
    
    args = parser.parse_args()
    days_to_scrape = args.days
    scrape_all = args.all
    selected_competitor = args.competitor # New argument

    # Load competitor data from the JSON file
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
        name = competitor['name']
        
        logger.info(f"\n--- Starting ETL process for {name} ---")
        
        post_batches_generator = extract_posts_in_batches(
            competitor, 
            days_to_scrape, 
            scrape_all
        )
        
        async for batch in post_batches_generator:
            if not batch:
                continue

            transformed_posts = await transform_posts(batch)

            if transformed_posts:
                load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
            else:
                logger.info(f"No recent posts found in this batch for {name}. No files will be created.")
            
    # Dynamic success message based on selection
    if selected_competitor:
        logger.success(f"\n--- ETL process completed for competitor: {selected_competitor} ---")
    else:
        logger.success("\n--- ETL process completed for all configured competitors ---")

if __name__ == "__main__":
    # Run the main asynchronous function
    asyncio.run(main())
