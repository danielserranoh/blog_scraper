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
        # Apply color based on the log level
        return colored(log_message, self.COLORS.get(record.levelname))

# Setup logging for the entire application
# Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO) # Set the default level for all loggers

# Create a console handler and set its formatter
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter('%(levelname)s: %(message)s'))

# Clear any existing handlers to prevent duplicate output
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Add the console handler to the root logger
root_logger.addHandler(console_handler)

# Dynamically add the 'success' method to the Logger class
# This allows us to call logger.success() directly from any logger instance
def success(self, message, *args, **kwargs):
    if self.isEnabledFor(25): # Check if SUCCESS level is enabled
        self._log(25, message, args, **kwargs)

logging.Logger.success = success

# Get a logger instance for this module (main.py)
logger = logging.getLogger(__name__)


# Import functions from other files in the same directory
from extract import extract_posts_in_batches
from transform import transform_posts
from load import load_posts

# This is where load_dotenv() is called to load environment variables from .env file
load_dotenv() 

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
        with open('competitor_seed_data.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("Error: 'competitor_seed_data.json' not found. Please ensure the file exists.")
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
            logger.error(f"Error: Competitor '{selected_competitor}' not found in 'competitor_seed_data.json'.")
            return
    else:
        competitors_to_process = config['competitors']


    for competitor in competitors_to_process:
        name = competitor['name']
        
        logger.info(f"\n--- Starting ETL process for {name} ---")
        
        # 1. EXTRACT: Scrape the blog and get recent posts in batches
        # extract_posts_in_batches is now an async generator
        post_batches_generator = extract_posts_in_batches(
            competitor, 
            days_to_scrape, 
            scrape_all
        )
        
        # Iterate over the async generator
        async for batch in post_batches_generator:
            if not batch:
                continue

            # 2. TRANSFORM: Sort and enrich the data for the current batch
            # transform_posts is now an async function
            transformed_posts = await transform_posts(batch)

            # 3. LOAD: Save the data for the current batch
            if transformed_posts:
                load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
            else:
                logger.info(f"No recent posts found in this batch for {name}. No files will be created.")
            
    logger.success("\n--- ETL process completed for all competitors ---")

if __name__ == "__main__":
    # Run the main asynchronous function
    asyncio.run(main())
