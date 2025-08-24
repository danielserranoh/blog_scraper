# main.py
# This file is the command-line entrypoint for the application.

import argparse
import logging
from termcolor import colored
import asyncio
from dotenv import load_dotenv
import warnings
# Import the main orchestrator function
from src.orchestrator import run_pipeline

warnings.filterwarnings("ignore", message=".* is not a valid JobState.*", category=UserWarning)

# --- Logger Setup ---
class ColorFormatter(logging.Formatter):
    COLORS = { 'INFO': 'blue', 'WARNING': 'yellow', 'ERROR': 'red' }
    def format(self, record):
        log_message = super().format(record)
        log_color = self.COLORS.get(record.levelname)
        
        # --- FIX: Convert the log level name to lowercase ---
        level_name = record.levelname.lower()
        
        log_level = colored(f"{level_name}:", color=log_color, attrs=['bold'])
        return f"{log_level} {log_message}"

def setup_logger():
    """Configures the root logger for the application."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter('%(message)s'))
    if root_logger.hasHandlers(): root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    # Silence the noisy google-genai library
    logging.getLogger("google.generativeai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

def main():
    """Parses command-line arguments and starts the ETL pipeline."""
    setup_logger()
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="Scrape blog posts from competitor websites.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # --- UPDATED: Redefine the days argument to be part of the --scrape flag ---
    parser.add_argument('--scrape', nargs='?', type=int, const=30, default=None, metavar='DAYS', help='Scrape posts from the last N days (default: 30).')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--scrape-all', action='store_true', help='Scrape all posts.')
    group.add_argument('--check-job', '-j', action='store_true', help='Check status of a batch job.')
    group.add_argument('--enrich', action='store_true', help='Enrich existing posts.')
    group.add_argument('--enrich-raw', action='store_true', help='Enrich posts from the raw data directory.')
    parser.add_argument('--competitor', '-c', type=str, help='Specify a single competitor.')
    parser.add_argument('--export', '-e',type=str, choices=['txt', 'json', 'md', 'gsheets', 'csv'], help='Export the latest data to a specified format (requires --competitor).')

    
    args = parser.parse_args()

    # Argument validation
    if args.check_job and (args.scrape is not None or args.scrape_all or args.enrich):
        logging.error("--check-job cannot be used with scraping-specific arguments.")
        return
    if args.enrich and (args.scrape is not None or args.scrape_all):
        logging.error("--enrich cannot be used with --days or --all.")
        return
    if args.scrape_all and args.scrape is not None:
        logging.error("--scrape-all cannot be used with --scrape.")
        return

    # Set the 'days' for the orchestrator to use
    if args.scrape is None and not args.scrape_all and not args.check_job and not args.enrich and not args.enrich_raw and not args.export:
        args.days = 30 # Default behavior if no flag is provided
        args.all = False
    elif args.scrape_all:
        args.days = None
        args.all = True
    else:
        args.days = args.scrape
        args.all = False

    # Start the asynchronous pipeline
    asyncio.run(run_pipeline(args))

if __name__ == "__main__":
    main()