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
    parser.add_argument('days', nargs='?', type=int, default=30, help='Days to look back for posts.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help='Scrape all posts.')
    group.add_argument('--check-job', '-j', action='store_true', help='Check status of a batch job.')
    group.add_argument('--enrich', action='store_true', help='Enrich existing posts.')
    parser.add_argument('--competitor', '-c', type=str, help='Specify a single competitor.')
    parser.add_argument('--export', '-e',type=str, choices=['txt', 'json', 'md', 'gsheets', 'csv'], help='Export the latest data to a specified format (requires --competitor).')

    
    args = parser.parse_args()

    # Argument validation
    if args.check_job and (args.days != 30 or args.all or args.enrich):
        logging.error("--check-job cannot be used with scraping-specific arguments.")
        return
    if args.enrich and (args.days != 30 or args.all):
        logging.error("--enrich cannot be used with --days or --all.")
        return

        
    # Start the asynchronous pipeline
    asyncio.run(run_pipeline(args))

if __name__ == "__main__":
    main()