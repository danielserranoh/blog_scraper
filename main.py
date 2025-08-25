# main.py
# This file is the command-line entrypoint for the application.

import logging
from termcolor import colored
import asyncio
from dotenv import load_dotenv
import warnings
import click
from types import SimpleNamespace # <--- ADD THIS

# Import the main orchestrator function
from src.orchestrator import run_pipeline

warnings.filterwarnings("ignore", message=".* is not a valid JobState.*", category=UserWarning)

# --- Logger Setup ---
class ColorFormatter(logging.Formatter):
    COLORS = { 'INFO': 'blue', 'WARNING': 'yellow', 'ERROR': 'red' }
    def format(self, record):
        log_message = super().format(record)
        log_color = self.COLORS.get(record.levelname)
        
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
    logging.getLogger("google.generativeai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

@click.group()
def cli():
    """An advanced ETL pipeline to scrape and enrich blog posts."""
    setup_logger()
    load_dotenv()

@cli.command()
@click.option('--days', '-d', type=int, default=30, help='Scrape posts from the last N days (default: 30).')
@click.option('--all', '-a', is_flag=True, help='Scrape all available posts, overriding --days.')
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to scrape.')
@click.option('--wait', is_flag=True, help='Waits for batch jobs to complete before exiting.')
def scrape(days, all, competitor, wait):
    """Scrape blog posts."""
    if all:
        days = None
    
    # <--- UPDATED: Create a SimpleNamespace object that mimics argparse's behavior. --->
    args = SimpleNamespace(
        days=days,
        all=all,
        competitor=competitor,
        wait=wait,
        scrape=True,
        enrich=False,
        enrich_raw=False,
        check_job=False,
        export=None
    )
    
    asyncio.run(run_pipeline(args))

@cli.command()
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to enrich.')
@click.option('--wait', is_flag=True, help='Waits for batch jobs to complete before exiting.')
@click.option('--raw', is_flag=True, help='Enrich posts from the raw data directory.')
def enrich(competitor, wait, raw):
    """Enrich existing posts or raw data."""
    # <--- UPDATED: Create a SimpleNamespace object. --->
    args = SimpleNamespace(
        competitor=competitor,
        wait=wait,
        enrich=not raw,
        enrich_raw=raw,
        days=None,
        all=False,
        scrape=False,
        check_job=False,
        export=None
    )
    
    asyncio.run(run_pipeline(args))

@cli.command()
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to check.')
def check_job(competitor):
    """Check the status of pending batch jobs."""
    # <--- UPDATED: Create a SimpleNamespace object. --->
    args = SimpleNamespace(
        competitor=competitor,
        check_job=True,
        wait=False,
        days=None,
        all=False,
        scrape=False,
        enrich=False,
        enrich_raw=False,
        export=None
    )
    
    asyncio.run(run_pipeline(args))

@cli.command()
@click.option('--format', '-f', 'export_format', type=click.Choice(['txt', 'json', 'md', 'gsheets', 'csv']), required=True, help='Export the data to a specified format.')
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to export.')
def export(export_format, competitor):
    """Export the latest data to a file."""
    # <--- UPDATED: Create a SimpleNamespace object. --->
    args = SimpleNamespace(
        export=export_format,
        competitor=competitor,
        check_job=True, # Export must check jobs first
        wait=False,
        days=None,
        all=False,
        scrape=False,
        enrich=False,
        enrich_raw=False
    )
    
    asyncio.run(run_pipeline(args))

if __name__ == "__main__":
    cli()