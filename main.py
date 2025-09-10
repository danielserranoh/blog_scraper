# main.py
# This file is the command-line entrypoint for the application.

import logging
from termcolor import colored
import asyncio
from dotenv import load_dotenv
import warnings
import click

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

async def handle_pipeline_result(result):
    """Handle the result from run_pipeline and provide user feedback."""
    if result is None:
        # Success case - pipeline completed normally
        return
    
    if isinstance(result, dict) and result.get('error'):
        # Error case - pipeline returned an error
        error_code = result.get('error_code', 'UNKNOWN')
        message = result.get('message', 'Unknown error occurred')
        details = result.get('details', {})
        
        print(colored(f"Error [{error_code}]: {message}", 'red'))
        if details:
            print(colored("Details:", 'yellow'))
            for key, value in details.items():
                print(colored(f"  {key}: {value}", 'yellow'))
    else:
        # Success case with return data
        if result.get('success'):
            operation = result.get('operation', 'unknown')
            print(colored(f"✓ {operation} completed successfully", 'green'))
            
            # Show operation-specific metrics
            if 'posts_scraped' in result:
                print(colored(f"  Posts scraped: {result['posts_scraped']}", 'cyan'))
            elif 'posts_enriched' in result:
                print(colored(f"  Posts enriched: {result['posts_enriched']}", 'cyan'))
            elif 'posts_processed' in result:
                print(colored(f"  Posts processed: {result['posts_processed']}", 'cyan'))
            elif 'results_count' in result:
                print(colored(f"  Results loaded: {result['results_count']}", 'cyan'))
            
            # Show enrichment failure warnings and recommendations
            if result.get('enrichment_failures', 0) > 0:
                failures = result['enrichment_failures']
                print(colored(f"⚠️  {failures} enrichment(s) failed due to API issues", 'yellow'))
                if result.get('recommendation'):
                    print(colored(f"💡 {result['recommendation']}", 'blue'))

@click.group()
def cli():
    """An advanced ETL pipeline to scrape and enrich blog posts."""
    setup_logger()
    load_dotenv()

@cli.command()
@click.option('--days', '-d', type=int, default=30, help='Scrape posts from the last N days (default: 30).')
@click.option('--all', '-a', is_flag=True, help='Scrape all available posts, overriding --days.')
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to process.')
@click.option('--wait', is_flag=True, help='Waits for batch jobs to complete before exiting.')
def get_posts(days, all, competitor, wait):
    """Scrape posts, enrich, and save the final output."""
    args = {
        'days': days if not all else None,
        'all': all,
        'competitor': competitor,
        'wait': wait,
        'get_posts': True,
        'scrape': False,
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'export': None
    }

    async def run():
        result = await run_pipeline(args)
        await handle_pipeline_result(result)
    
    asyncio.run(run())

@cli.command()
@click.option('--days', '-d', type=int, default=30, help='Scrape posts from the last N days (default: 30).')
@click.option('--all', '-a', is_flag=True, help='Scrape all available posts, overriding --days.')
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to scrape.')
def scrape(days, all, competitor):
    """Scrape posts and save raw data."""
    args = {
        'days': days if not all else None,
        'all': all,
        'competitor': competitor,
        'scrape': True,
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'wait': False,
        'get_posts': False
    }
    
    async def run():
        result = await run_pipeline(args)
        await handle_pipeline_result(result)
    
    asyncio.run(run())

@cli.command()
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to enrich.')
@click.option('--wait', is_flag=True, help='Waits for batch jobs to complete before exiting.')
@click.option('--raw', is_flag=True, help='Enrich posts from the raw data directory.')
def enrich(competitor, wait, raw):
    """Enrich existing posts or raw data."""
    args = {
        'competitor': competitor,
        'wait': wait,
        'enrich': not raw,
        'enrich_raw': raw,
        'days': None,
        'all': False,
        'scrape': False,
        'check_job': False,
        'export': None,
        'get_posts': False
    }
    
    async def run():
        result = await run_pipeline(args)
        await handle_pipeline_result(result)
    
    asyncio.run(run())

@cli.command()
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to check.')
def check_job(competitor):
    """Check the status of pending batch jobs."""
    args = {
        'competitor': competitor,
        'check_job': True,
        'wait': False,
        'days': None,
        'all': False,
        'scrape': False,
        'enrich': False,
        'enrich_raw': False,
        'export': None,
        'get_posts': False
    }
    
    async def run():
        result = await run_pipeline(args)
        await handle_pipeline_result(result)
    
    asyncio.run(run())

@cli.command()
@click.option('--format', '-f', 'export_format', type=click.Choice(['txt', 'json', 'md', 'strategy-brief', 'content-gaps', 'gsheets', 'csv']), required=True, help='Export the data to a specified format.')
@click.option('--competitor', '-c', type=str, help='Specify a single competitor to export.')
def export(export_format, competitor):
    """Export the latest data to a file."""
    args = {
        'export': True,
        'export_format': export_format,
        'competitor': competitor,
        'check_job': False,
        'wait': False,
        'days': None,
        'all': False,
        'scrape': False,
        'enrich': False,
        'enrich_raw': False,
        'get_posts': False
    }
    
    async def run():
        result = await run_pipeline(args)
        await handle_pipeline_result(result)
    
    asyncio.run(run())

@cli.command()
@click.option('--gaps', is_flag=True, help='Analyze content gaps and opportunities across competitors.')
@click.option('--strategy', is_flag=True, help='Generate strategic intelligence brief.')
@click.option('--competitor', '-c', type=str, help='Focus analysis on a specific competitor.')
def analyze(gaps, strategy, competitor):
    """Analyze competitive content for strategic insights."""
    if not gaps and not strategy:
        click.echo("Error: Must specify either --gaps or --strategy analysis type.")
        return
    
    if gaps and strategy:
        click.echo("Error: Please specify only one analysis type at a time.")
        return
    
    analysis_type = 'content_gaps' if gaps else 'strategy_brief'
    
    args = {
        'analyze': True,
        'analysis_type': analysis_type,
        'competitor': competitor,
        'check_job': False,
        'wait': False,
        'days': None,
        'all': False,
        'scrape': False,
        'enrich': False,
        'enrich_raw': False,
        'export': None,
        'get_posts': False
    }
    
    async def run():
        result = await run_pipeline(args)
        await handle_pipeline_result(result)
    
    asyncio.run(run())

if __name__ == "__main__":
    cli()