# src/extract/__init__.py
# This file serves as the router for the extraction phase.

import logging
import importlib
from ._common import ScrapeStats
from .scraper_manager import ScraperManager

logger = logging.getLogger(__name__)

# A mapping from the structure pattern to the module that handles it.
STRUCTURE_MAP = {
    "multi_category": ".blog_patterns.multi_category",
    "single_list": ".blog_patterns.single_list",
    "single_page": ".blog_patterns.single_page",
}

async def extract_posts_in_batches(config, days=30, scrape_all=False, batch_size=10):
    """
    A router that dynamically dispatches to the correct structure scraper
    based on the configured pattern.
    """
    competitor_name = config['name']
    pattern = config.get('structure_pattern')
    
    stats = ScrapeStats()
    logger.info(f"--- Starting scrape for '{competitor_name}' ---")

    if not pattern or pattern not in STRUCTURE_MAP:
        logger.warning(f"No valid structure_pattern found for '{competitor_name}'. Skipping.")
        return

    try:
        module_path = STRUCTURE_MAP[pattern]
        scraper_module = importlib.import_module(module_path, package='src.extract')
        
        # Each structure scraper will handle its own logic, including pagination
        async for batch in scraper_module.scrape(config, days, scrape_all, batch_size, stats):
            yield batch

    except ImportError:
        logger.error(f"Could not import the scraper module for pattern '{pattern}'.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while scraping '{competitor_name}': {e}")
    
    finally:
        print() 
        logger.info(f"--- Scrape Summary for '{competitor_name}' ---")
        logger.info(f"  New Posts Found: {stats.successful}")
        logger.info(f"  Skipped (already exist): {stats.skipped}")
        if stats.errors > 0:
            logger.warning(f"  Errors (failed requests): {stats.errors}")
        if stats.failed_urls:
            logger.warning("  The following URLs failed to scrape:")
            for url in stats.failed_urls:
                logger.warning(f"    - {url}")
        logger.info("---------------------------------" + "-" * len(competitor_name))