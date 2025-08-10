# src/extract/__init__.py
# This file serves as the router for the extraction phase.

import logging
import importlib

logger = logging.getLogger(__name__)

# A mapping from the pattern name in the config to the module that handles it.
PATTERN_MAP = {
    "multi_category_pagination": ".competitors.multi_category_pagination",
    "single_list_pagination": ".competitors.single_list_pagination",
    "single_page_filter": ".competitors.single_page_filter",
}

async def extract_posts_in_batches(config, days=30, scrape_all=False, batch_size=10):
    """
    A router function that dynamically dispatches to the correct
    competitor-specific extraction logic based on the configured pattern.
    """
    competitor_name = config['name']
    pattern = config.get('scraping_pattern')

    if not pattern or pattern not in PATTERN_MAP:
        logger.warning(f"No valid scraping_pattern found for '{competitor_name}'. Skipping.")
        return

    try:
        # Dynamically import the module that corresponds to the pattern
        module_path = PATTERN_MAP[pattern]
        scraper_module = importlib.import_module(module_path, package='src.extract')
        
        # Call the standardized 'scrape' function within that module
        async for batch in scraper_module.scrape(config, days, scrape_all, batch_size):
            yield batch

    except ImportError:
        logger.error(f"Could not import the scraper module for pattern '{pattern}'. Skipping '{competitor_name}'.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while scraping '{competitor_name}': {e}")