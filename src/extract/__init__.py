# src/extract/__init__.py
# This file serves as the router for the extraction phase.

import logging
import httpx
import asyncio
from datetime import datetime, timedelta

# Import common helper functions
from ._common import is_recent, _get_existing_urls, _get_post_details

# Import competitor-specific extraction functions
from .competitors.terminalfour import extract_from_terminalfour
from .competitors.moderncampus import extract_from_modern_campus
from .competitors.squiz import extract_from_squiz

logger = logging.getLogger(__name__)

async def extract_posts_in_batches(config, days=30, scrape_all=False, batch_size=10):
    """
    A router function that scrapes posts in batches and yields them.
    It dispatches to the correct competitor-specific extraction logic.
    """
    competitor_name = config['name']

    if competitor_name == 'terminalfour':
        async for batch in extract_from_terminalfour(config, days, scrape_all, batch_size):
            yield batch
    elif competitor_name == 'modern campus':
        async for batch in extract_from_modern_campus(config, days, scrape_all, batch_size):
            yield batch
    elif competitor_name == 'squiz':
        async for batch in extract_from_squiz(config, days, scrape_all, batch_size):
            yield batch
    else:
        logger.warning(f"No specific scraping logic found for {competitor_name}. Skipping.")
        return # Yields nothing if no logic is found

