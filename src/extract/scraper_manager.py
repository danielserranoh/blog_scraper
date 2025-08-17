# src/extract/scraper_manager.py
# This module contains the high-level logic for managing the scraping process.

import os
import logging
from . import extract_posts_in_batches
from src.state_management import get_storage_adapter
from src.load import get_processed_data_adapter
from src.transform.enrichment_manager import EnrichmentManager

logger = logging.getLogger(__name__)


class ScraperManager:
    """
    Manages the end-to-end scraping workflow for a given competitor,
    including data extraction, saving to a raw state file, and
    deciding whether to submit for live or batch enrichment.
    """

    def __init__(self):
        self.enrichment_manager = EnrichmentManager()
    
    async def run_scrape_and_submit(self, competitor, days_to_scrape, scrape_all, batch_threshold, live_model, batch_model, app_config):
        """
        Scrapes new posts, saves the raw output, and submits for enrichment.
        """
        name = competitor['name']
        all_posts = []
        async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all):
            all_posts.extend(batch)
        if not all_posts: 
            return

        # 1. Save the raw, unprocessed data and get the filepath
        storage_adapter = get_storage_adapter(app_config)
        raw_filepath = storage_adapter.save(all_posts, name)

        if not raw_filepath:
            logger.error("Failed to save raw data, aborting enrichment.")
            return

        # 2. Pass the posts to the EnrichmentManager for processing
        await self.enrichment_manager.enrich_posts(
            competitor,
            all_posts,
            raw_filepath,
            batch_threshold,
            live_model,
            batch_model,
            app_config
        )