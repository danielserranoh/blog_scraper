# src/extract/scraper_manager.py
# This module contains the high-level logic for managing the scraping process.

import os
import logging
from . import extract_posts_in_batches
from src.state_management.state_manager import StateManager
from src.transform.enrichment_manager import EnrichmentManager
from src.extract._common import ScrapeStats

logger = logging.getLogger(__name__)


class ScraperManager:
    """
    Manages the end-to-end scraping workflow for a given competitor,
    including data extraction, saving to a raw state file, and
    deciding whether to submit for live or batch enrichment.
    """

    def __init__(self, app_config):
        self.enrichment_manager = EnrichmentManager(app_config)
        self.state_manager = StateManager(app_config)
    
    async def run_scrape_and_submit(self, competitor, days_to_scrape, scrape_all, batch_threshold, live_model, batch_model, app_config):
        """
        Scrapes new posts, saves the raw output, and submits for enrichment.
        """
        name = competitor['name']
        
        existing_urls = self.state_manager.load_raw_urls(name)
        
        all_posts = []
        
        # <--- CORRECTED: Pass the correct arguments to extract_posts_in_batches --->
        async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all, batch_threshold, existing_urls):
            all_posts.extend(batch)
        
        if not all_posts: 
            return

        # 1. Save the raw, unprocessed data and get the filepath
        raw_filepath = self.state_manager.save_raw_data(all_posts, name)

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