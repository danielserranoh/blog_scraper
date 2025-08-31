# src/extract/scraper_manager.py
# This module contains the high-level logic for managing the scraping process.

import os
import logging
from . import extract_posts_in_batches
from src.state_management.state_manager import StateManager
from src.extract._common import ScrapeStats

logger = logging.getLogger(__name__)

class ScraperManager:
    """
    Manages the end-to-end scraping workflow for a given competitor,
    and returns the posts to the orchestrator for further processing.
    """

    def __init__(self, app_config, state_manager):
        self.state_manager = state_manager
        self.app_config = app_config
    
    async def scrape_and_return_posts(self, competitor, days_to_scrape, scrape_all):
        """
        Scrapes new posts and returns the list of posts to the orchestrator.
        """
        name = competitor['name']
        
        existing_urls = self.state_manager.load_raw_urls(name)
        
        all_posts = []
        batch_size = self.app_config.get('batch_threshold', 10) # Using batch_threshold as batch_size
        
        async for batch in extract_posts_in_batches(competitor, days_to_scrape, scrape_all, batch_size, existing_urls):
            all_posts.extend(batch)
        
        if not all_posts: 
            return None

        return all_posts