# src/extract/scraper_manager.py
# This module contains the high-level logic for managing the scraping process.

import os
import logging
from typing import Optional, List, Dict, Any
from . import extract_posts_in_batches
from src.state_management.state_manager import StateManager
from src.extract._common import ScrapeStats
from src.exceptions import ScrapingError

logger = logging.getLogger(__name__)

class ScraperManager:
    """
    Manages the end-to-end scraping workflow for a given competitor,
    and returns the posts to the orchestrator for further processing.
    """

    def __init__(self, app_config: Dict[str, Any], state_manager: StateManager):
        self.state_manager = state_manager
        self.app_config = app_config
    
    async def scrape_and_return_posts(self, competitor: Dict[str, Any], days: Optional[int], scrape_all: bool) -> Optional[List[Dict[str, Any]]]:
        """
        Scrapes new posts and returns the list of posts to the orchestrator.
        
        Args:
            competitor: Competitor configuration dictionary
            days: Number of days to scrape (None if scrape_all is True)
            scrape_all: Whether to scrape all available posts
            
        Returns:
            List of scraped posts or None if no posts found
            
        Raises:
            ScrapingError: If scraping process fails
        """
        try:
            competitor_name = competitor['name']
            logger.info(f"Starting scrape for '{competitor_name}' (days: {days}, all: {scrape_all})")
            
            # Load existing URLs to avoid duplicates
            existing_urls = self.state_manager.load_raw_urls(competitor_name)
            
            all_posts = []
            batch_size = self.app_config.get('batch_threshold', 10) # Using batch_threshold as batch_size
            
            async for batch in extract_posts_in_batches(competitor, days, scrape_all, batch_size, existing_urls):
                all_posts.extend(batch)
            
            if not all_posts: 
                logger.info(f"No new posts found for '{competitor_name}'")
                return None

            logger.info(f"Successfully scraped {len(all_posts)} posts for '{competitor_name}'")
            return all_posts
            
        except Exception as e:
            logger.error(f"Scraping failed for '{competitor.get('name', 'unknown')}': {e}")
            raise ScrapingError(
                f"Failed to scrape posts for {competitor.get('name', 'unknown')}: {str(e)}",
                competitor=competitor.get('name'),
                details={"days": days, "scrape_all": scrape_all}
            )