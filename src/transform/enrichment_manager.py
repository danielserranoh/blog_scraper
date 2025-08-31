# src/transform/enrichment_manager.py
# This module contains the high-level logic for managing the post enrichment process.

import os
import csv
import logging
from .live import transform_posts_live
from .batch_manager import BatchJobManager
from src.state_management.state_manager import StateManager
from src import utils

logger = logging.getLogger(__name__)

class EnrichmentManager:
    """
    Manages the process of enriching existing posts from the canonical state file.
    It discovers posts with missing data and submits them for enrichment via
    either live or batch API calls.
    """
    def __init__(self, app_config, state_manager, batch_manager):
        self.batch_manager = batch_manager
        self.state_manager = state_manager
        self.app_config = app_config

    async def enrich_posts(self, competitor, posts_to_enrich, all_posts_for_merge, batch_threshold, live_model, batch_model, wait, source_raw_filepath):
        """
        The central point for all post enrichment. It decides whether to use
        live or batch mode and then calls the appropriate manager.
        This function returns a list of enriched posts.
        """
        competitor_name = competitor['name']
        
        if len(posts_to_enrich) < batch_threshold:
            logger.info(f"Processing {len(posts_to_enrich)} posts in LIVE mode...")
            enriched_posts = await transform_posts_live(posts_to_enrich, live_model)
            if not enriched_posts:
                return None
            
            # Merge the new enriched data with the original posts
            enriched_map = {post['url']: post for post in enriched_posts}
            final_posts = [enriched_map.get(post['url'], post) for post in all_posts_for_merge]
            return final_posts
        else:
            logger.info(f"Processing {len(posts_to_enrich)} posts in BATCH mode...")
            await self.batch_manager.submit_new_jobs(
                competitor, 
                posts_to_enrich, 
                batch_model, 
                self.app_config,
                source_raw_filepath,
                wait
            )
            return None


    async def run_enrichment_process(self, competitor, batch_threshold, live_model, batch_model, app_config, wait):
        """
        Discovers posts that require enrichment and submits them for processing.
        """
        competitor_name = competitor['name']
        
        all_posts_from_file, posts_to_enrich = self._find_posts_to_enrich(competitor_name)
        
        if not posts_to_enrich:
            logger.info(f"No posts found that require enrichment for '{competitor_name}'.")
            return
        
        logger.info(f"Will enrich {len(posts_to_enrich)} posts for '{competitor_name}'.")
        
        final_posts = await self.enrich_posts(
            competitor,
            posts_to_enrich,
            all_posts_from_file,
            batch_threshold,
            live_model,
            batch_model,
            wait,
            source_raw_filepath=None
        )

        if final_posts:
            self.state_manager.save_processed_data(final_posts, competitor_name, "placeholder.csv")


    async def enrich_raw_data(self, competitor, batch_threshold, live_model, batch_model, app_config, wait):
        """
        Loads all raw data for a competitor and submits it for enrichment.
        This is a recovery method for failed scrapes.
        """
        competitor_name = competitor['name']
        raw_posts = self.state_manager.load_raw_data(competitor_name)
        
        if not raw_posts:
            logger.info(f"No raw data found for '{competitor_name}'.")
            return

        logger.info(f"Found {len(raw_posts)} raw posts to enrich for '{competitor_name}'.")

        latest_raw_filepath = self.state_manager.get_latest_raw_filepath(competitor_name)
        
        final_posts = await self.enrich_posts(
            competitor,
            raw_posts,
            raw_posts,
            batch_threshold,
            live_model,
            batch_model,
            wait,
            source_raw_filepath=latest_raw_filepath
        )
        
        if final_posts:
            self.state_manager.save_processed_data(final_posts, competitor_name, "placeholder.csv")

    def _find_posts_to_enrich(self, competitor_name):
        """
        Loads all posts from the 'processed' directory and returns a list of
        those that are missing enrichment data.
        """
        processed_posts = self.state_manager.load_processed_data(competitor_name)
        posts_to_enrich = []
        for post in processed_posts:
            if post.get('summary') == 'N/A' or post.get('seo_keywords') == 'N/A':
                posts_to_enrich.append(post)
        return processed_posts, posts_to_enrich