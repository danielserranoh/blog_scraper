# src/transform/enrichment_manager.py
# This module contains the high-level logic for managing the post enrichment process.

import os
import csv
import logging
from .live import transform_posts_live
from .batch_manager import BatchJobManager
from src.state_management import get_storage_adapter
from src.load import get_processed_data_adapter

logger = logging.getLogger(__name__)

class EnrichmentManager:
    """
    Manages the process of enriching existing posts from the canonical state file.
    It discovers posts with missing data and submits them for enrichment via
    either live or batch API calls.
    """
    def __init__(self):
        self.batch_manager = BatchJobManager()
        self.processed_data_adapter = get_processed_data_adapter({})

    async def run_enrichment_process(self, competitor, batch_threshold, live_model, batch_model, app_config):
        """
        Discovers posts in the state file that require enrichment, and submits
        them for processing.
        """
        competitor_name = competitor['name']
        state_folder = os.path.join("state", competitor_name)
        if not os.path.isdir(state_folder):
            logger.warning(f"No state folder found for '{competitor_name}'. Skipping enrichment.")
            return

        state_filepath = os.path.join(state_folder, f"{competitor_name}_state.csv")
        if not os.path.exists(state_filepath):
            logger.warning(f"No state file found for '{competitor_name}'. Skipping enrichment.")
            return

        all_posts_from_file = []
        posts_to_enrich = []
        with open(state_filepath, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for post in reader:
                all_posts_from_file.append(post)
                if post.get('summary') == 'N/A' or post.get('seo_keywords') == 'N/A':
                    posts_to_enrich.append(post)
        
        if not posts_to_enrich:
            logger.info(f"No posts found that require enrichment for '{competitor_name}'.")
            return
        
        logger.info(f"Will enrich {len(posts_to_enrich)} posts for '{competitor_name}'.")
        
        # Now call the single enrichment method
        await self.enrich_posts(
            competitor,
            posts_to_enrich,
            state_filepath,
            batch_threshold,
            live_model,
            batch_model,
            app_config
        )


    async def enrich_posts(self, competitor, posts, raw_filepath, batch_threshold, live_model, batch_model, app_config):
        """
        The central point for all post enrichment. It decides whether to use
        live or batch mode and then calls the appropriate manager.
        """
        if len(posts) < batch_threshold and not raw_filepath.endswith('.jsonl'):
            logger.info(f"Processing {len(posts)} posts in LIVE mode...")
            enriched_posts = await transform_posts_live(posts, live_model)
            if enriched_posts:
                self.processed_data_adapter.save(enriched_posts, competitor['name'], os.path.basename(raw_filepath))
        else:
            logger.info(f"Processing {len(posts)} posts in BATCH mode...")
            await self.batch_manager.submit_new_jobs(
                competitor, 
                posts, 
                batch_model, 
                app_config,
                raw_filepath
            )