# src/transform/enrichment_manager.py
# This module contains the high-level logic for managing the post enrichment process.

import os
import csv
import logging
from typing import List, Dict, Any, Optional, Tuple
from .live import transform_posts_live
from .batch_manager import BatchJobManager
from src.state_management.state_manager import StateManager
from src.di_container import EnrichmentError
from src import utils

logger = logging.getLogger(__name__)

class EnrichmentManager:
    """
    Manages the process of enriching existing posts from the canonical state file.
    It discovers posts with missing data and submits them for enrichment via
    either live or batch API calls.
    """
    def __init__(self, app_config: Dict[str, Any], state_manager: StateManager, batch_manager: BatchJobManager):
        self.batch_manager = batch_manager
        self.state_manager = state_manager
        self.app_config = app_config

    async def enrich_posts(
        self, 
        competitor: Dict[str, Any], 
        posts_to_enrich: List[Dict[str, Any]], 
        all_posts_for_merge: List[Dict[str, Any]], 
        batch_threshold: int, 
        live_model: str, 
        batch_model: str, 
        wait: bool, 
        source_raw_filepath: Optional[str]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        The central point for all post enrichment. It decides whether to use
        live or batch mode and then calls the appropriate manager.
        This function returns a list of enriched posts.
        
        Args:
            competitor: Competitor configuration dictionary
            posts_to_enrich: List of posts that need enrichment
            all_posts_for_merge: All posts to merge with enriched results
            batch_threshold: Threshold for switching between live/batch mode
            live_model: Model name for live enrichment
            batch_model: Model name for batch enrichment
            wait: Whether to wait for batch jobs to complete
            source_raw_filepath: Path to source raw data file
            
        Returns:
            List of enriched posts or None if using batch mode
            
        Raises:
            EnrichmentError: If enrichment process fails
        """
        try:
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
                
        except Exception as e:
            logger.error(f"Enrichment failed for '{competitor_name}': {e}")
            raise EnrichmentError(
                f"Failed to enrich posts for {competitor_name}: {str(e)}",
                posts_count=len(posts_to_enrich),
                model=live_model if len(posts_to_enrich) < batch_threshold else batch_model,
                details={"batch_threshold": batch_threshold, "wait": wait}
            )

    def _find_posts_to_enrich(self, competitor_name: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Loads all posts from the 'processed' directory and returns a tuple of:
        1. All posts from file
        2. Posts that are missing enrichment data
        
        Args:
            competitor_name: Name of the competitor
            
        Returns:
            Tuple of (all_posts, posts_needing_enrichment)
            
        Raises:
            EnrichmentError: If loading processed data fails
        """
        try:
            processed_posts = self.state_manager.load_processed_data(competitor_name)
            posts_to_enrich = []
            
            for post in processed_posts:
                if (post.get('summary') in [None, 'N/A', ''] or 
                    post.get('seo_keywords') in [None, 'N/A', ''] or
                    post.get('funnel_stage') in [None, 'N/A', '']):
                    posts_to_enrich.append(post)
                    
            logger.info(f"Found {len(posts_to_enrich)} posts needing enrichment out of {len(processed_posts)} total")
            return processed_posts, posts_to_enrich
            
        except Exception as e:
            logger.error(f"Failed to find posts to enrich for '{competitor_name}': {e}")
            raise EnrichmentError(
                f"Failed to load posts for enrichment: {str(e)}",
                details={"competitor": competitor_name, "operation": "load_processed_data"}
            )