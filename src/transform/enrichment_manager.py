# src/transform/enrichment_manager.py
# This module contains the high-level logic for managing the post enrichment process.

import os
import csv
import logging
from typing import List, Dict, Any, Optional, Tuple
from .content_preprocessor import ContentPreprocessor
from .live import transform_posts_live
from .batch_manager import BatchJobManager
from src.state_management.state_manager import StateManager
from src.exceptions import EnrichmentError

from src import utils
from src.models import PostModel

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
            
            # Preprocess content for API consumption
            logger.info(f"Preprocessing {len(posts_to_enrich)} posts for enrichment")
            processed_posts = ContentPreprocessor.prepare_posts_for_enrichment(posts_to_enrich)
            
            # Check if preprocessing created chunks (affects our batch threshold decision)
            if len(processed_posts) < batch_threshold:
                logger.info(f"Processing {len(processed_posts)} items in LIVE mode...")
                enriched_posts = await transform_posts_live(processed_posts, live_model)
                if not enriched_posts:
                    return None
                
                # Merge chunked results back together if necessary
                merged_posts = ContentPreprocessor.merge_chunked_results(enriched_posts)
                
                # Merge the new enriched data with the original posts
                enriched_map = {post['url']: post for post in merged_posts}
                final_posts = [enriched_map.get(post['url'], post) for post in all_posts_for_merge]
                return final_posts
            else:
                logger.info(f"Processing {len(processed_posts)} items in BATCH mode...")
                await self.batch_manager.submit_new_jobs(
                    competitor, 
                    processed_posts,  # Use preprocessed posts
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
        2. Posts that are missing enrichment data OR failed previous enrichment
        
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
            failed_count = 0
            missing_count = 0
            
            for post in processed_posts:
                # Use data model to check if post needs enrichment
                needs_enrichment, missing_fields = PostModel.needs_enrichment(post)
                
                if needs_enrichment:
                    posts_to_enrich.append(post)
                    logger.debug(f"Post '{post.get('title', 'unknown')}' needs enrichment - missing: {', '.join(missing_fields)}")
                    
                    # Count different types of missing data for reporting
                    if any('strategic_analysis' in field for field in missing_fields):
                        missing_count += 1
                    # Check for failed status in both old and new metadata structure
                    if (post.get('enrichment_status') == 'failed' or 
                        post.get('metadata', {}).get('enrichment_status') == 'failed'):
                        failed_count += 1
            
            # Provide detailed logging about what needs enrichment
            if posts_to_enrich:
                logger.info(f"Found {len(posts_to_enrich)} posts needing enrichment:")
                if missing_count > 0:
                    logger.info(f"  - {missing_count} posts missing strategic analysis or other enrichment data")
                if failed_count > 0:
                    logger.info(f"  - {failed_count} posts with previous enrichment failures")
                
                # Show some example missing fields for the first few posts
                for i, post in enumerate(posts_to_enrich[:3]):
                    _, missing_fields = PostModel.needs_enrichment(post)
                    logger.info(f"  - '{post.get('title', 'unknown')[:50]}...' missing: {', '.join(missing_fields[:3])}")
                    
            else:
                logger.info(f"All {len(processed_posts)} posts are fully enriched with strategic analysis")
                    
            return processed_posts, posts_to_enrich
            
        except Exception as e:
            logger.error(f"Failed to find posts to enrich for '{competitor_name}': {e}")
            raise EnrichmentError(
                f"Failed to load posts for enrichment: {str(e)}",
                details={"competitor": competitor_name, "operation": "load_processed_data"}
            )