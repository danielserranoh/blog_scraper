# src/load/export_manager.py
# This module contains the high-level logic for managing the data export process.

import os
import csv
import logging
import json
from . import exporters
from .file_saver import save_export_file
from src.state_management.state_manager import StateManager # <--- ADD THIS

logger = logging.getLogger(__name__)

class ExportManager:
    """
    Manages the process of exporting data from the processed directory
    to user-facing files in various formats.
    """

    def __init__(self, app_config, state_manager):
        self.app_config = app_config
        self.state_manager = state_manager
    
    def _get_post_richness_score(self, post):
        """
        Calculates a score for a post based on how many fields contain
        meaningful data.
        """
        score = 0
        for key, value in post.items():
            # A field is considered "rich" if it's not None, 'N/A', or an empty string/list
            if value is not None and value != 'N/A' and value != '' and value != []:
                score += 1
        return score
        
    def _deduplicate_and_merge_posts(self, all_posts):
        """
        Deduplicates a list of posts based on URL, keeping the most data-rich entry
        in case of duplicates.
        """
        unique_posts_map = {}
        for post in all_posts:
            url = post.get('url')
            if not url:
                continue
            
            # If we haven't seen this URL before, add it
            if url not in unique_posts_map:
                unique_posts_map[url] = post
            else:
                # If a duplicate is found, compare scores and keep the richest one
                existing_post = unique_posts_map[url]
                if self._get_post_richness_score(post) > self._get_post_richness_score(existing_post):
                    unique_posts_map[url] = post
                    
        return list(unique_posts_map.values())


    def run_export_process(self, competitors_to_export, export_format, app_config):
        """
        Reads from the 'processed' data directory to create user-facing exports.
        """
        logger.info(f"--- Starting export process to {export_format.upper()} ---")
        all_posts_to_export = []
        
        # Determine the file extension from the configuration
        file_extension = app_config.get('processed_data', {}).get('adapter', 'csv')
        
        for competitor in competitors_to_export:
            competitor_name = competitor['name']
            
            processed_posts = self.state_manager.load_processed_data(competitor_name)
            
            if not processed_posts:
                logger.warning(f"No processed data found for '{competitor_name}'. Please run --check-job first.")
                continue

            for post in processed_posts:
                post['competitor'] = competitor_name
                all_posts_to_export.append(post)

        if not all_posts_to_export:
            logger.warning("‼️ No data found to export. Please ensure you have processed data.")
            return
            
        final_posts = self._deduplicate_and_merge_posts(all_posts_to_export)

        try:
            formatted_data = exporters.export_data(final_posts, export_format, app_config)
        except ValueError as e:
            logger.error(e)
            return
        if export_format == 'gsheets':
            logger.info(formatted_data)
        else:
            save_export_file(formatted_data, export_format, competitors_to_export)