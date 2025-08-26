# src/load/export_manager.py
# This module contains the high-level logic for managing the data export process.

import os
import csv
import logging
import json
from . import exporters
from .file_saver import save_export_file

logger = logging.getLogger(__name__)

class ExportManager:
    """
    Manages the process of exporting data from the processed directory
    to user-facing files in various formats.
    """

    def __init__(self, app_config):
        self.app_config = app_config
    
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

        file_extension = app_config.get('processed_data', {}).get('adapter', 'json')
        
        print(f"DEBUG: Processing these competitors: {[c['name'] for c in competitors_to_export]}")


        for competitor in competitors_to_export:
            competitor_name = competitor['name']
            processed_folder = os.path.join("data", "processed", competitor_name)
            
            if not os.path.isdir(processed_folder):
                logger.warning(f"No processed data found for '{competitor_name}'. Skipping.")
                continue
            
            # <--- MODIFIED: Use the JSON-based read, which returns native objects --->
            # We set up the type of file in the config.json
            # The previous CSV-specific deserialization loop is now removed.
            # We now read all files from the processed directory.
            files_found = False
            for filename in os.listdir(processed_folder):
                if filename.endswith(f'.{file_extension}'):
                    files_found = True
                    filepath = os.path.join(processed_folder, filename)
                    logger.info(f"Reading data for '{competitor_name}' from: {filename}")
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for post in data:
                            post['competitor'] = competitor_name
                            all_posts_to_export.append(post)
            # --- DEBUG: Print if any files were found in this competitor's folder ---
            if not files_found:
                print(f"DEBUG: No files with '.{file_extension}' extension found in {processed_folder}")

        # --- DEBUG: Print the total number of posts read from all files ---
        print(f"DEBUG: Total posts read before deduplication: {len(all_posts_to_export)}")


        if not all_posts_to_export:
            logger.warning("‼️ No data found to export. Please ensure you have processed data.")
            return
            
        final_posts = self._deduplicate_and_merge_posts(all_posts_to_export)

        # --- DEBUG: Print the number of posts after deduplication ---
        print(f"DEBUG: Total posts after deduplication: {len(final_posts)}")

        try:
            formatted_data = exporters.export_data(final_posts, export_format, app_config)
        except ValueError as e:
            logger.error(e)
            return
        # For gsheets, the returned data is a success message, not file content
        if export_format == 'gsheets':
            # For Google Sheets, the return value is a status message, so we just log it.
            logger.info(formatted_data)
        else:
            # For all other formats, call our new dedicated saver function.
            save_export_file(formatted_data, export_format, competitors_to_export)