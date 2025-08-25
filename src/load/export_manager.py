# src/load/export_manager.py
# This module contains the high-level logic for managing the data export process.

import os
import csv
import json
import logging
from . import exporters
from .file_saver import save_export_file

logger = logging.getLogger(__name__)

class ExportManager:
    """
    Manages the process of exporting data from the processed directory
    to user-facing files in various formats.
    """

    def __init__(self):
        pass
    
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
        
        for competitor in competitors_to_export:
            competitor_name = competitor['name']
            # --- UPDATED PATH: Read from the 'processed' data directory ---
            processed_folder = os.path.join("data", "processed", competitor_name)
            
            if not os.path.isdir(processed_folder):
                logger.warning(f"No processed data found for '{competitor_name}'. Skipping.")
                continue
            
            # Read all CSVs in the processed folder for that competitor
            for filename in os.listdir(processed_folder):
                if filename.endswith('.csv'):
                    filepath = os.path.join(processed_folder, filename)
                    logger.info(f"Reading data for '{competitor_name}' from: {filename}")
                    with open(filepath, mode='r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for post in reader:
                            post['competitor'] = competitor_name
                            all_posts_to_export.append(post)

        if not all_posts_to_export:
            logger.warning("‼️ No data found to export.")
            return

        # <--- ADDED: Deserialization step for headings and schemas --->
        for post in all_posts_to_export:
            for field in ['headings', 'schemas']:
                 # Ensure the field exists and is not an empty string before attempting to parse
                if post.get(field, '').strip():
                    #print(f"Para el campo {field}, se ha recuperado del csv el valor {post[field]} del tipo {type(post[field])}")
                    try:
                        post[field] = json.loads(post[field].replace("'", '"'))
                        print(post[field])
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON for field '{field}' in post '{post.get('url')}'. Setting to empty list.")
                        post[field] = []
                else:
                    # If the field is an empty string, set it to an empty list
                    post[field] = []

        # --- NEW: Deduplicate and merge posts before exporting ---
        final_posts = self._deduplicate_and_merge_posts(all_posts_to_export)

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