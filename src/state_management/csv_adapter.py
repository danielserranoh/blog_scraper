# src/state_management/csv_adapter.py
# This file contains the logic for saving data to a state CSV file.

import csv
import os
import logging
from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)

class CsvAdapter(BaseAdapter):
    """
    A storage adapter for saving and managing scraped data in a .csv file.
    """
    def save(self, posts, competitor_name, mode='append'):
        """
        Saves posts to the canonical CSV state file for the competitor.
        """
        if not posts:
            logger.warning(f"No new posts provided to save for {competitor_name}.")
            return

        state_folder = os.path.join('state', competitor_name)
        os.makedirs(state_folder, exist_ok=True)
        state_filepath = os.path.join(state_folder, f"{competitor_name}_state.csv")

        # --- NEW: Get the current number of posts before writing ---
        existing_post_count = 0
        # Only count if the file exists and we're in append mode
        if mode == 'append' and os.path.exists(state_filepath):
            try:
                with open(state_filepath, mode='r', newline='', encoding='utf-8') as f:
                    # Sum the lines, subtract 1 for the header
                    existing_post_count = sum(1 for row in f) - 1
            except Exception:
                # If there's an error reading, default to 0
                existing_post_count = 0

        file_mode = 'w' if mode == 'overwrite' else 'a'
        write_header = not os.path.exists(state_filepath)

        try:
            fieldnames = ['title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
            
            with open(state_filepath, file_mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if write_header:
                    writer.writeheader()
                writer.writerows(posts)
            
            # --- NEW: More informative log message ---
            if mode == 'overwrite':
                logger.info(f"Successfully overwrote state file with {len(posts)} posts: {state_filepath}")
            else:
                total_posts = existing_post_count + len(posts)
                logger.info(f"Successfully appended {len(posts)} new posts to state file ({total_posts} total): {state_filepath}")

        except IOError as e:
            logger.error(f"Could not write to state file {state_filepath}: {e}")