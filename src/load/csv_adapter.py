# src/load/csv_adapter.py
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
    def save(self, posts, competitor_name):
        """
        Appends new posts to a single, canonical CSV file for the competitor,
        which serves as the state for the scraper.
        """
        if not posts:
            logger.warning(f"No new posts provided to save for {competitor_name}.")
            return

        state_folder = os.path.join('state', competitor_name)
        os.makedirs(state_folder, exist_ok=True)
        
        # Use a single, consistent filename for state management
        state_filepath = os.path.join(state_folder, f"{competitor_name}_state.csv")

        file_exists = os.path.exists(state_filepath)

        try:
            fieldnames = ['title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
            with open(state_filepath, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if not file_exists:
                    writer.writeheader()
                writer.writerows(posts)
            logger.info(f"Successfully appended {len(posts)} posts to state file: {state_filepath}")
        except IOError as e:
            logger.error(f"Could not write to state file {state_filepath}: {e}")