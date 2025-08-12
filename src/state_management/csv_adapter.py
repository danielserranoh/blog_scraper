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
    # --- Add 'mode' parameter with a default of 'append' ---
    def save(self, posts, competitor_name, mode='append'):
        """
        Saves posts to the canonical CSV state file for the competitor.

        Args:
            posts (list): The list of post dictionaries to save.
            competitor_name (str): The name of the competitor.
            mode (str): 'append' to add new posts, 'overwrite' to replace the file.
        """
        if not posts:
            logger.warning(f"No posts provided to save for {competitor_name}.")
            return

        state_folder = os.path.join('state', competitor_name)
        os.makedirs(state_folder, exist_ok=True)
        state_filepath = os.path.join(state_folder, f"{competitor_name}_state.csv")

        # --- Use 'w' for overwrite, 'a' for append ---
        file_mode = 'w' if mode == 'overwrite' else 'a'
        # The header should only be written if the file is being newly created.
        write_header = not os.path.exists(state_filepath)

        try:
            fieldnames = ['competitor', 'title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
            with open(state_filepath, file_mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if write_header:
                    writer.writeheader()
                writer.writerows(posts)
            
            action = "overwritten" if mode == 'overwrite' else "appended to"
            logger.info(f"Successfully {action} {len(posts)} posts in state file: {state_filepath}")

        except IOError as e:
            logger.error(f"Could not write to state file {state_filepath}: {e}")