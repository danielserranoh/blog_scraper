# src/state_management/csv_adapter.py
# This file contains the logic for saving data to a state CSV file.

import csv
import os
import logging
from datetime import datetime
from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)

class CsvAdapter(BaseAdapter):
    """
    A storage adapter for saving and managing scraped data in a .csv file.
    """
    def save(self, posts, competitor_name, mode='append'):
        """
        Saves the list of raw post data to a new, timestamped CSV file
        in the data/raw/ directory. The 'mode' is ignored as we always
        create a new file for each scrape.
        """
        if not posts:
            logger.warning(f"No new posts provided to save for {competitor_name}.")
            return

        output_folder = os.path.join('data', 'raw', competitor_name)
        os.makedirs(output_folder, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = os.path.join(output_folder, f"{competitor_name}_{timestamp}.csv")

        try:
            fieldnames = ['title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if write_header:
                    writer.writeheader()
                writer.writerows(posts)
            
            logger.info(f"Successfully saved {len(posts)} raw posts to: {filepath}")
        except IOError as e:
            logger.error(f"Could not write to raw data file {filepath}: {e}")