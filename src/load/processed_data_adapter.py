# src/load/processed_data_adapter.py
# This module contains adapters for saving the final, processed data.

import os
import csv
from datetime import datetime
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseProcessedDataAdapter(ABC):
    """
    Abstract Base Class for adapters that save the final, processed data.
    """
    @abstractmethod
    def save(self, posts, competitor_name, source_filename):
        """
        The core method to save a list of final, enriched posts.
        
        Args:
            posts (list): The list of enriched post dictionaries.
            competitor_name (str): The name of the competitor.
            source_filename (str): The filename of the raw data file that
                                 was the source of these posts.
        """
        pass


class CsvProcessedDataAdapter(BaseProcessedDataAdapter):
    """
    An adapter that saves the final, processed data to a CSV file in the
    'data/processed/' directory.
    """
    def save(self, posts, competitor_name, source_filename):
        if not posts:
            logger.warning(f"No processed posts provided to save for {competitor_name}.")
            return None

        output_folder = os.path.join('data', 'processed', competitor_name)
        os.makedirs(output_folder, exist_ok=True)
        
        # Use the same base filename as the raw file, but in the processed directory
        processed_filepath = os.path.join(output_folder, os.path.basename(source_filename))

        try:
            fieldnames = ['title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
            with open(processed_filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(posts)
            logger.info(f"Successfully saved {len(posts)} processed posts to: {processed_filepath}")
            return processed_filepath
        except IOError as e:
            logger.error(f"Could not write to processed data file {processed_filepath}: {e}")
            return None