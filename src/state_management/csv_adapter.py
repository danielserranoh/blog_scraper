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
    def save(self, posts, competitor_name, file_type, source_filename=None):
        """
        Saves the list of posts to a CSV file in the 'data/raw/' or 'data/processed/' directory.
        """
        if not posts:
            logger.warning(f"No posts provided to save for {competitor_name}.")
            return None

        output_folder = os.path.join('data', file_type, competitor_name)
        os.makedirs(output_folder, exist_ok=True)
        
        # Use a consistent filename based on the type
        if file_type == 'raw':
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = os.path.join(output_folder, f"{competitor_name}_{timestamp}.csv")
        elif file_type == 'processed' and source_filename:
            filepath = os.path.join(output_folder, os.path.basename(source_filename))
        else:
            logger.error(f"Invalid file_type '{file_type}' or missing source_filename for processed data.")
            return None

        try:
            fieldnames = ['title', 'publication_date', 'url', 'funnel_stage', 'seo_keywords', 'summary', 'headings', 'schemas', 'seo_meta_keywords', 'content']
            
            posts_to_write = []
            for post in posts:
                for field in ['headings', 'schemas']:
                    if field in post and post[field]:
                        # Convert the list of dictionaries to a JSON string
                        post[field] = json.dumps(post[field])
                    else:
                        # Save as an empty JSON array if the field is missing or empty
                        post[field] = '[]'
                posts_to_write.append(post)

            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(posts_to_write)
            
            logger.info(f"Successfully saved {len(posts)} posts to: {filepath}")
            return filepath
        except IOError as e:
            logger.error(f"Could not write to data file {filepath}: {e}")
            return None

    def read(self, competitor_name, file_type):
        """
        Reads all posts from a specific data directory (raw or processed) for a given competitor.
        """
        input_folder = os.path.join('data', file_type, competitor_name)
        posts = []
        
        if not os.path.isdir(input_folder):
            logger.warning(f"No '{file_type}' data found for '{competitor_name}'.")
            return []

        for filename in os.listdir(input_folder):
            if filename.endswith('.csv'):
                filepath = os.path.join(input_folder, filename)
                try:
                    with open(filepath, mode='r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        posts.extend(list(reader))
                except Exception as e:
                    logger.error(f"Could not read file {filepath}: {e}")
        
        logger.info(f"Read {len(posts)} posts from the '{file_type}' directory for '{competitor_name}'.")
        return posts
    
    
    def read_urls(self, competitor_name, file_type):
        """
        Reads all post URLs from all CSV files in a specific data directory.
        """
        input_folder = os.path.join('data', file_type, competitor_name)
        urls = set()
        
        if not os.path.isdir(input_folder):
            return urls
        
        for filename in os.listdir(input_folder):
            if filename.endswith('.csv'):
                filepath = os.path.join(input_folder, filename)
                try:
                    with open(filepath, mode='r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            if 'url' in row and row['url']:
                                urls.add(row['url'])
                except Exception as e:
                    logger.error(f"Could not read URLs from file {filepath}: {e}")
        
        logger.info(f"Found {len(urls)} existing URLs in the '{file_type}' directory for '{competitor_name}'.")
        return urls