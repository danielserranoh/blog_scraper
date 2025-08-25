# src/state_management/json_adapter.py
# This file contains the logic for saving data to a state JSON file.

import os
import json
import logging
from datetime import datetime
from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)

class JsonAdapter(BaseAdapter):
    """
    A storage adapter for saving and managing scraped data in a .json file.
    """
    def save(self, posts, competitor_name, file_type, source_filename=None):
        """
        Saves the list of posts to a JSON file in the 'data/raw/' or 'data/processed/' directory.
        """
        if not posts:
            logger.warning(f"No posts provided to save for {competitor_name}.")
            return None

        output_folder = os.path.join('data', file_type, competitor_name)
        os.makedirs(output_folder, exist_ok=True)
        
        # Use a consistent filename based on the type
        if file_type == 'raw':
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = os.path.join(output_folder, f"{competitor_name}_{timestamp}.json")
        elif file_type == 'processed' and source_filename:
            # Change file extension to .json
            base_filename = os.path.splitext(os.path.basename(source_filename))[0]
            filepath = os.path.join(output_folder, f"{base_filename}.json")
        else:
            logger.error(f"Invalid file_type '{file_type}' or missing source_filename for processed data.")
            return None
        
        try:
            with open(filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(posts, jsonfile, indent=4)
            
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
            if filename.endswith('.json'):
                filepath = os.path.join(input_folder, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        posts.extend(data)
                except Exception as e:
                    logger.error(f"Could not read file {filepath}: {e}")
        
        logger.info(f"Read {len(posts)} posts from the '{file_type}' directory for '{competitor_name}'.")
        return posts
    
    def read_urls(self, competitor_name, file_type):
        """
        Reads all post URLs from all JSON files in a specific data directory.
        """
        input_folder = os.path.join('data', file_type, competitor_name)
        urls = set()
        
        if not os.path.isdir(input_folder):
            return urls
        
        for filename in os.listdir(input_folder):
            if filename.endswith('.json'):
                filepath = os.path.join(input_folder, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for post in data:
                            if 'url' in post and post['url']:
                                urls.add(post['url'])
                except Exception as e:
                    logger.error(f"Could not read URLs from file {filepath}: {e}")
        
        logger.info(f"Found {len(urls)} existing URLs in the '{file_type}' directory for '{competitor_name}'.")
        return urls