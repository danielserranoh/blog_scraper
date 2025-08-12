# src/extract/_common.py
# This file contains common helper functions for the extraction phase.

import csv
import os
import re
from datetime import datetime
import logging
import httpx
from bs4 import BeautifulSoup
from dateutil.parser import parse as dateparse
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class ScrapeStats:
    """A simple class to hold statistics for a scraping run."""
    def __init__(self):
        self.successful = 0
        self.skipped = 0
        self.errors = 0
        self.failed_urls = []

def _get_existing_urls(competitor_name):
    """
    Reads the canonical state CSV file to get a list of all previously
    scraped post URLs.
    """
    existing_urls = set()
    state_filepath = os.path.join('state', competitor_name, f"{competitor_name}_state.csv")

    if os.path.exists(state_filepath):
        try:
            with open(state_filepath, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if 'url' in row and row['url']:
                        existing_urls.add(row['url'])
            logger.info(f"Found {len(existing_urls)} existing URLs in state file: {state_filepath}.")
        except Exception as e:
            logger.error(f"Could not read state file at {state_filepath}: {e}")
    else:
        logger.info("No previous state file found. Starting a fresh scrape.")

    return existing_urls

async def _get_post_details(client, base_url, post_url_path, config, stats): 
    """
    Scrapes an individual blog post page using selectors from the config.
    """
    full_url = post_url_path if post_url_path.startswith(('http://', 'https://')) else f"{base_url.rstrip('/')}/{post_url_path.lstrip('/')}"
    logger.debug(f"  Scraping details from: {full_url}")
    
    try:
        response = await client.get(full_url, follow_redirects=True) 
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        date_selector = config.get('date_selector')
        content_selector = config.get('content_selector')
        content_filter_selector = config.get('content_filter_selector')

        title_element = soup.find('h1') or soup.find('h2')
        title = title_element.text.strip() if title_element else 'No Title Found'
        
        # --- FIX: Re-introduced Meta Keyword Extraction ---
        keywords_meta = soup.find('meta', {'name': 'keywords'})
        seo_meta_keywords = keywords_meta.get('content', 'N/A') if keywords_meta else 'N/A'

        pub_date = None
        if date_selector:
            date_element = soup.select_one(date_selector)
            if date_element:
                date_text = date_element.get('datetime') or date_element.get_text()
                try:
                    pub_date = dateparse(date_text)
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse date: '{date_text}' from {full_url}")
        
        content_text = ""
        if content_selector:
            content_container = soup.select_one(content_selector)
            if content_container:
                # --- FIX: Apply the content filter if it's defined in the config ---
                if content_filter_selector:
                    element_to_remove = content_container.select_one(content_filter_selector)
                    if element_to_remove:
                        element_to_remove.decompose() # Remove the element from the parse tree
                
                content_text = ' '.join(content_container.get_text(separator=' ', strip=True).split())

        return {
            'title': title,
            'url': full_url,
            'publication_date': pub_date.strftime('%Y-%m-%d') if pub_date else 'N/A',
            'content': content_text if content_text else 'N/A',
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'seo_meta_keywords': seo_meta_keywords
        }

    except httpx.RequestError as e: 
        logger.error(f"Error fetching post details from {full_url}: {e}")
        stats.errors += 1
        stats.failed_urls.append(full_url)
        return None
    

def get_next_page_url(pagination_config, soup, current_url, page_number, base_url):
    """
    Determines the URL of the next page to scrape based on the pagination pattern.
    Returns None if there is no next page.
    """
    if not pagination_config:
        return None

    pagination_type = pagination_config.get('type')
   

    if pagination_type in ["linked_path", "linked_ajax"]:
        selector = pagination_config.get('selector')
        if selector:
            next_link_element = soup.select_one(selector)
            if next_link_element and next_link_element.get('href'):
                return urljoin(base_url, next_link_element['href'])
    
    elif pagination_type == "numeric_query":
        query_param = pagination_config.get('selector', 'page') # Default to 'page' if not specified
        next_page_num = page_number + 1
        
        base_path = current_url.split('?')[0]
        return f"{base_path}?{query_param}={next_page_num}"
            
    return None