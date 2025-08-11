# src/extract/_common.py
# This file contains common helper functions for the extraction phase.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import json
import time
import os
import csv
import logging
import httpx
import asyncio

logger = logging.getLogger(__name__)

class ScrapeStats:
    """A simple class to hold statistics for a scraping run."""
    def __init__(self):
        self.successful = 0
        self.skipped = 0
        self.errors = 0
        self.failed_urls = []

def is_recent(post_date, days=30):
    """
    Checks if a post's publication date is within the last `days` from today.
    """
    # Create a naive `now` object for a clean comparison
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=days)
    
    return post_date >= thirty_days_ago

def _get_existing_urls(competitor_name):
    """
    Reads the existing CSV file to get a list of all scraped post URLs.
    This prevents re-scraping the same articles.
    """
    existing_urls = set()
    state_filepath = os.path.join('state', competitor_name, f"{competitor_name}_state.csv")

    if os.path.exists(state_filepath):
        try:
            with open(state_filepath, mode='r', newline='', encoding='utf-8') as csvfile:
                # Use DictReader to handle potential header mismatches gracefully
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Check if the 'url' column exists in the row before accessing it
                    if 'url' in row and row['url']:
                        existing_urls.add(row['url'])
            logger.info(f"Found {len(existing_urls)} existing URLs in state file: {state_filepath}.")
        except Exception as e:
            logger.error(f"Could not read state file at {state_filepath}: {e}")
            # Continue with an empty set of URLs if the file is corrupt
    else:
        logger.info("No previous state file found. Scraping all posts.")

    return existing_urls


async def _get_post_details(client, base_url, post_url_path, competitor_name, stats):
    """
    Scrapes an individual blog post page to find the title, URL, publication date,
    content, summary, and SEO keywords.
    """
    cleaned_post_url_path = post_url_path.rstrip(':') 
    full_url = cleaned_post_url_path if cleaned_post_url_path.startswith('http://') or cleaned_post_url_path.startswith('https://') else f"{base_url.rstrip('/')}/{cleaned_post_url_path.lstrip('/')}"
    logger.debug(f"  Scraping details from: {full_url}")
    
    try:
        response = await client.get(full_url, follow_redirects=True) 
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Extract Publication Date ---
        pub_date = None
        date_element_time = soup.find('time')
        if date_element_time and 'datetime' in date_element_time.attrs:
            pub_date_str = date_element_time['datetime']
            date_formats_to_try = ['%Y-%m-%d %H:%M', '%Y-%m-%d']
            for fmt in date_formats_to_try:
                try:
                    pub_date = datetime.strptime(pub_date_str, fmt)
                    break
                except ValueError:
                    continue
        else:
            date_element_p = soup.find('p', string=lambda text: text and "Last updated:" in text)
            if date_element_p:
                date_text = date_element_p.text.replace('Last updated:', '').strip()
                try:
                    pub_date = datetime.strptime(date_text, '%B %d, %Y')
                except ValueError:
                    pub_date = None
            elif soup.find('span', class_='blog-banner__contents-author__date'): 
                date_element_squiz = soup.find('span', class_='blog-banner__contents-author__date')
                pub_date_str = date_element_squiz.text.strip()
                try:
                    pub_date = datetime.strptime(pub_date_str, '%d %b %Y')
                except ValueError:
                    pub_date = None

        # --- Extract Title ---
        title_element = soup.find('h1') or soup.find('h2')
        title = title_element.text.strip() if title_element else 'No Title Found'
        
        # --- Extract Meta Keywords ---
        keywords_meta = soup.find('meta', {'name': 'keywords'})
        seo_meta_keywords = keywords_meta['content'] if keywords_meta and 'content' in keywords_meta.attrs else 'N/A'

        # --- Extract Content ---
        content_container = soup.find('div', class_=['article-content__main', 'post-content', 'blog-post-body', 'item-content', 'no-wysiwyg'])
        content_text = ""
        if content_container:
            # Specific logic to remove the table of contents for the 'squiz' competitor
            if competitor_name == 'squiz':
                # Find the 'Skip ahead:' header
                skip_ahead_header = content_container.find('h3', string=lambda text: text and 'Skip ahead:' in text)
                if skip_ahead_header:
                    # Find the <ul> that immediately follows the header
                    toc_list = skip_ahead_header.find_next_sibling('ul')
                    if toc_list:
                        toc_list.decompose() # This removes the element from the parse tree

            for element in content_container.children:
                # The 'ul' removal logic is now more specific and handled above,
                # so we can remove the generic 'ul' skip.
                # if element.name == 'ul':
                #     continue
                
                if hasattr(element, 'get_text'):
                    content_text += element.get_text(separator=' ', strip=True) + " "

            content_text = re.sub(r'\s+', ' ', content_text)

        return {
            'title': title,
            'url': full_url,
            'publication_date': pub_date.strftime('%Y-%m-%d') if pub_date else 'N/A',
            'content': content_text.strip() if content_text else 'N/A',
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'seo_meta_keywords': seo_meta_keywords
        }

    except httpx.RequestError as e: 
        logger.error(f"Error fetching post details from {full_url}: {e}")
        stats.errors += 1
        stats.failed_urls.append(full_url)
        return None
