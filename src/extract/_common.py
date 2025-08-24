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
    
    raw_data_folder = os.path.join('data', 'raw', competitor_name)

    if os.path.exists(raw_data_folder):
        try:
            for filename in os.listdir(raw_data_folder):
                if filename.endswith('.csv'):
                    filepath = os.path.join(raw_data_folder, filename)
                    with open(filepath, mode='r', newline='', encoding='utf-8-sig') as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            if 'url' in row and row['url']:
                                existing_urls.add(row['url'])
            logger.info(f"Found {len(existing_urls)} existing URLs in raw data files for '{competitor_name}'.")
        except Exception as e:
            logger.error(f"❌ Could not read raw data files in {raw_data_folder}: {e}")
    else:
        logger.info("🗂️ No previous raw data files found. Starting a fresh scrape.")

    return existing_urls

def _validate_post_url(response, original_url, config, stats):
    """
    Checks if a post URL was redirected to a main category page.
    Returns True if the URL is valid, False if it was redirected.
    """
    final_url = str(response.url)
    base_url = config.get('base_url', '')
    
    # Create a set of the full, absolute URLs for the main category pages
    main_category_urls = {urljoin(base_url, path) for path in config.get('category_paths', [])}

    # Check if the final URL is one of these main pages
    if final_url != original_url and final_url in main_category_urls:
        logger.warning(f"  URL {original_url} redirected to a main category page. Skipping.")
        stats.skipped += 1
        return False
    return True

def _extract_post_publication_date(soup, config, url):
    """
    Extracts and parses the publication date from a post's page.
    """
    date_selector = config.get('date_selector')
    date_prefix_to_strip = config.get('date_strip_prefix')
    
    if not date_selector:
        return None

    date_element = soup.select_one(date_selector)
    if not date_element:
        return None

    date_text = date_element.get('datetime') or date_element.get_text()
    
    if date_prefix_to_strip and date_text.startswith(date_prefix_to_strip):
        date_text = date_text.replace(date_prefix_to_strip, "").strip()

    try:
        return dateparse(date_text)
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date: '{date_text}' from {url}")
        return None
    
def _extract_post_title(soup, config):
    """
    Extracts the title from a post's page using a specific selector
    from the config, with a fallback to generic selectors.
    """
    title_selector = config.get('title_selector')
    
    if title_selector:
        title_element = soup.select_one(title_selector)
        if title_element:
            return title_element.text.strip()
    
    # Fallback for when no specific selector is provided
    title_element = soup.find('h1') or soup.find('h2')
    if title_element:
        return title_element.text.strip()
        
    return 'No Title Found'
    
def _extract_post_content(soup, config):
    """
    Extracts and cleans the main blog post content from a page.
    """
    content_selector = config.get('content_selector')
    content_filter_selector = config.get('content_filter_selector')
    
    if not content_selector:
        return ""

    content_container = soup.select_one(content_selector)
    if not content_container:
        return ""

    if content_filter_selector:
        element_to_remove = content_container.select_one(content_filter_selector)
        if element_to_remove:
            element_to_remove.decompose()
            
    #return ' '.join(content_container.get_text(separator=' ', strip=True).split())
    return content_container

def _extract_headings(soup):
    """
    Extracts all headings (h1, h2, h3) from the BeautifulSoup object.
    """
    headings = []
    heading_tags = soup.find_all(['h1', 'h2', 'h3'])
    for tag in heading_tags:
        headings.append({
            'tag': tag.name,
            'text': tag.get_text(strip=True)
        })
    return headings

def _extract_json_ld(soup):
    """
    Scrapes an blog post article for all JSON-LD schemas.

    Args:
        soup: The content of the webpage to scrape in a bs4 object.

    Returns:
        list: A list of dictionaries, where each dictionary is a JSON-LD schema found on the page.
    """

    # 1. Find all script tags with the specific JSON-LD type
    schemas = []
    script_tags = soup.find_all('script', type='application/ld+json')
    
    # 2. Extract and parse the JSON data from each tag
    for tag in script_tags:
        try:
            # The data is inside the tag as text content
            data = json.loads(tag.string)
            schemas.append(data)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from a script tag: {e}")
        except AttributeError:
            # This handles cases where a script tag might not have .string content
            continue

    return schemas



async def _get_post_details(client, base_url, post_url_path, config, stats): 
    """
    Scrapes an individual blog post page using selectors from the config.
    """
    full_url = post_url_path if post_url_path.startswith(('http://', 'https://')) else f"{base_url.rstrip('/')}/{post_url_path.lstrip('/')}"
    logger.debug(f"  Scraping details from: {full_url}")
    
    try:
        response = await client.get(full_url, follow_redirects=True) 
        response.raise_for_status()

        if not _validate_post_url(response, full_url, config, stats):
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        pub_date = _extract_post_publication_date(soup, config, full_url)
        title = _extract_post_title(soup, config)
        content_soup = _extract_post_content(soup, config)
        content_text = ' '.join(content_soup.get_text(separator=' ', strip=True).split())
        #content_text = _extract_post_content(soup, config)
        keywords_meta = soup.find('meta', {'name': 'keywords'})
        seo_meta_keywords = keywords_meta.get('content', 'N/A') if keywords_meta else 'N/A'       
        headings_list = _extract_headings(content_soup)
        schemas_list = _extract_json_ld(soup)

        return {
            'title': title,
            'url': full_url,
            'publication_date': pub_date.strftime('%Y-%m-%d') if pub_date else 'N/A',
            'content': content_text if content_text else 'N/A',
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'seo_meta_keywords': seo_meta_keywords,
            'headings': headings_list,
            'schemas': schemas_list
        }

    except httpx.RequestError as e: 
        logger.error(f"Error fetching post details from {full_url} : {e}")
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