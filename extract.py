# extract.py
# This file contains the data extraction logic for the blog scraper.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import json
import time
import os
import csv
import logging
import httpx # Import httpx for async HTTP requests
import asyncio # Import asyncio for concurrent operations

logger = logging.getLogger(__name__)

def is_recent(post_date, days=30):
    """
    Checks if a post's publication date is within the last `days` from today.
    
    This version uses timezone-naive datetime objects for a simpler comparison,
    avoiding the need for external libraries like pytz.
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
    output_folder = os.path.join('scraped', competitor_name)

    # Find the most recent CSV file for the competitor
    latest_file = None
    if os.path.isdir(output_folder):
        files = os.listdir(output_folder)
        csv_files = [f for f in files if f.endswith('.csv') and f.startswith(competitor_name)]
        if csv_files:
            # Sort files by name to find the most recent one (due to YYMMDD format)
            csv_files.sort(reverse=True)
            latest_file = os.path.join(output_folder, csv_files[0])
    
    if latest_file and os.path.exists(latest_file):
        with open(latest_file, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_urls.add(row['url'])
        logger.info(f"Found {len(existing_urls)} existing URLs in {latest_file}.")
    else:
        logger.info("No previous CSV file found. Scraping all posts.")

    return existing_urls


async def _get_post_details(client, base_url, post_url_path): 
    """
    Scrapes an individual blog post page to find the title, URL, publication date,
    content, summary, and SEO keywords.
    """
    # Clean the post_url_path by stripping any trailing colons or other problematic characters
    # This addresses the issue of URLs like '...firms:' causing errors.
    cleaned_post_url_path = post_url_path.rstrip(':') 

    # Determine the full URL: if cleaned_post_url_path is already absolute, use it directly.
    # Otherwise, combine with base_url.
    full_url = cleaned_post_url_path if cleaned_post_url_path.startswith('http://') or cleaned_post_url_path.startswith('https://') else f"{base_url.rstrip('/')}/{cleaned_post_url_path.lstrip('/')}"
    logger.info(f"  Scraping details from: {full_url}")
    
    try:
        # Explicitly tell httpx to follow redirects for robustness
        response = await client.get(full_url, follow_redirects=True) 
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Extract Publication Date ---
        pub_date = None
        # Check for TerminalFour's <time> tag first
        date_element_time = soup.find('time')
        if date_element_time and 'datetime' in date_element_time.attrs:
            pub_date_str = date_element_time['datetime']
            # Try parsing with multiple formats for robustness
            date_formats_to_try = ['%Y-%m-%d %H:%M', '%Y-%m-%d']
            for fmt in date_formats_to_try:
                try:
                    pub_date = datetime.strptime(pub_date_str, fmt)
                    break
                except ValueError:
                    continue
        else:
            # Check for Modern Campus's "<p>Last updated: Month DD, YYYY</p>"
            date_element_p = soup.find('p', string=lambda text: text and "Last updated:" in text)
            if date_element_p:
                date_text = date_element_p.text.replace('Last updated:', '').strip()
                try:
                    pub_date = datetime.strptime(date_text, '%B %d, %Y')
                except ValueError:
                    pub_date = None
            # Check for Squiz's <span> tag with class 'blog-banner__contents-author__date'
            elif soup.find('span', class_='blog-banner__contents-author__date'): 
                date_element_squiz = soup.find('span', class_='blog-banner__contents-author__date')
                pub_date_str = date_element_squiz.text.strip()
                try:
                    pub_date = datetime.strptime(pub_date_str, '%d %b %Y') # Example: 23 Jul 2025
                except ValueError:
                    pub_date = None


        # --- Extract Title ---
        title_element = soup.find('h1') or soup.find('h2')
        title = title_element.text.strip() if title_element else 'No Title Found'
        
        # --- Extract Meta Keywords ---
        # Find the meta tag for keywords and extract its content
        keywords_meta = soup.find('meta', {'name': 'keywords'})
        seo_meta_keywords = keywords_meta['content'] if keywords_meta and 'content' in keywords_meta.attrs else 'N/A'

        # --- Extract Content ---
        content_container = soup.find('div', class_=['article-content__main', 'post-content', 'blog-post-body', 'item-content'])
        content_text = ""
        if content_container:
            # More robust way to get and clean up text
            raw_text = content_container.get_text(separator=' ', strip=True)
            content_text = re.sub(r'\s+', ' ', raw_text)
        
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
        return None

async def extract_posts_in_batches(config, days=30, scrape_all=False, batch_size=10):
    """
    A router function that scrapes posts in batches and yields them.
    """
    posts_to_process = []
    base_url = config['base_url']
    competitor_name = config['name']
    existing_urls = _get_existing_urls(competitor_name)

    async with httpx.AsyncClient() as client: # Use an async client for the session
        if competitor_name == 'terminalfour':
            for category_path in config['urls']:
                category_url = f"{base_url.rstrip('/')}/{category_path.lstrip('/')}"
                
                logger.info(f"Scanning category: {category_url}")
                try:
                    # Explicitly follow redirects for the category page
                    response = await client.get(category_url, follow_redirects=True) 
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    post_links = soup.select(config['post_list_selector'])
                    
                    if not post_links:
                        continue
                    
                    found_older_post = False
                    tasks = []
                    for link_element in post_links:
                        if link_element and link_element.get('href'):
                            post_url_path = link_element['href']
                            full_post_url = f"{base_url.rstrip('/')}/{post_url_path.lstrip('/')}"
                            
                            if full_post_url in existing_urls:
                                logger.info(f"  Skipping existing post: {full_post_url}")
                                continue
                            
                            tasks.append(_get_post_details(client, base_url, post_url_path)) 

                    post_details_list = await asyncio.gather(*tasks)

                    for post_details in post_details_list:
                        if post_details:
                            if post_details['publication_date'] != 'N/A':
                                pub_date = datetime.strptime(post_details['publication_date'], '%Y-%m-%d')
                                if scrape_all or is_recent(pub_date, days):
                                    posts_to_process.append(post_details)
                                    existing_urls.add(post_details['url']) 
                                    if len(posts_to_process) >= batch_size:
                                        yield posts_to_process
                                        posts_to_process = []
                                else:
                                    found_older_post = True
                                    if not scrape_all:
                                        break
                            else:
                                posts_to_process.append(post_details)
                                existing_urls.add(post_details['url']) # Add to existing_urls even if date is N/A
                                if len(posts_to_process) >= batch_size:
                                    yield posts_to_process
                                    posts_to_process = []
                    
                    if found_older_post and not scrape_all:
                        break
                except httpx.RequestError as e:
                    logger.error(f"Error fetching {category_url}: {e}")
                    continue

        elif competitor_name == 'modern campus':
            for category_path in config['urls']: 
                page_number = 1
                while True:
                    current_url = f"{base_url.rstrip('/')}/{category_path.lstrip('/')}".split('index.html')[0] + f"index.html?page={page_number}"
                    logger.info(f"Scanning page: {current_url}")
                    
                    try:
                        # Explicitly follow redirects for the category page
                        response = await client.get(current_url, follow_redirects=True) 
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, 'html.parser')
                        post_link_elements = soup.select(config['post_list_selector'])

                        if not post_link_elements:
                            break
                        
                        tasks = []
                        for link_element in post_link_elements:
                            post_url_path = link_element['href']
                            full_post_url = f"{base_url.rstrip('/')}/{post_url_path.lstrip('/')}"
                            
                            if full_post_url in existing_urls:
                                logger.info(f"  Skipping existing post: {full_post_url}")
                                continue
                            
                            tasks.append(_get_post_details(client, base_url, post_url_path)) 
                        
                        post_details_list = await asyncio.gather(*tasks)

                        for post_details in post_details_list:
                            if post_details and post_details['publication_date'] != 'N/A':
                                pub_date = datetime.strptime(post_details['publication_date'], '%Y-%m-%d')
                                if scrape_all or is_recent(pub_date, days):
                                    posts_to_process.append(post_details)
                                    existing_urls.add(post_details['url'])
                                    if len(posts_to_process) >= batch_size:
                                        yield posts_to_process
                                        posts_to_process = []
                            
                            if not scrape_all:
                                last_post_date_str = posts_to_process[-1]['publication_date'] if posts_to_process else None
                                if last_post_date_str:
                                    last_post_date = datetime.strptime(last_post_date_str, '%Y-%m-%d')
                                    if not is_recent(last_post_date, days):
                                        break
                            
                            page_number += 1

                    except httpx.RequestError as e:
                        logger.error(f"Error fetching {current_url}: {e}")
                        break
            
            if posts_to_process: 
                yield posts_to_process
                posts_to_process = []

        elif competitor_name == 'squiz':
            # Squiz: Main blog page shows all posts, categories are filters.
            # We only need to scrape the single main blog URL once.
            main_blog_url = f"{base_url.rstrip('/')}/{config['urls'][0].lstrip('/')}"
            print(f"🟢 Scanning main blog page for Squiz: {main_blog_url}")
            logger.info(f"Scanning main blog page for Squiz: {main_blog_url}")
            try:
                # Explicitly follow redirects for the main blog page
                response = await client.get(main_blog_url, follow_redirects=True)
                
                # Check for successful status code (2xx) after redirects
                if not response.is_success:
                    logger.error(f"Failed to fetch Squiz main blog page {main_blog_url}. Final status: {response.status_code}")
                    return # Exit this competitor's scraping if main page cannot be fetched

                soup = BeautifulSoup(response.text, 'html.parser')
                post_links = soup.select(config['post_list_selector'])

                if not post_links:
                    logger.info(f"No posts found on Squiz main blog page: {main_blog_url}")
                    return 

                tasks = []
                for link_element in post_links:
                    if link_element and link_element.get('href'):
                        post_url_path = link_element['href']
                        # Squiz links are already absolute, so no need to prepend base_url
                        full_post_url = post_url_path 

                        if full_post_url in existing_urls:
                            logger.info(f"  Skipping existing post: {full_post_url}")
                            continue

                        # Pass the full_post_url directly as post_url, and base_url as empty string
                        tasks.append(_get_post_details(client, "", full_post_url)) 
                
                post_details_list = await asyncio.gather(*tasks)

                for post_details in post_details_list:
                    if post_details:
                        if post_details['publication_date'] != 'N/A':
                            pub_date = datetime.strptime(post_details['publication_date'], '%Y-%m-%d')
                            if scrape_all or is_recent(pub_date, days):
                                posts_to_process.append(post_details)
                                existing_urls.add(post_details['url'])
                                if len(posts_to_process) >= batch_size:
                                    yield posts_to_process
                                    posts_to_process = []
                        else:
                            posts_to_process.append(post_details)
                            existing_urls.add(post_details['url'])
                            if len(posts_to_process) >= batch_size:
                                yield posts_to_process
                                posts_to_process = []
                
            except httpx.RequestError as e:
                logger.error(f"Error fetching Squiz main blog page {main_blog_url}: {e}")
                
    if posts_to_process:
        yield posts_to_process

def extract_posts(config, days=30, scrape_all=False):
    """
    A router function that selects the correct scraping logic based on the competitor's name.
    This function is now deprecated in favor of the batched version.
    """
    pass
