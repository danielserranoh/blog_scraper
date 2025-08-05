# src/extract/competitors/moderncampus.py
# Specific extraction logic for Modern Campus blog.

import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime

# Import common helpers from the parent package
from .._common import is_recent, _get_existing_urls, _get_post_details

logger = logging.getLogger(__name__)

async def extract_from_modern_campus(config, days, scrape_all, batch_size):
    """
    Scrapes the Modern Campus blog by first getting a list of post URLs
    and then visiting each one to get full details.
    Yields posts in batches.
    """
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])

    async with httpx.AsyncClient() as client:
        for category_path in config['urls']: # Modern Campus currently has one main blog URL
            page_number = 1
            while True:
                current_url = f"{base_url.rstrip('/')}/{category_path.lstrip('/')}".split('index.html')[0] + f"index.html?page={page_number}"
                logger.info(f"Scanning page: {current_url}")
                
                try:
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
                        if post_details:
                            if post_details['publication_date'] != 'N/A':
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
