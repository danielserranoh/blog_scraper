# src/extract/competitors/terminalfour.py
# Specific extraction logic for TerminalFour blog.

import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime

# Import common helpers from the parent package
from .._common import is_recent, _get_existing_urls, _get_post_details

logger = logging.getLogger(__name__)

async def extract_from_terminalfour(config, days, scrape_all, batch_size):
    """
    Scrapes the TerminalFour blog by iterating through its category URLs.
    Yields posts in batches.
    """
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])

    async with httpx.AsyncClient() as client:
        for category_path in config['urls']:
            category_url = f"{base_url.rstrip('/')}/{category_path.lstrip('/')}"
            
            logger.info(f"Scanning category: {category_url}")
            try:
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
                            logger.debug(f"  Skipping existing post: {full_post_url}")
                            continue
                        
                        tasks.append(_get_post_details(client, base_url, post_url_path, config['name'])) 
                
                if tasks:
                    logger.info(f"  Found {len(tasks)} new posts in this category. Fetching details...")


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
                            existing_urls.add(post_details['url'])
                            if len(posts_to_process) >= batch_size:
                                yield posts_to_process
                                posts_to_process = []
                
                if found_older_post and not scrape_all:
                    break
            except httpx.RequestError as e:
                logger.error(f"Error fetching {category_url}: {e}")
                continue
    
    if posts_to_process:
        yield posts_to_process
