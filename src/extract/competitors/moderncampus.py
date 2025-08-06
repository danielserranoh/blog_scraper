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
                        break # No more posts on this page, so break the pagination loop
                    
                    tasks = []
                    found_new_posts_on_page = False # Flag to track if any new posts are found on this specific page
                    for link_element in post_link_elements:
                        post_url_path = link_element['href']
                        full_post_url = f"{base_url.rstrip('/')}/{post_url_path.lstrip('/')}"
                        
                        if full_post_url in existing_urls:
                            logger.info(f"  Skipping existing post: {full_post_url}")
                            continue
                        
                        tasks.append(_get_post_details(client, base_url, post_url_path)) 
                        found_new_posts_on_page = True # A new post was found on this page
                    
                    # Only gather tasks if there are any new posts to process
                    if tasks:
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
                                    
                                    # For --all, we continue as long as new posts are found.
                                    # For date-limited, we break if an old post is found.
                                    if not scrape_all and not is_recent(pub_date, days):
                                        break # Found an old post, stop for this category/page
                                else:
                                    # If no date is found, we still process it but don't add to existing_urls
                                    # if it's not recent and we're not scraping all.
                                    posts_to_process.append(post_details)
                                    existing_urls.add(post_details['url'])
                                    if len(posts_to_process) >= batch_size:
                                        yield posts_to_process
                                        posts_to_process = []
                    
                    # If scraping all and no new posts were found on this specific page, break the loop
                    if scrape_all and not found_new_posts_on_page:
                        logger.info(f"  No new posts found on {current_url}. Stopping pagination for --all.")
                        break

                    # If not scraping all, and we broke due to an old post, then break the outer loop too
                    if not scrape_all and any(p['publication_date'] != 'N/A' and not is_recent(datetime.strptime(p['publication_date'], '%Y-%m-%d'), days) for p in posts_to_process if p):
                        break
                    
                    page_number += 1

                except httpx.RequestError as e:
                    logger.error(f"Error fetching {current_url}: {e}")
                    break
            
            if posts_to_process: # Yield any remaining posts if a category finishes or an error occurs
                yield posts_to_process
                posts_to_process = []
