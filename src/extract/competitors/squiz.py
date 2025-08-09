# src/extract/competitors/squiz.py
# Specific extraction logic for Squiz blog.

import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime

# Import common helpers from the parent package
from .._common import is_recent, _get_existing_urls, _get_post_details

logger = logging.getLogger(__name__)

async def extract_from_squiz(config, days, scrape_all, batch_size):
    """
    Scrapes the Squiz blog by scraping its main blog page (which contains all posts).
    Yields posts in batches.
    """
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])

    async with httpx.AsyncClient() as client:
        # Squiz: Main blog page shows all posts, categories are filters.
        # We only need to scrape the single main blog URL once.
        main_blog_url = f"{base_url.rstrip('/')}/{config['urls'][0].lstrip('/')}"
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
                        logger.debug(f"  Skipping existing post: {full_post_url}")
                        continue

                    # Pass the full_post_url directly as post_url, and base_url as empty string
                    tasks.append(_get_post_details(client, "", full_post_url, config['name'])) 
            if tasks:
                logger.info(f"  Found {len(tasks)} new posts on this page. Fetching details...")

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
