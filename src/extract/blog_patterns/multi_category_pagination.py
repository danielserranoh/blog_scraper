# src/extract/blog_patterns/multi_category_pagination.py
# Scrapes blogs that use a multi-category structure with linked pagination.

import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from .._common import _get_existing_urls, _get_post_details

logger = logging.getLogger(__name__)

async def scrape(config, days, scrape_all, batch_size, stats):
    """
    Scrapes blogs by looping through category URLs and following a 'next page'
    link for pagination, defined by a CSS selector in the config.
    """
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])
    processed_in_run_urls = set()
    
    # Get the specific selector for the next page link from the config
    next_page_selector = config.get("next_page_selector")

    async with httpx.AsyncClient() as client:
        for category_path in config['category_paths']:
            next_page_url = f"{base_url.rstrip('/')}/{category_url_path.lstrip('/')}"
            
            while next_page_url:
                logger.info(f"Scanning: {next_page_url}")

                try:
                    response = await client.get(next_page_url, follow_redirects=True)
                    if response.status_code == 404:
                        logger.warning(f"  Page not found (404): {next_page_url}. Moving on.")
                        break
                    response.raise_for_status()
                    
                    # This logic handles both regular HTML and AJAX JSON responses
                    if "application/json" in response.headers.get("content-type", ""):
                        json_response = response.json()
                        html_content = ""
                        for item in json_response:
                            if item.get("command") == "insert":
                                html_content += item.get("data", "")
                        soup = BeautifulSoup(html_content, 'html.parser')
                    else:
                        soup = BeautifulSoup(response.text, 'html.parser')

                    post_links = soup.select(config['post_list_selector'])
                    if not post_links:
                        logger.info(f"  No posts found on this page. Moving on.")
                        break
                    
                    tasks = []
                    for link in post_links:
                        if link and link.get('href'):
                            post_url = urljoin(base_url, link['href'])
                            if post_url in existing_urls or post_url in processed_in_run_urls:
                                if post_url not in processed_in_run_urls: stats.skipped += 1
                                logger.debug(f"  Skipping duplicate post: {post_url}")
                                continue
                            
                            processed_in_run_urls.add(post_url)
                            tasks.append(_get_post_details(client, base_url, link['href'], config['name'], stats))

                    if tasks:
                        logger.info(f"  Found {len(tasks)} new posts on this page. Fetching details...")
                        post_details_list = await asyncio.gather(*tasks)
                        for details in post_details_list:
                            if details:
                                stats.successful += 1
                                posts_to_process.append(details)
                                if len(posts_to_process) >= batch_size:
                                    yield posts_to_process
                                    posts_to_process = []
                    
                    # Use the configured selector to find the next page link
                    if next_page_selector:
                        next_link_element = soup.select_one(next_page_selector)
                        if next_link_element and next_link_element.get('href'):
                            next_page_url = urljoin(base_url, next_link_element['href'])
                        else:
                            next_page_url = None # End of pagination
                    else:
                        # If no selector is provided, assume no pagination for this category
                        next_page_url = None

                except httpx.RequestError as e:
                    logger.error(f"Error fetching {next_page_url}: {e}")
                    stats.errors += 1
                    stats.failed_urls.append(next_page_url)
                    break
    
    if posts_to_process:
        yield posts_to_process