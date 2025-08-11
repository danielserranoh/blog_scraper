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

async def scrape(config, days, scrape_all, batch_size, stats):
    """
    Scrapes the TerminalFour blog by iterating through its category URLs
    and handling pagination for each category correctly. Yields posts in batches.
    """
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])

    processed_in_run_urls = set()

    async with httpx.AsyncClient() as client:
        for category_path in config['urls']:
            # --- FIX: Handle the main 'blog/' page as a special case ---
            # This page is just a directory and has no pagination.
            if category_path == "blog/":
                main_blog_url = f"{base_url.rstrip('/')}/{category_path.strip('/')}/"
                logger.info(f"Scanning main blog distributor page: {main_blog_url}")
                try:
                    # We only need to process this single page.
                    response = await client.get(main_blog_url, follow_redirects=True)
                    response.raise_for_status()
                    # We don't need to scrape posts from here, just continue to the actual categories.
                    continue
                except httpx.RequestError as e:
                    logger.error(f"Could not fetch main blog page {main_blog_url}: {e}")
                    continue # Skip to the next URL if the main page fails

            # --- Logic for all other category pages ---
            page_number = 1
            while True:
                # --- FIX: Correct URL construction for pagination ---
                if page_number == 1:
                    # The first page of a category does not have a number in the URL
                    paginated_url = f"{base_url.rstrip('/')}/{category_path.strip('/')}/"
                else:
                    # Pages 2 and onwards have the number
                    paginated_url = f"{base_url.rstrip('/')}/{category_path.strip('/')}/{page_number}/"
                
                logger.info(f"Scanning: {paginated_url}")

                try:
                    response = await client.get(paginated_url, follow_redirects=True)
                    
                    if response.status_code == 404:
                        logger.info(f"  Reached the end of pages for category '{category_path}'.")
                        break
                    
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    post_links = soup.select(config['post_list_selector'])
                    
                    if not post_links:
                        logger.info(f"  No posts found on this page. Moving to next category.")
                        break
                    
                    tasks = []
                    for link_element in post_links:
                        if link_element and link_element.get('href'):
                            post_url_path = link_element['href']
                            full_post_url = f"{base_url.rstrip('/')}/{post_url_path.lstrip('/')}"
                            
                            if full_post_url in existing_urls or full_post_url in processed_in_run_urls:
                                #if full_post_url in existing_urls:
                                stats.skipped += 1
                                logger.debug(f"  Skipping duplicate post: {full_post_url}")
                                continue

                            # If the URL is new for this run, add it to our tracking set and the task list.    
                            processed_in_run_urls.add(full_post_url)
                            tasks.append(_get_post_details(client, base_url, post_url_path, config['name'], stats))

                    if tasks:
                        logger.info(f"  Found {len(tasks)} new posts on this page. Fetching details...")
                        post_details_list = await asyncio.gather(*tasks)
                        for post_details in post_details_list:
                            if post_details:
                                stats.successful += 1
                                #print(f"\r  Progress: {stats.successful} new posts found, {stats.skipped} skipped.", end="", flush=True)
                                posts_to_process.append(post_details)
                                if len(posts_to_process) >= batch_size:
                                    yield posts_to_process
                                    posts_to_process = []

                    # Use the robust pagination check
                    next_page_link = soup.select_one('div.pagination span.currentpage + a')
                    if not next_page_link:
                        logger.info(f"  No 'next page' link found. Reached the end of category '{category_path}'.")
                        break

                    page_number += 1

                except httpx.RequestError as e:
                    logger.error(f"Error fetching {paginated_url}: {e}")
                    break
    
    if posts_to_process:
        yield posts_to_process