# src/extract/blog_patterns/single_list.py
import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .._common import _get_existing_urls, _get_post_details, get_next_page_url, ScrapeStats

logger = logging.getLogger(__name__)

async def scrape(config, days, scrape_all, batch_size, stats):
    """
    Scrapes blogs that have a single, paginated list of posts, with a robust
    check for the end of pagination.
    """
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])
    processed_in_run_urls = set()
    
    pagination_config = config.get('pagination_pattern')
    next_page_selector = config.get('next_page_selector')

    semaphore = asyncio.Semaphore(5)

    async with httpx.AsyncClient() as client:
        current_url = f"{base_url.rstrip('/')}/{config['category_paths'][0].lstrip('/')}"
        page_number = 1
        
        while current_url:
            logger.info(f"Scanning: {current_url}")
            
            try:
                response = await client.get(current_url, follow_redirects=True)
                if response.status_code == 404:
                    logger.info("  Page not found (404). Reached the end of pagination.")
                    break
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                post_links = soup.select(config['post_list_selector'])

                tasks = []
                for link in post_links:
                    if link and link.get('href'):
                        post_url = urljoin(base_url, link['href'])
                        if post_url in existing_urls or post_url in processed_in_run_urls:
                            if post_url not in processed_in_run_urls: stats.skipped += 1
                            continue
                        processed_in_run_urls.add(post_url)

                        async def fetch_with_semaphore(post_link):
                            async with semaphore:
                                return await _get_post_details(client, base_url, post_link['href'], config, stats)

                        tasks.append(fetch_with_semaphore(link))
                
                if tasks:
                    post_details_list = await asyncio.gather(*tasks)
                    for details in post_details_list:
                        if details:
                            stats.successful += 1
                            posts_to_process.append(details)
                            if len(posts_to_process) >= batch_size:
                                yield posts_to_process
                                posts_to_process = []

                # --- Final, Corrected Pagination Logic ---
                
                # 1. First, check our two "brake" conditions based on the current page.
                if not post_links:
                    logger.info("  No posts found on page. Reached the end of pagination.")
                    current_url = None
                elif next_page_selector and not soup.select_one(next_page_selector):
                    logger.info("  'next_page_selector' found no link. Reached the end of pagination.")
                    current_url = None
                else:
                    # 2. Only if we are clear to proceed, get the next URL.
                    current_url = get_next_page_url(pagination_config, soup, current_url, page_number, base_url)
                
                page_number += 1

            except httpx.RequestError as e:
                logger.error(f"Error fetching page {current_url}: {e}")
                stats.errors += 1
                stats.failed_urls.append(current_url)
                break

    if posts_to_process:
        yield posts_to_process