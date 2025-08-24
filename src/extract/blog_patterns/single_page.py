# src/extract/blog_patterns/single_page.py
import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .._common import _get_existing_urls, _get_post_details, get_next_page_url, ScrapeStats
import random
logger = logging.getLogger(__name__)

async def scrape(config, days, scrape_all, batch_size, stats, existing_urls):
    """Scrapes blogs that contain all posts on a single page."""
    posts_to_process = []
    base_url = config['base_url']
    processed_in_run_urls = set()

     # --- ADD THIS: Create a semaphore to limit concurrency ---
    semaphore = asyncio.Semaphore(5) # Allow up to 5 concurrent detail scrapes

    async with httpx.AsyncClient() as client:
        # For this pattern, we only ever process the first path in the list
        scan_url = f"{base_url.rstrip('/')}/{config['category_paths'][0].lstrip('/')}"
        logger.info(f"Scanning single page: {scan_url}")
        try:
            response = await client.get(scan_url, follow_redirects=True)
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

                    async def fetch_with_semaphore(post_link_href):
                        async with semaphore:
                            await asyncio.sleep(random.uniform(0.5, 1.5))
                            return await _get_post_details(client, base_url, post_link_href, config, stats)
                    
                    tasks.append(fetch_with_semaphore(link['href']))
            
            if tasks:
                post_details_list = await asyncio.gather(*tasks)
                for details in post_details_list:
                    if details:
                        stats.successful += 1
                        posts_to_process.append(details)
                        if len(posts_to_process) >= batch_size:
                            yield posts_to_process
                            posts_to_process = []

        except httpx.RequestError as e:
            logger.error(f"Error fetching page {scan_url}: {e}")
            stats.errors += 1
            stats.failed_urls.append(scan_url)

    if posts_to_process:
        yield posts_to_process