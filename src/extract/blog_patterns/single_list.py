# src/extract/blog_patterns/single_list.py
import logging
import httpx
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .._common import _get_existing_urls, _get_post_details, get_next_page_url, ScrapeStats

logger = logging.getLogger(__name__)

async def scrape(config, days, scrape_all, batch_size, stats):
    """Scrapes blogs that have a single, paginated list of posts."""
    posts_to_process = []
    base_url = config['base_url']
    existing_urls = _get_existing_urls(config['name'])
    processed_in_run_urls = set()
    
    pagination_config = config.get('pagination_pattern')
    
    async with httpx.AsyncClient() as client:
        # Start with the first (and only) path for this pattern
        next_page_url = f"{base_url.rstrip('/')}/{config['category_paths'][0].lstrip('/')}"
        page_number = 1
        
        while next_page_url:
            logger.info(f"Scanning: {next_page_url}")
            found_posts_on_page = False
            try:
                response = await client.get(next_page_url, follow_redirects=True)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                post_links = soup.select(config['post_list_selector'])

                if post_links:
                    found_posts_on_page = True
                
                tasks = []
                for link in post_links:
                    if link and link.get('href'):
                        post_url = urljoin(base_url, link['href'])
                        if post_url in existing_urls or post_url in processed_in_run_urls:
                            if post_url not in processed_in_run_urls: stats.skipped += 1
                            continue
                        processed_in_run_urls.add(post_url)
                        tasks.append(_get_post_details(client, base_url, link['href'], config, stats))
                
                if tasks:
                    post_details_list = await asyncio.gather(*tasks)
                    for details in post_details_list:
                        if details:
                            stats.successful += 1
                            posts_to_process.append(details)
                            if len(posts_to_process) >= batch_size:
                                yield posts_to_process
                                posts_to_process = []
                
                # Use our smart pagination handler to find the next URL
                # For numeric query, we stop if we didn't find any posts on the page
                if pagination_config.get('type') == 'numeric_query' and not found_posts_on_page:
                    next_page_url = None
                else:
                    next_page_url = get_next_page_url(pagination_config, soup, next_page_url, page_number, base_url)
                
                page_number += 1

            except httpx.RequestError as e:
                logger.error(f"Error fetching page {next_page_url}: {e}")
                stats.errors += 1
                stats.failed_urls.append(next_page_url)
                break

    if posts_to_process:
        yield posts_to_process