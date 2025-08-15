# src/transform/live.py
# This file contains the live API processing logic.

import time
import logging
import asyncio
from datetime import datetime
from src.api_connector import GeminiAPIConnector # <-- Import our new connector

logger = logging.getLogger(__name__)

async def transform_posts_live(posts, model_name):
    """
    Transforms a batch of extracted post data by enriching it with live,
    asynchronous calls to the Gemini API via the connector.
    """
    start_time = time.time()
    connector = GeminiAPIConnector() # Instantiate the connector
    
    tasks = []
    for post in posts:
        if post['content'] and post['content'] != 'N/A':
            tasks.append(connector.enrich_post_live(post['content'], model_name, post['title']))
        else:
            # Create a completed future for posts with no content
            future = asyncio.Future()
            future.set_result(('N/A', 'N/A'))
            tasks.append(future)

    gemini_results = await asyncio.gather(*tasks)

    transformed_posts = []
    for i, post in enumerate(posts):
        summary, seo_keywords = gemini_results[i]
        post['summary'] = summary
        post['seo_keywords'] = seo_keywords
        transformed_posts.append(post)

    logger.info(f"Live enrichment of {len(posts)} posts completed in {time.time() - start_time:.2f} seconds.")

    # Sort the final list
    posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
    posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']
    posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
    
    return posts_with_dates + posts_without_dates