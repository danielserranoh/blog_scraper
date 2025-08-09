# src/transform/live.py
# This file contains the live API processing logic.

from datetime import datetime
import json
import os
import time
import logging
import re
import asyncio
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

async def _get_gemini_details_live(content, model_name, post_title="Unknown Post"):
    """
    Calls the Gemini API asynchronously to get a summary and keywords,
    using the correct client.aio pattern from the migration guide.
    """
    # Initialize the client. The .aio attribute provides the async client.
    client = genai.Client()
    
    summary = "N/A"
    seo_keywords = "N/A"

    if content:
        prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of the 5 most important SEO keywords, ordered by importance. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {content}"
        
        for i in range(1):
            try:
                logger.info(f"    Calling Gemini API for '{post_title}' (attempt {i+1}/3)")
                
                # --- CORRECTED PATTERN based on Migration Guide ---
                # Use the async client (client.aio) to make the async call.
                response = await client.aio.models.generate_content(
                    model_name=f"models/{model_name}",
                    contents=prompt,
                    generation_config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                
                json_str = response.text
                parsed_json = json.loads(json_str)
                print(parsed_json)
                summary = parsed_json.get('summary', 'N/A')
                seo_keywords = ', '.join(parsed_json.get('seo_keywords', []))
                logger.info(f"    Gemini API call successful for '{post_title}'")
                
                return summary, seo_keywords

            except Exception as e:
                logger.error(f"Failed to process API response for '{post_title}': {e}")
                await asyncio.sleep(2**i)
                
    return summary, seo_keywords

# The transform_posts_live function does not need to change.
async def transform_posts_live(posts, model_name):
    start_time = time.time()
    transformed_posts = []
    
    tasks = []
    for post in posts:
        if post['content'] and post['content'] != 'N/A':
            tasks.append(_get_gemini_details_live(post['content'], model_name, post['title']))
        else:
            tasks.append(asyncio.sleep(0, result=('N/A', 'N/A')))

    gemini_results = await asyncio.gather(*tasks)

    for i, post in enumerate(posts):
        summary, seo_keywords = gemini_results[i]
        post['summary'] = summary
        post['seo_keywords'] = seo_keywords
        transformed_posts.append(post)

    end_time = time.time()
    logger.info(f"Batch of {len(posts)} posts transformed in {end_time - start_time:.2f} seconds.")

    posts_with_dates = [p for p in transformed_posts if p['publication_date'] != 'N/A']
    posts_without_dates = [p for p in transformed_posts if p['publication_date'] == 'N/A']

    posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
    
    return posts_with_dates + posts_without_dates