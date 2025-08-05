# transform.py
# This file contains the data transformation logic.

from datetime import datetime
import json
import requests
import os
import time
import logging
import re
import httpx
import asyncio

logger = logging.getLogger(__name__)

async def _get_gemini_details(client, content, post_title="Unknown Post"): # Changed to async def
    """
    Calls the Gemini API to get a summary and a list of 5 keywords,
    ordered by importance.
    """
    summary = "N/A"
    seo_keywords = "N/A"
    
    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        logger.warning("GEMINI_API_KEY not set in environment variables. Skipping API call.")
        return summary, seo_keywords

    if content:
        prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of the 5 most important SEO keywords, ordered by importance. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {content}"

        chatHistory = []
        chatHistory.append({ "role": "user", "parts": [{ "text": prompt }] })
        
        payload = {
            "contents": chatHistory,
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "OBJECT",
                    "properties": {
                        "summary": { "type": "STRING" },
                        "seo_keywords": { 
                            "type": "ARRAY", 
                            "items": { "type": "STRING" }
                        }
                    }
                }
            }
        }

        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={gemini_api_key}"
        
        for i in range(3): # We still try a few times, but with smarter delays
            try:
                logger.info(f"    Calling Gemini API for '{post_title}' (attempt {i+1}/3)")
                response_gen = await client.post(
                    api_url,
                    headers={ 'Content-Type': 'application/json' },
                    json=payload,
                    timeout=30.0
                )
                response_gen.raise_for_status() # This will raise an exception for 4xx/5xx responses
                
                json_match = re.search(r'\{.*\}', response_gen.text, re.DOTALL)
                
                if json_match:
                    json_str = json_match.group(0)
                    parsed_json = json.loads(json_str)
                    
                    summary = parsed_json.get('summary', 'N/A')
                    seo_keywords = ', '.join(parsed_json.get('seo_keywords', []))
                    logger.info(f"    Gemini API call successful for '{post_title}'")
                    break
                else:
                    logger.error(f"Could not find a valid JSON object in the API response for '{post_title}'.")
                    raise ValueError("Invalid API response format")

            except httpx.HTTPStatusError as e: # Catch HTTP errors specifically
                if e.response.status_code == 429: # Too Many Requests
                    retry_after = 60 # Default retry delay in seconds
                    try:
                        # Attempt to parse retryDelay from API response details
                        error_details = e.response.json()
                        for detail in error_details.get('error', {}).get('details', []):
                            if detail.get('@type') == 'type.googleapis.com/google.rpc.RetryInfo':
                                # retryDelay is typically in "Xs" format
                                delay_str = detail.get('retryDelay', '0s')
                                retry_after = int(delay_str.rstrip('s'))
                                break
                    except Exception as parse_error:
                        logger.warning(f"Could not parse retryDelay from 429 error details: {parse_error}")
                    
                    logger.warning(f"API quota exceeded for '{post_title}'. Retrying after {retry_after} seconds. Error: {e}")
                    time.sleep(retry_after)
                else:
                    logger.error(f"HTTP error for '{post_title}', not retrying. Status: {e.response.status_code}. Error: {e}")
                    break # Don't retry for other HTTP errors
            except httpx.RequestError as e: # Catch other request errors (e.g., network issues)
                logger.warning(f"API call failed for '{post_title}', retrying in {2**i} seconds... Error: {e}")
                time.sleep(2**i)
            except Exception as e: # Catch other unexpected errors
                logger.error(f"Failed to process API response for '{post_title}': {e}")
                logger.error(f"Raw API response content: {response_gen.text if 'response_gen' in locals() else 'No response'}")
                break
    
    return summary, seo_keywords

async def transform_posts(posts):
    """
    Transforms a batch of extracted post data by enriching it with Gemini API calls.
    
    Args:
        posts (list): A list of dictionaries with raw post data.

    Returns:
        list: The transformed list of dictionaries with enriched data.
    """
    start_time = time.time()
    transformed_posts = []
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for post in posts:
            if post['content'] and post['content'] != 'N/A':
                tasks.append(_get_gemini_details(client, post['content'], post['title']))
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
