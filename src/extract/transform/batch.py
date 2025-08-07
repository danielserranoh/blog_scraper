# src/transform/batch.py
# This file contains the batch API processing logic.

from datetime import datetime
import json
import os
import time
import logging
import re
import httpx
import asyncio

logger = logging.getLogger(__name__)

def _create_jsonl_from_posts(posts):
    """
    Creates a JSONL string from a list of posts for the Gemini Batch API.
    
    This function serializes a list of post dictionaries into a
    JSON Lines format, which is required by the API.
    """
    jsonl_lines = []
    for i, post in enumerate(posts):
        if post['content'] and post['content'] != 'N/A':
            prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of the 5 most important SEO keywords, ordered by importance. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {post['content']}"
            request_payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json"}
            }
            # Add original post data as metadata to link it back later
            metadata = {
                "url": post.get("url"),
                "title": post.get("title"),
                "publication_date": post.get("publication_date"),
                "seo_meta_keywords": post.get("seo_meta_keywords")
            }
            # Create a JSON line with a unique key
            try:
                json_line = {"key": f"post-{i}", "request": request_payload, "metadata": metadata}
                jsonl_lines.append(json.dumps(json_line))
            except TypeError as e:
                logger.error(f"Failed to serialize post {i} to JSON: {e}")
                logger.error(f"Post data that caused the error: {post}")
    return "\n".join(jsonl_lines)

async def create_gemini_batch_job(posts, competitor_name):
    """
    Submits a list of posts to the Gemini Batch API.
    
    This function handles the multi-step process of uploading the file
    and creating the batch job. It includes retry logic for robustness.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        logger.error("GEMINI_API_KEY not set. Cannot create batch job.")
        return None

    jsonl_data = _create_jsonl_from_posts(posts)
    if not jsonl_data:
        logger.warning("No posts with content to submit to Gemini API. Skipping batch job creation.")
        return None

    batch_file_path = os.path.join("scraped", competitor_name, "temp_batch_requests.jsonl")
    os.makedirs(os.path.dirname(batch_file_path), exist_ok=True)
    with open(batch_file_path, "w") as f:
        f.write(jsonl_data)
    
    logger.info(f"Generated {len(posts)} Gemini Batch API requests in '{batch_file_path}'.")

    async with httpx.AsyncClient() as client:
        for i in range(3):
            try:
                upload_url = f"https://generativelanguage.googleapis.com/v1beta/files?key={gemini_api_key}"
                
                with open(batch_file_path, "rb") as f:
                    upload_response = await client.post(
                        upload_url,
                        content=f.read(),
                        headers={
                            'x-goog-upload-content-type': 'application/jsonl',
                            'x-goog-upload-protocol': 'resumable'
                        },
                        timeout=300.0
                    )
                
                upload_response.raise_for_status()
                file_details = upload_response.json()
                file_id = file_details.get("name")
                
                if not file_id:
                    logger.error(f"Failed to upload batch file: API returned no file ID (attempt {i+1}/3).")
                    time.sleep(2**i)
                    continue

                logger.success(f"Successfully uploaded batch file with ID: {file_id}")
                
                batch_url = f"https://generativelanguage.googleapis.com/v1beta/batches:create?key={gemini_api_key}"
                batch_payload = {
                    "input_file": { "uri": f"gs://genai-batch-processing/{file_id}" },
                    "model": "models/gemini-2.5-flash-lite-05-20", # Using the lite version for batch
                    "output_file": {"uri": f"gs://genai-batch-processing/{competitor_name}-results.jsonl"}
                }
                batch_response = await client.post(batch_url, json=batch_payload, timeout=300.0)
                batch_response.raise_for_status()
                batch_details = batch_response.json()
                job_id = batch_details.get("name")
                
                os.remove(batch_file_path)
                
                return job_id

            except httpx.RequestError as e:
                logger.error(f"Error submitting Gemini Batch API job: {e} (attempt {i+1}/3)")
                time.sleep(2**i)
                
    logger.error("All retry attempts failed. Cannot create batch job.")
    return None

async def check_gemini_batch_job(job_id):
    """
    Checks the status of a Gemini batch job.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        return "ERROR"
        
    url = f"https://generativelanguage.googleapis.com/v1beta/{job_id}?key={gemini_api_key}"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=30.0)
            response.raise_for_status()
            status = response.json().get('state')
            return status
        except httpx.RequestError as e:
            logger.error(f"Error checking status for job {job_id}: {e}")
            return "ERROR"

async def download_gemini_batch_results(job_id, original_posts):
    """
    Downloads the results of a completed Gemini batch job and combines them
    with the original posts.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        return []

    competitor_name = job_id.split('/')[1]
    output_uri_path = f"gs://genai-batch-processing/{competitor_name}-results.jsonl"
    url = f"https://generativelanguage.googleapis.com/v1beta/files/{output_uri_path}?key={gemini_api_key}"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=300.0)
            response.raise_for_status()
            
            results_map = {}
            for line in response.text.strip().split('\n'):
                result_obj = json.loads(line)
                key = result_obj['key']
                text_part = result_obj['response']['candidates'][0]['content']['parts'][0]['text']
                
                json_match = re.search(r'\{.*\}', text_part, re.DOTALL)
                if json_match:
                    parsed_json = json.loads(json_match.group(0))
                    results_map[key] = {
                        'summary': parsed_json.get('summary', 'N/A'),
                        'seo_keywords': ', '.join(parsed_json.get('seo_keywords', []))
                    }
                else:
                    results_map[key] = {'summary': 'N/A', 'seo_keywords': 'N/A'}
            
            transformed_posts = []
            for i, post in enumerate(original_posts):
                key = f"post-{i}"
                gemini_data = results_map.get(key, {})
                
                if gemini_data.get('summary') != 'N/A':
                    post['summary'] = gemini_data.get('summary')
                    post['seo_keywords'] = gemini_data.get('seo_keywords')
                
                transformed_posts.append(post)

            posts_with_dates = [p for p in transformed_posts if p['publication_date'] != 'N/A']
            posts_without_dates = [p for p in transformed_posts if p['publication_date'] == 'N/A']

            posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
            
            return posts_with_dates + posts_without_dates
        except httpx.RequestError as e:
            logger.error(f"Error downloading results for job {job_id}: {e}")
            return []
