# src/transform.py
# This file contains the data transformation logic.

from datetime import datetime
import json
import os
import time
import logging
import re
import httpx
import asyncio

logger = logging.getLogger(__name__)

async def _get_gemini_details_batch(client, content):
    """
    Prepares a single request payload for the Gemini Batch API.
    """
    prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of the 5 most important SEO keywords, ordered by importance. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {content}"
    return {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json"}
    }

async def create_gemini_batch_job(posts, competitor_name):
    """
    Submits a list of posts to the Gemini Batch API.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        logger.error("GEMINI_API_KEY not set. Cannot create batch job.")
        return None

    batch_requests = []
    for i, post in enumerate(posts):
        if post['content'] and post['content'] != 'N/A':
            request_payload = await _get_gemini_details_batch(None, post['content']) # No client needed for request payload
            batch_requests.append({
                "key": f"post-{i}",
                "request": request_payload
            })
    
    if not batch_requests:
        logger.warning("No posts with content to submit to Gemini API. Skipping batch job creation.")
        return None

    # The API documentation recommends preparing a JSON Lines file
    batch_file_path = os.path.join("scraped", competitor_name, "temp_batch_requests.jsonl")
    os.makedirs(os.path.dirname(batch_file_path), exist_ok=True)
    with open(batch_file_path, "w") as f:
        for request_item in batch_requests:
            f.write(json.dumps(request_item) + "\n")
    
    logger.info(f"Generated {len(batch_requests)} Gemini Batch API requests in '{batch_file_path}'.")

    async with httpx.AsyncClient() as client:
        try:
            # Step 1: Upload the file
            upload_url = f"https://generativelanguage.googleapis.com/v1beta/files:upload?key={gemini_api_key}"
            with open(batch_file_path, "rb") as f:
                upload_response = await client.post(upload_url, data=f.read(), headers={"Content-Type": "application/jsonl"}, timeout=300.0)
            upload_response.raise_for_status()
            file_details = upload_response.json()
            file_id = file_details.get("name")
            
            if not file_id:
                logger.error("Failed to upload batch file. No file ID received.")
                return None
            logger.success(f"Successfully uploaded batch file with ID: {file_id}")
            
            # Step 2: Create the batch job
            batch_url = f"https://generativelanguage.googleapis.com/v1beta/batches:create?key={gemini_api_key}"
            batch_payload = {
                "input_file": { "uri": f"gs://genai-batch-processing/{file_id}" }, # Note: The API uses a Google Storage URI
                "model": "models/gemini-2.5-flash-preview-05-20",
                "output_file": {"uri": f"gs://genai-batch-processing/{competitor_name}-results.jsonl"}
            }
            batch_response = await client.post(batch_url, json=batch_payload, timeout=300.0)
            batch_response.raise_for_status()
            batch_details = batch_response.json()
            job_id = batch_details.get("name")
            
            # Clean up the temporary requests file
            os.remove(batch_file_path)
            
            return job_id

        except httpx.RequestError as e:
            logger.error(f"Error submitting Gemini Batch API job: {e}")
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

    # Note: The API does not have a direct download endpoint.
    # We must construct the path to the output file saved to Google Cloud Storage.
    # This requires knowing the output file name, which is part of the job metadata.
    # For now, we'll assume a consistent naming convention.
    competitor_name = job_id.split('/')[1] # Get name from job_id: batches/competitor-id
    output_url = f"https://generativelanguage.googleapis.com/v1beta/files/{competitor_name}-results.jsonl?key={gemini_api_key}"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(output_url, timeout=300.0)
            response.raise_for_status()
            
            # Parse the JSONL content line by line
            results_map = {}
            for line in response.text.strip().split('\n'):
                result_obj = json.loads(line)
                key = result_obj['key']
                text_part = result_obj['response']['candidates'][0]['content']['parts'][0]['text']
                
                # Extract the JSON object from the text
                json_match = re.search(r'\{.*\}', text_part, re.DOTALL)
                if json_match:
                    parsed_json = json.loads(json_match.group(0))
                    results_map[key] = {
                        'summary': parsed_json.get('summary', 'N/A'),
                        'seo_keywords': ', '.join(parsed_json.get('seo_keywords', []))
                    }
                else:
                    results_map[key] = {'summary': 'N/A', 'seo_keywords': 'N/A'}
            
            # Combine the results with the original posts
            transformed_posts = []
            for i, post in enumerate(original_posts):
                key = f"post-{i}"
                gemini_data = results_map.get(key, {})
                post['summary'] = gemini_data.get('summary', 'N/A')
                post['seo_keywords'] = gemini_data.get('seo_keywords', 'N/A')
                transformed_posts.append(post)

            return transformed_posts
        except httpx.RequestError as e:
            logger.error(f"Error downloading results for job {job_id}: {e}")
            return []

async def transform_posts(posts):
    """
    This function is now deprecated in favor of the new batch processing workflow.
    """
    pass

