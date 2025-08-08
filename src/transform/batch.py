# src/transform/batch.py
# This file contains the batch API processing logic.

from datetime import datetime
import json
import os
import time
import logging
import re
from google import genai
from google.genai import types


logger = logging.getLogger(__name__)


def _create_jsonl_from_posts(posts):
    """
    Creates a JSONL string from a list of posts in memory.
    This function serializes a list of post dictionaries into a
    JSON Lines format, which is required by the API.
    """
    jsonl_lines = []
    for i, post in enumerate(posts):
        if post['content'] and post['content'] != 'N/A':
            prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of the 5 most important SEO keywords, ordered by importance. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {post['content']}"
            request_payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"response_mime_type": "application/json"}
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

def create_gemini_batch_job(posts, competitor_name):
    """
    Submits a list of posts to the Gemini Batch API using the SDK.
    This function is now synchronous.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        logger.error("GEMINI_API_KEY not set. Cannot create batch job.")
        return None
    
    client = genai.Client()

    jsonl_data = _create_jsonl_from_posts(posts)
    if not jsonl_data:
        logger.warning("No posts with content to submit to Gemini API. Skipping batch job creation.")
        return None

    batch_file_path = os.path.join("scraped", competitor_name, "temp_batch_requests.jsonl")
    os.makedirs(os.path.dirname(batch_file_path), exist_ok=True)
    with open(batch_file_path, "w") as f:
        f.write(jsonl_data)

    try:
        logger.info(f"Uploading {len(posts)} posts for batch processing.")
        
        uploaded_file = client.files.upload(
            file=batch_file_path,
            config={
                'display_name': f"{competitor_name}-posts.jsonl",
                'mime_type': 'application/jsonl'
            }
        )
        logger.success(f"Successfully uploaded batch file with ID: {uploaded_file.name}")
        
        logger.info("Creating the batch job.")
        batch_job = client.batches.create(
            model='gemini-2.0-flash-lite',
            src=uploaded_file.name,
            config={
                'display_name': f"{competitor_name}-job-{datetime.now().strftime('%Y%m%d%H%M')}"
            }
        )
        
        job_id = batch_job.name
        logger.success(f"Successfully submitted Gemini batch job with ID: {job_id}")
        
        os.remove(batch_file_path)
        
        return job_id
    except Exception as e: # Catch a broader range of exceptions
        logger.error(f"Error submitting Gemini Batch API job: {e}")
        return None

def check_gemini_batch_job(job_id):
    """
    Checks the status of a Gemini batch job using the SDK.
    """
    completed_states = {
        'JOB_STATE_SUCCEEDED',
        'JOB_STATE_FAILED',
        'JOB_STATE_CANCELLED',
        'JOB_STATE_EXPIRED',
    }
    client = genai.Client()
    logger.info(f"Checking status of Gemini batch job: {job_id}")
    
    try:
        batch_job = client.batches.get(job_id)
        job_state = batch_job.state.name
        
        if job_state in completed_states:
            logger.info(f"Gemini batch job {job_id} has completed with state: {job_state}")
        else:
            logger.info(f"Gemini batch job {job_id} is currently {job_state}.")
            
        return job_state
    except Exception as e:
        logger.error(f"Error checking status for job {job_id}: {e}")
        return "ERROR"

def download_gemini_batch_results(job_id, original_posts):
    """
    Downloads the results of a completed Gemini batch job using the SDK
    and combines them with the original posts.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.error("GEMINI_API_KEY not set. Cannot download batch results.")
        return []

    genai.configure(api_key=gemini_api_key)
    client = genai.Client()

    try:
        batch_job = client.batches.get(job_id)
        
        # This assumes results are inlined. For very large jobs, you might need
        # to handle downloading from a result file URI.
        results = batch_job.inlined_responses
        if not results:
            logger.warning(f"Job {job_id} did not have inlined responses. Cannot process results.")
            return original_posts # Return original posts to avoid data loss

        results_map = {}
        for result_item in results:
            key = result_item.metadata.get('key')
            
            # Ensure there are candidates and content parts
            if not result_item.candidates or not result_item.candidates[0].content.parts:
                logger.warning(f"No content found in response for key {key}.")
                results_map[key] = {'summary': 'N/A', 'seo_keywords': 'N/A'}
                continue

            text_part = result_item.candidates[0].content.parts[0].text
            parsed_json = None
            
            try:
                # 1. First, try to load the whole text as JSON (ideal case)
                parsed_json = json.loads(text_part)
            except json.JSONDecodeError:
                # 2. If that fails, look for a JSON object inside markdown code fences
                logger.warning(f"Could not parse the full response for key {key}. Falling back to regex search.")
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', text_part, re.DOTALL)
                if not json_match:
                    # 3. If that also fails, try a more general search for a JSON object
                    json_match = re.search(r'(\{.*?\})', text_part, re.DOTALL)

                if json_match:
                    try:
                        parsed_json = json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse extracted JSON for key {key}. Response text: {text_part}")
                        parsed_json = None
            
            if parsed_json:
                results_map[key] = {
                    'summary': parsed_json.get('summary', 'N/A'),
                    'seo_keywords': ', '.join(parsed_json.get('seo_keywords', []))
                }
            else:
                logger.error(f"All parsing methods failed for key {key}. Storing N/A.")
                results_map[key] = {'summary': 'N/A', 'seo_keywords': 'N/A'}
        
        transformed_posts = []
        for i, post in enumerate(original_posts):
            key = f"post-{i}"
            if key in results_map:
                gemini_data = results_map[key]
                # Only update if the batch job provided a real summary
                if gemini_data.get('summary') != 'N/A':
                    post['summary'] = gemini_data.get('summary')
                    post['seo_keywords'] = gemini_data.get('seo_keywords')
            
            transformed_posts.append(post)

        # Sort posts by date, handling posts without a valid date
        posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
        posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']

        posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
        
        return posts_with_dates + posts_without_dates

    except Exception as e: # Catch a broader range of potential SDK or other errors
        logger.error(f"An unexpected error occurred while downloading results for job {job_id}: {e}")
        return original_posts # Return original posts to prevent data loss