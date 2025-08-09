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

def create_gemini_batch_job(posts, competitor_name, model_name):
    """
    Submits a list of posts to the Gemini Batch API using the SDK.
    This function is now synchronous.
    """

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
        logger.info(f"Successfully uploaded batch file with ID: {uploaded_file.name}")
        
        logger.info("Creating the batch job.")
        batch_job = client.batches.create(
            model='gemini-2.0-flash-lite',
            src=uploaded_file.name,
            config={
                'display_name': f"{competitor_name}-job-{datetime.now().strftime('%Y%m%d%H%M')}"
            }
        )
        
        job_id = batch_job.name
        logger.info(f"Successfully submitted Gemini batch job with ID: {job_id}")
        
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
        batch_job = client.batches.get(name=job_id)
        job_state = batch_job.state.name
        
        if job_state in completed_states:
            logger.info(f"Gemini batch job {job_id} has completed with state: {job_state}")
        else:
            logger.info(f"Gemini batch job {job_id} is currently {job_state}.")
            
        return job_state
    except Exception as e:
        logger.error(f"Error checking status for job {job_id}: {e}")
        return "ERROR"




def download_gemini_batch_results(job_id, original_posts=None):
    """
    Downloads and processes batch job results. If original_posts is provided,
    it merges the results. If not, it reconstructs posts from the result metadata.
    """
    client = genai.Client()
    logger.info(f"Downloading results for batch job: {job_id}")
    transformed_posts = []

    try:
        batch_job = client.batches.get(name=job_id)
        
        if batch_job.state.name != 'JOB_STATE_SUCCEEDED' or not hasattr(batch_job, 'dest') or not hasattr(batch_job.dest, 'file_name'):
            logger.error(f"Cannot download results. Job state is {batch_job.state.name}.")
            return original_posts or []

        result_file_name = batch_job.dest.file_name
        logger.info(f"Found result file: {result_file_name}. Downloading...")
        file_content_bytes = client.files.download(file=result_file_name)
        result_content = file_content_bytes.decode('utf-8')

        # If we have the original posts, create a map for efficient merging
        if original_posts:
            original_posts_map = {f"post-{i}": post for i, post in enumerate(original_posts)}
        else:
            logger.warning("Original posts file not found. Reconstructing data from batch results. 'content' field will be missing.")
            original_posts_map = {}

        for line in result_content.splitlines():
            if not line.strip():
                continue
            
            result_json = json.loads(line)
            key = result_json.get('key')
            
            # Start with the original post if it exists, otherwise create a new one
            post = original_posts_map.get(key, {})
            if not post: # Reconstruct if missing
                metadata = result_json.get('request', {}).get('metadata', {})
                post = {
                    'title': metadata.get('title', 'N/A'),
                    'url': metadata.get('url', 'N/A'),
                    'publication_date': metadata.get('publication_date', 'N/A'),
                    'content': 'N/A (Reconstructed from batch result)',
                    'seo_meta_keywords': metadata.get('seo_meta_keywords', 'N/A')
                }

            # Add the enriched data from the response
            response_data = result_json.get('response', {})
            if 'candidates' in response_data and response_data['candidates']:
                text_part = response_data['candidates'][0]['content']['parts'][0]['text']
                
                parsed_json = None
                try:
                    parsed_json = json.loads(text_part)
                except json.JSONDecodeError:
                    json_match = re.search(r'\{.*\}', text_part, re.DOTALL)
                    if json_match:
                        try:
                            parsed_json = json.loads(json_match.group(0))
                        except json.JSONDecodeError: pass

                if parsed_json:
                    post['summary'] = parsed_json.get('summary', 'N/A')
                    post['seo_keywords'] = ', '.join(parsed_json.get('seo_keywords', []))
                else:
                    post['summary'] = 'N/A'
                    post['seo_keywords'] = 'N/A'
            
            transformed_posts.append(post)

        # Sort the final list of posts by date
        posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
        posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']
        posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
        
        return posts_with_dates + posts_without_dates

    except Exception as e:
        logger.error(f"An unexpected error occurred while downloading results for job {job_id}: {e}")
        return original_posts or []