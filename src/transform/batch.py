# src/transform/batch.py
# This file acts as a simple interface to the batch processing capabilities
# of the Gemini API Connector.

import os
import json
import logging
from datetime import datetime
from src.api_connector import GeminiAPIConnector

logger = logging.getLogger(__name__)

def _create_jsonl_from_posts(posts):
    """
    Creates a JSONL formatted string from a list of post dictionaries.
    It filters out posts that do not have content.
    """
    jsonl_lines = []
    for i, post in enumerate(posts):
        # This check is crucial: only include posts that have content.
        if post.get('content') and post['content'] != 'N/A':
            prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of the 5 most important SEO keywords, ordered by importance. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {post['content']}"
            
            # This metadata is included in the request and returned in the result,
            # allowing us to reconstruct the post if the temp file is lost.
            metadata = {
                "url": post.get("url"),
                "title": post.get("title"),
                "publication_date": post.get("publication_date"),
                "seo_meta_keywords": post.get("seo_meta_keywords")
            }
            
            request_payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"response_mime_type": "application/json"},
                "metadata": metadata
            }

            json_line = {"key": f"post-{i}", "request": request_payload}
            jsonl_lines.append(json.dumps(json_line))
            
    return "\n".join(jsonl_lines)

def create_gemini_batch_job(posts, competitor_name, model_name, temp_file_path):
    """
    Calls the connector to submit the batch job using the provided file path.
    This function no longer creates or deletes the temp file.
    """
    if not os.path.exists(temp_file_path):
        logger.error(f"Cannot create batch job, temp file not found at: {temp_file_path}")
        return None

    connector = GeminiAPIConnector()
    # Pass the existing file path directly to the connector
    job_id = connector.create_batch_job(posts, competitor_name, model_name, temp_file_path)
    return job_id

def check_gemini_batch_job(job_id, verbose=True):
    """Pass-through function to the API connector."""
    connector = GeminiAPIConnector()
    return connector.check_batch_job(job_id, verbose)

def download_gemini_batch_results(job_id, original_posts=None):
    """Pass-through function to the API connector."""
    connector = GeminiAPIConnector()
    return connector.download_batch_results(job_id, original_posts)