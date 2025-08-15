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
    Creates a JSONL formatted string from a list of post dictionaries,
    adhering to the correct Gemini API schema.
    """
    jsonl_lines = []
    for i, post in enumerate(posts):
        if post.get('content') and post.get('content') != 'N/A':
            prompt = utils.get_prompt("enrichment_instruction", content=post['content'])
            if not prompt: continue
            
            # The contents and generationConfig must be nested inside a 'request' object.
            request_payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"response_mime_type": "application/json"}
            }

            # The final line in the JSONL must have a 'key' and a 'request' object.
            json_line = {"key": f"post-{i}", "request": request_payload}
            jsonl_lines.append(json.dumps(json_line))
            
    return "\n".join(jsonl_lines)

def create_gemini_batch_job(posts, competitor_name, model_name):
    """
    Creates a temporary, correctly formatted JSONL file and calls the
    connector to submit the batch job.
    """
    # 1. Prepare the API-compliant JSONL data.
    jsonl_data = _create_jsonl_from_posts(posts)
    if not jsonl_data:
        logger.warning("No posts with content to submit to Gemini API. Skipping batch job creation.")
        return None

    # 2. Create a new, temporary file specifically for this API request.
    workspace_folder = os.path.join('workspace', competitor_name)
    os.makedirs(workspace_folder, exist_ok=True)
    # This file is different from the 'unsubmitted_posts' file.
    temp_api_file_path = os.path.join(workspace_folder, "temp_api_requests.jsonl")
    
    try:
        with open(temp_api_file_path, "w", encoding="utf-8") as f:
            f.write(jsonl_data)

        # 3. Call the connector with the path to the correctly formatted file.
        connector = GeminiAPIConnector()
        return connector.create_batch_job(posts, competitor_name, model_name, temp_api_file_path)
    finally:
        # 4. Ensure the temporary API file is always cleaned up.
        if os.path.exists(temp_api_file_path):
            os.remove(temp_api_file_path)
    return job_id

def check_gemini_batch_job(job_id, verbose=True):
    """Pass-through function to the API connector."""
    connector = GeminiAPIConnector()
    return connector.check_batch_job(job_id, verbose)

def download_gemini_batch_results(job_id, original_posts=None):
    """Pass-through function to the API connector."""
    connector = GeminiAPIConnector()
    return connector.download_batch_results(job_id, original_posts)