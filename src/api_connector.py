# src/api_connector.py
# This module contains a dedicated connector for all Gemini API interactions.

import logging
import json
import re
import asyncio
import os
from datetime import datetime
from google import genai
from google.genai import types
from google.genai.errors import APIError

from src import utils

logger = logging.getLogger(__name__)

class GeminiAPIConnector:
    """
    A wrapper for all interactions with the Google GenAI SDK.
    """
    def __init__(self):
        try:
            self.client = genai.Client()
        except Exception as e:
            logger.error(f"Failed to initialize Gemini API client. Check your API key. Error: {e}")
            self.client = None

    async def enrich_post_live(self, content, model_name, post_title="Unknown Post"):
        """
        Calls the Gemini API asynchronously to get a summary and keywords for a single post.
        """
        if not self.client:
            return "N/A", "N/A"

        summary = "N/A"
        seo_keywords = "N/A"
        prompt = utils.get_prompt("enrichment_instruction", content=content)

        
        for i in range(3): # Retry logic
            try:
                # --- FIX: The model name should NOT have the 'models/' prefix ---
                response = await self.client.aio.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    generation_config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                parsed_json = json.loads(response.text)
                summary = parsed_json.get('summary', 'N/A')
                seo_keywords = ', '.join(parsed_json.get('seo_keywords', []))
                logger.info(f"    Live enrichment successful for '{post_title}'")
                return summary, seo_keywords
            except Exception as e:
                logger.error(f"    Live enrichment attempt {i+1} failed for '{post_title}': {e}")
                await asyncio.sleep(2**i)
        
        return summary, seo_keywords

    def create_batch_job(self, posts, competitor_name, model_name, temp_file_path):
        """Creates and submits a new batch job from a pre-existing temp file."""
        if not self.client:
            return None
        
        print(temp_file_path)
        try:
            logger.info(f"Uploading file '{os.path.basename(temp_file_path)}' for batch processing.")
            
            uploaded_file = self.client.files.upload(
                file=temp_file_path,
                config=types.UploadFileConfig(mime_type="application/jsonl")
            )
            
            logger.info(f"Successfully uploaded batch file with ID: {uploaded_file.name}")
            
            # The model name for batch jobs also does not need the prefix
            batch_job = self.client.batches.create(
                model=model_name,
                src=uploaded_file.name,
            )
            logger.info(f"Successfully submitted Gemini batch job with ID: {batch_job.name}")
            return batch_job.name
        except APIError as e:
            logger.error(f"Error submitting Gemini Batch API job: {e}")
            return None

    def check_batch_job(self, job_id, verbose=True):
        """Checks the status of a given batch job."""
        if not self.client:
            return "ERROR"
        if verbose:
            logger.info(f"Checking status of Gemini batch job: {job_id}")
        try:
            batch_job = self.client.batches.get(name=job_id)
            return batch_job.state.name
        except Exception as e:
            logger.error(f"Error checking status for job {job_id}: {e}")
            return "ERROR"

    def download_batch_results(self, job_id, original_posts=None):
        """
        Downloads and processes batch job results.
        """
        if not self.client:
            return original_posts or []
        
        logger.info(f"Downloading results for batch job: {job_id}")
        transformed_posts = []

        try:
            batch_job = self.client.batches.get(name=job_id)
            
            if batch_job.state.name != 'JOB_STATE_SUCCEEDED' or not hasattr(batch_job, 'dest') or not hasattr(batch_job.dest, 'file_name'):
                logger.error(f"Cannot download results. Job state is {batch_job.state.name}.")
                return original_posts or []

            result_file_name = batch_job.dest.file_name
            logger.info(f"Found result file: {result_file_name}. Downloading...")
            file_content_bytes = self.client.files.download(file=result_file_name)
            result_content = file_content_bytes.decode('utf-8')

            if original_posts:
                original_posts_map = {f"post-{i}": post for i, post in enumerate(original_posts)}
            else:
                logger.warning("Original posts file not found. Reconstructing data from batch results. 'content' field will be missing.")
                original_posts_map = {}

            for line in result_content.splitlines():
                if not line.strip(): continue
                
                result_json = json.loads(line)
                key = result_json.get('key')
                
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

                response_data = result_json.get('response', {})
                if 'candidates' in response_data and response_data['candidates']:
                    text_part = response_data['candidates'][0]['content']['parts'][0]['text']
                    
                    parsed_json = None
                    try:
                        parsed_json = json.loads(text_part)
                    except json.JSONDecodeError:
                        json_match = re.search(r'\{.*\}', text_part, re.DOTALL)
                        if json_match:
                            try: parsed_json = json.loads(json_match.group(0))
                            except json.JSONDecodeError: pass

                    if parsed_json:
                        post['summary'] = parsed_json.get('summary', 'N/A')
                        post['seo_keywords'] = ', '.join(parsed_json.get('seo_keywords', []))
                    else:
                        post['summary'], post['seo_keywords'] = 'N/A', 'N/A'
                
                transformed_posts.append(post)

            posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
            posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']
            posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
            
            return posts_with_dates + posts_without_dates

        except Exception as e:
            logger.error(f"An unexpected error occurred while downloading results for job {job_id}: {e}")
            return original_posts or []