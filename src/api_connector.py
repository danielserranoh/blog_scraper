# src/api_connector.py
# This module contains a dedicated connector for all Gemini API interactions.

import logging
import json
import re
import asyncio
import os
import csv
import time
from datetime import datetime
from google import genai
from google.genai import types
from google.genai.errors import APIError

from src import utils

logger = logging.getLogger(__name__)

class GeminiAPIConnector:
    """
    A wrapper for all interactions with the Google GenAI SDK, ensuring that
    all API calls are funneled through this single class.
    """
    def __init__(self):
        try:
            self.client = genai.Client()
        except Exception as e:
            logger.error(f"Failed to initialize Gemini API client. Check your API key. Error: {e}")
            self.client = None

    async def enrich_post_live(self, content, model_name, post_title="Unknown Post", headings=None, primary_competitors=None, dxp_competitors=None):
        """
        Calls the Gemini API asynchronously to get a summary and keywords for a single post.
        """
        if not self.client:
            return "N/A", "N/A", "N/A"

        summary = "N/A"
        seo_keywords = "N/A"
        funnel_stage = "N/A"
        prompt = utils.get_prompt("enrichment_instruction", content=content, headings=headings, primary_competitors=primary_competitors, dxp_competitors=dxp_competitors)

        plogger.info(f"Will enrich with the following prompt: {prompt} ")
        for i in range(3): # Retry logic
            try:
                response = await self.client.aio.models.generate_content(
                    model=model_name,
                    contents=prompt,
                   # generation_config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                parsed_json = json.loads(response.text)
                summary = parsed_json.get('summary', 'N/A')
                seo_keywords = ', '.join(parsed_json.get('seo_keywords', []))
                funnel_stage = parsed_json.get('funnel_stage', 'N/A') # <-- NEW: Get funnel_stage from response
                logger.info(f"    Live enrichment successful for '{post_title}'")
                return summary, seo_keywords, funnel_stage
            except Exception as e:
                logger.error(f"    Live enrichment attempt {i+1} failed for '{post_title}': {e}")
                await asyncio.sleep(2**i)
        
        return summary, seo_keywords, funnel_stage

    async def batch_enrich_posts_live(self, posts, model_name, primary_competitors=None, dxp_competitors=None):
        """
        Transforms a batch of extracted post data by enriching it with live,
        asynchronous calls to the Gemini API via the connector.
        """
        start_time = time.time()
        logger.info(f"Batch got {len(posts)} posts to Enrich Live with the {model_name} model")
        tasks = []
        for post in posts:
            if post['content'] and post['content'] != 'N/A':
                tasks.append(self.enrich_post_live(post['content'], model_name, post['title'], post.get('headings'), primary_competitors, dxp_competitors))
            else:
                # Create a completed future for posts with no content
                future = asyncio.Future()
                future.set_result(('N/A', 'N/A', 'N/A'))
                tasks.append(future)

        gemini_results = await asyncio.gather(*tasks)

        transformed_posts = []
        for i, post in enumerate(posts):
            summary, seo_keywords, funnel_stage = gemini_results[i]
            post['summary'] = summary
            post['seo_keywords'] = seo_keywords
            post['funnel_stage'] = funnel_stage
            transformed_posts.append(post)

        logger.info(f"Live enrichment of {len(posts)} posts completed in {time.time() - start_time:.2f} seconds.")

        # Sort the final list
        posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
        posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']
        posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
        
        return posts_with_dates + posts_without_dates

    def create_batch_job(self, posts, competitor_name, model_name, primary_competitors=None, dxp_competitors=None):
        """
        Creates and submits a new batch job from a list of post dictionaries.
        This method handles the creation of the temporary JSONL file internally.
        """
        if not self.client:
            return None
        
        # 1. Create a JSONL formatted string from the post data.
        jsonl_data = self._create_jsonl_from_posts(posts, primary_competitors, dxp_competitors)
        if not jsonl_data:
            logger.warning("No posts with content to submit to Gemini API. Skipping batch job creation.")
            return None

        # 2. Create a temporary file to upload.
        workspace_folder = os.path.join('workspace', competitor_name)
        os.makedirs(workspace_folder, exist_ok=True)
        temp_api_file_path = os.path.join(workspace_folder, "temp_api_requests.jsonl")
        
        try:
            with open(temp_api_file_path, "w", encoding="utf-8") as f:
                f.write(jsonl_data)
            
            logger.info(f"Uploading file '{os.path.basename(temp_api_file_path)}' for batch processing.")
            
            uploaded_file = self.client.files.upload(
                file=temp_api_file_path,
                config=types.UploadFileConfig(display_name=competitor_name+'_'+str(len(posts)) ,mime_type="application/jsonl")
            )
            
            logger.info(f"Successfully uploaded batch file with ID: {uploaded_file.name}")
            
            batch_job = self.client.batches.create(
                model=model_name,
                src=uploaded_file.name,
            )
            logger.info(f"Successfully submitted Gemini batch job with ID: {batch_job.name}")
            return batch_job.name
        except APIError as e:
            logger.error(f"Error submitting Gemini Batch API job: {e}")
            return None
        finally:
            # Always clean up the temporary file after upload.
            if os.path.exists(temp_api_file_path):
                os.remove(temp_api_file_path)

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

    # <--- UPDATED: The function will now be able to reconstruct the post using metadata if the original source file is missing. --->
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
                        'seo_meta_keywords': metadata.get('seo_meta_keywords', 'N/A'),
                        # Include other metadata fields to ensure full reconstruction
                        'headings': metadata.get('headings', []),
                        'schemas': metadata.get('schemas', [])
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
                        # print(parsed_json)
                        post['summary'] = parsed_json.get('summary', 'N/A')
                        post['seo_keywords'] = ', '.join(parsed_json.get('seo_keywords', []))
                        post['funnel_stage'] = parsed_json.get('funnel_stage', 'N/A') # <-- NEW: Get funnel_stage from response
                    else:
                        post['summary'], post['seo_keywords'], post['funnel_stage'] = 'N/A', 'N/A', 'N/A'
                
                transformed_posts.append(post)

            posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
            posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']
            posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
            
            return posts_with_dates + posts_without_dates

        except Exception as e:
            logger.error(f"An unexpected error occurred while downloading results for job {job_id}: {e}")
            return original_posts or []
    
    def list_batch_jobs(self):
        """Lists all batch jobs associated with the API key."""
        if not self.client:
            return []
        try:
            return list(self.client.batches.list())
        except APIError as e:
            logger.error(f"Error listing batch jobs: {e}")
            return []

    def cancel_batch_job(self, job_id):
        """Cancels a specific batch job."""
        if not self.client:
            return
        try:
            self.client.batches.cancel(name=job_id)
            logger.info(f"Successfully cancelled job: {job_id}")
        except APIError as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
    
    def delete_batch_job_file(self, job_id):
        """Deletes file associated with a job from the Gemini API."""
        if not self.client:
            return

        try:
            job = self.client.batches.get(name=job_id)
            if hasattr(job, 'src'):
                self.client.files.delete(job.src)
            if hasattr(job, 'dest'):
                self.client.files.delete(job.dest.file_name)
            logger.info(f"Successfully deleted file for job: {job_id}")
        except APIError as e:
            logger.error(f"Error deleting file for job {job_id}: {e}")

    def _create_jsonl_from_posts(self, posts, primary_competitors=None, dxp_competitors=None):
        """
        Creates a JSONL formatted string from a list of post dictionaries,
        adhering to the correct Gemini API schema.
        """
        jsonl_lines = []
        for i, post in enumerate(posts):
            if post.get('content') and post.get('content') != 'N/A':
                prompt = utils.get_prompt("enrichment_instruction", content=post['content'], headings=post.get('headings'), primary_competitors=primary_competitors, dxp_competitors=dxp_competitors)
                if not prompt: continue
                
                # Create a metadata object to embed in the request payload
                metadata = {
                    "url": post.get('url'),
                    "title": post.get('title'),
                    "publication_date": post.get('publication_date'),
                    "seo_meta_keywords": post.get('seo_meta_keywords'),
                    "headings": post.get('headings', []),
                    "schemas": post.get('schemas', [])
                }
                
                request_payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"response_mime_type": "application/json"}
                }
                json_line = {"key": f"post-{i}", "request": request_payload, "metadata": metadata}
                jsonl_lines.append(json.dumps(json_line))
        return "\n".join(jsonl_lines)