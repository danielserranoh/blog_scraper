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
        Calls the Gemini API asynchronously to get enhanced analysis including strategic insights for a single post.
        Note: Content should already be preprocessed by ContentPreprocessor before calling this method.
        """
        if not self.client:
            return "N/A", "N/A", "N/A", "N/A", {}

        summary = "N/A"
        seo_keywords = "N/A"
        funnel_stage = "N/A"
        target_audience = "N/A"
        strategic_analysis = {}
        
        prompt = utils.get_prompt("enrichment_instruction", content=content, headings=headings, primary_competitors=primary_competitors, dxp_competitors=dxp_competitors)

        logger.debug(f"Enriching post: '{post_title[:50]}...'")
        logger.debug(f"  Content length: {len(content)} characters")
        logger.debug(f"  Prompt length: {len(prompt)} characters")
        
        for attempt in range(3): # Retry logic
            try:
                response = await self.client.aio.models.generate_content(
                    model=model_name,
                    contents=prompt
                )
                
                # Check if response has content
                if not response or not hasattr(response, 'text') or not response.text:
                    logger.warning(f"    Attempt {attempt+1}: API returned empty response for '{post_title}'")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    continue
                
                response_text = response.text.strip()
                
                # Check if response starts with AFC message (indicates malformed API request)
                if response_text.startswith("AFC is enabled") or "AFC is enabled" in response_text:
                    logger.error(f"    Attempt {attempt+1}: AFC error detected - API request is malformed for '{post_title}'")
                    logger.error(f"    Raw response: {response_text[:200]}...")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    continue
                
                # Try to parse as JSON
                try:
                    parsed_json = json.loads(response_text)
                except json.JSONDecodeError as json_error:
                    # Try to extract JSON from response if it's wrapped in other text
                    import re
                    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                    if json_match:
                        try:
                            parsed_json = json.loads(json_match.group(0))
                            logger.debug(f"    Successfully extracted JSON from wrapped response")
                        except json.JSONDecodeError:
                            # Only log warning if both direct parsing and extraction failed
                            logger.warning(f"    Attempt {attempt+1}: JSON parsing failed for '{post_title}': {json_error}")
                            if attempt < 2:
                                await asyncio.sleep(2 ** attempt)
                            continue
                    else:
                        # Only log warning if both direct parsing and extraction failed
                        logger.warning(f"    Attempt {attempt+1}: JSON parsing failed for '{post_title}': {json_error}")
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)
                        continue
                
                # Successfully parsed JSON - verify we have actual data
                if parsed_json and isinstance(parsed_json, dict):
                    summary = parsed_json.get('summary', 'N/A')
                    seo_keywords = ', '.join(parsed_json.get('seo_keywords', []))
                    funnel_stage = parsed_json.get('funnel_stage', 'N/A')
                    target_audience = parsed_json.get('target_audience', 'N/A')
                    strategic_analysis = parsed_json.get('strategic_analysis', {})
                    
                    # Only mark as successful if we got actual content, not just N/A
                    if summary != 'N/A' or seo_keywords != 'N/A' or funnel_stage != 'N/A':
                        logger.info(f"    ✓ Live enrichment successful for '{post_title}' (attempt {attempt+1})")
                        return summary, seo_keywords, funnel_stage, target_audience, strategic_analysis
                    else:
                        logger.warning(f"    API returned only N/A values for '{post_title}' (attempt {attempt+1})")
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)
                        continue
                else:
                    logger.warning(f"    API returned invalid JSON structure for '{post_title}' (attempt {attempt+1})")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    continue
                
            except Exception as e:
                logger.warning(f"    Attempt {attempt+1}: API call failed for '{post_title}': {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        
        logger.error(f"    ❌ API enrichment completely failed for '{post_title}' after 3 attempts")
        logger.error(f"    Possible causes: malformed API request (AFC error), network issues, or API rate limiting")
        return summary, seo_keywords, funnel_stage, target_audience, strategic_analysis

    def _prepare_content_for_api(self, content, post_title):
        """
        Prepares content for API consumption by cleaning and potentially truncating it.
        """
        if not content or content == 'N/A':
            return content
            
        # Clean problematic characters
        cleaned_content = content
        
        # Replace smart quotes and other problematic Unicode characters
        char_replacements = {
            ''': "'",  # Left single quotation mark
            ''': "'",  # Right single quotation mark
            '"': '"',  # Left double quotation mark  
            '"': '"',  # Right double quotation mark
            '—': '-',  # Em dash
            '–': '-',  # En dash
            '…': '...',  # Horizontal ellipsis
        }
        
        for old_char, new_char in char_replacements.items():
            cleaned_content = cleaned_content.replace(old_char, new_char)
        
        # Check content length and truncate if necessary
        from src import utils
        config = utils.get_content_processing_config()
        MAX_CONTENT_LENGTH = config['api_content_limit']  # Get limit from config
        
        if len(cleaned_content) > MAX_CONTENT_LENGTH:
            logger.warning(f"Content for '{post_title}' is {len(cleaned_content)} chars, truncating to {MAX_CONTENT_LENGTH}")
            
            # Try to truncate at a sentence boundary
            truncated = cleaned_content[:MAX_CONTENT_LENGTH]
            last_period = truncated.rfind('.')
            last_exclamation = truncated.rfind('!')
            last_question = truncated.rfind('?')
            
            # Find the last sentence ending
            last_sentence_end = max(last_period, last_exclamation, last_question)
            
            if last_sentence_end > MAX_CONTENT_LENGTH * 0.8:  # If we can keep 80% and end on sentence
                cleaned_content = truncated[:last_sentence_end + 1] + " [Content truncated for API processing]"
            else:
                # Just truncate and add notice
                cleaned_content = truncated + "... [Content truncated for API processing]"
            
            logger.info(f"Content truncated to {len(cleaned_content)} characters for '{post_title}'")
        
        return cleaned_content

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
                future.set_result(('N/A', 'N/A', 'N/A', 'N/A', {}))
                tasks.append(future)

        gemini_results = await asyncio.gather(*tasks)

        transformed_posts = []
        failed_posts = []
        
        for i, post in enumerate(posts):
            summary, seo_keywords, funnel_stage, target_audience, strategic_analysis = gemini_results[i]
            
            # Check if enrichment actually failed (all core values are N/A despite having content)
            has_content = post.get('content') and post.get('content') != 'N/A' and len(post.get('content', '').strip()) > 10
            all_na_values = (summary == 'N/A' and seo_keywords == 'N/A' and funnel_stage == 'N/A')
            
            # Initialize metadata if not present
            if 'metadata' not in post:
                post['metadata'] = {}
                
            if has_content and all_na_values:
                # This indicates API failure for a post that should have been enrichable
                failed_posts.append(post['title'])
                post['metadata']['enrichment_status'] = 'failed'
                logger.warning(f"  ⚠️ Post '{post['title']}' marked as failed - API enrichment returned only N/A values")
            elif not has_content:
                # Post had no content to enrich
                post['metadata']['enrichment_status'] = 'no_content'
            else:
                # Successfully enriched
                post['metadata']['enrichment_status'] = 'completed'
                
            # Update post with enrichment data regardless (for consistency)
            post['summary'] = summary
            post['seo_keywords'] = seo_keywords
            post['funnel_stage'] = funnel_stage
            post['target_audience'] = target_audience
            post['strategic_analysis'] = strategic_analysis
            transformed_posts.append(post)

        # Calculate enrichment statistics
        completed_count = len([p for p in transformed_posts if p.get('metadata', {}).get('enrichment_status') == 'completed'])
        failed_count = len([p for p in transformed_posts if p.get('metadata', {}).get('enrichment_status') == 'failed'])
        no_content_count = len([p for p in transformed_posts if p.get('metadata', {}).get('enrichment_status') == 'no_content'])
        
        logger.info(f"Live enrichment completed in {time.time() - start_time:.2f} seconds:")
        logger.info(f"  ✅ Successfully enriched: {completed_count}/{len(posts)} posts")
        
        if failed_count > 0:
            logger.error(f"  ❌ API enrichment failed: {failed_count} posts")
            logger.error(f"  Common causes: AFC errors (malformed requests), API rate limits, or network issues")
            for title in failed_posts[:3]:  # Show first 3 failed titles
                logger.error(f"    - {title}")
            if len(failed_posts) > 3:
                logger.error(f"    ... and {len(failed_posts) - 3} more")
            logger.error("💡 These posts will need re-processing when API issues are resolved")
            logger.error("🔧 If you see AFC errors, check the API request format and prompt structure")
        
        if no_content_count > 0:
            logger.info(f"  ⭕ No content to enrich: {no_content_count} posts")

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
        Downloads and processes batch job results, including merging chunked content.
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
                        'headings': metadata.get('headings', []),
                        'schemas': metadata.get('schemas', []),
                        'content_processing': metadata.get('content_processing', {})
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

                    # Initialize metadata if not present
                    if 'metadata' not in post:
                        post['metadata'] = {}
                        
                    if parsed_json:
                        post['summary'] = parsed_json.get('summary', 'N/A')
                        post['seo_keywords'] = ', '.join(parsed_json.get('seo_keywords', []))
                        post['funnel_stage'] = parsed_json.get('funnel_stage', 'N/A')
                        post['target_audience'] = parsed_json.get('target_audience', 'N/A')
                        post['strategic_analysis'] = parsed_json.get('strategic_analysis', {})
                        post['metadata']['enrichment_status'] = 'completed'
                    else:
                        post['summary'], post['seo_keywords'], post['funnel_stage'], post['target_audience'] = 'N/A', 'N/A', 'N/A', 'N/A'
                        post['strategic_analysis'] = {}
                        post['metadata']['enrichment_status'] = 'failed'
                
                transformed_posts.append(post)

            # Merge chunked results back together
            from src.transform.content_preprocessor import ContentPreprocessor
            merged_posts = ContentPreprocessor.merge_chunked_results(transformed_posts)

            posts_with_dates = [p for p in merged_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
            posts_without_dates = [p for p in merged_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']
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
        Note: Posts should already be preprocessed by ContentPreprocessor.
        """
        jsonl_lines = []
        for i, post in enumerate(posts):
            content = post.get('content', '')
            if content and content != 'N/A':
                prompt = utils.get_prompt("enrichment_instruction", content=content, headings=post.get('headings'), primary_competitors=primary_competitors, dxp_competitors=dxp_competitors)
                if not prompt: continue
                
                # Create a metadata object to embed in the request payload
                metadata = {
                    "url": post.get('url'),
                    "title": post.get('title'),
                    "publication_date": post.get('publication_date'),
                    "seo_meta_keywords": post.get('seo_meta_keywords'),
                    "headings": post.get('headings', []),
                    "schemas": post.get('schemas', []),
                    "content_processing": post.get('content_processing', {})  # Include preprocessing info
                }
                
                request_payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"response_mime_type": "application/json"}
                }
                json_line = {"key": f"post-{i}", "request": request_payload, "metadata": metadata}
                jsonl_lines.append(json.dumps(json_line))
        return "\n".join(jsonl_lines)