# tests/test_transform_batch.py
# This file contains unit tests for the batch processing logic.

import pytest
import os
import json
import asyncio
from datetime import datetime
import httpx
import respx

# Import the functions to be tested from their new location
from src.transform.batch import (
    _create_jsonl_from_posts,
    create_gemini_batch_job,
    check_gemini_batch_job,
    download_gemini_batch_results
)
from src.extract._common import _get_post_details # A helper used by the tests

# Set a mock API key for testing
os.environ['GEMINI_API_KEY'] = "test_api_key"

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing."""
    return [
        {
            'title': 'Test Post 1',
            'url': 'https://example.com/post-1',
            'publication_date': '2025-07-30',
            'content': 'This is the content for test post number one. It is a very good post about something.',
            'seo_meta_keywords': 'keyword1, keyword2'
        },
        {
            'title': 'Test Post 2',
            'url': 'https://example.com/post-2',
            'publication_date': '2025-07-29',
            'content': 'This is the content for test post number two. It discusses something else entirely.',
            'seo_meta_keywords': 'keyword3, keyword4'
        }
    ]

@pytest.fixture
def mock_gemini_response_data():
    """Returns a mock JSON response for a successful Gemini API call."""
    return {
        "summary": "This is a mock summary.",
        "seo_keywords": ["mock_keyword1", "mock_keyword2", "mock_keyword3", "mock_keyword4", "mock_keyword5"]
    }

@pytest.mark.asyncio
async def test_create_jsonl_from_posts(mock_posts):
    """
    Tests if the _create_jsonl_from_posts function correctly formats posts
    into a JSONL string.
    """
    jsonl_data = _create_jsonl_from_posts(mock_posts)
    lines = jsonl_data.strip().split('\n')
    
    assert len(lines) == 2
    
    # Check the first line for correct structure
    line1 = json.loads(lines[0])
    assert line1['key'] == 'post-0'
    assert 'summary' in line1['request']['contents'][0]['parts'][0]['text']
    assert line1['metadata']['title'] == 'Test Post 1'

@pytest.mark.asyncio
@respx.mock
async def test_create_gemini_batch_job_success(mock_posts, tmp_path):
    """
    Tests a successful file upload and batch job creation.
    """
    # Define mock responses for the API endpoints
    respx.post("https://generativelanguage.googleapis.com/v1beta/files").mock(
        return_value=httpx.Response(200, json={"name": "files/mock-file-id"})
    )
    respx.post("https://generativelanguage.googleapis.com/v1beta/batches:create").mock(
        return_value=httpx.Response(200, json={"name": "batches/mock-job-id"})
    )
    
    competitor_name = "test_competitor"
    job_id = await create_gemini_batch_job(mock_posts, competitor_name)
    
    assert job_id == "batches/mock-job-id"
    # Ensure the temporary file was cleaned up
    temp_file = tmp_path / "scraped" / competitor_name / "temp_batch_requests.jsonl"
    assert not os.path.exists(temp_file)

@pytest.mark.asyncio
@respx.mock
async def test_create_gemini_batch_job_failed_upload(mock_posts):
    """
    Tests a failed file upload (e.g., non-200 status).
    """
    respx.post("https://generativelanguage.googleapis.com/v1beta/files").mock(
        return_value=httpx.Response(500) # Simulate a server error
    )
    
    competitor_name = "test_competitor"
    job_id = await create_gemini_batch_job(mock_posts, competitor_name)
    
    assert job_id is None

@pytest.mark.asyncio
@respx.mock
async def test_check_gemini_batch_job_succeeded():
    """
    Tests a successful check on a Gemini batch job.
    """
    job_id = "batches/mock-job-id"
    respx.get(f"https://generativelanguage.googleapis.com/v1beta/{job_id}").mock(
        return_value=httpx.Response(200, json={"state": "SUCCEEDED"})
    )
    
    status = await check_gemini_batch_job(job_id)
    
    assert status == "SUCCEEDED"

@pytest.mark.asyncio
@respx.mock
async def test_download_gemini_batch_results_success(mock_posts, mock_gemini_response_data):
    """
    Tests a successful download and parsing of Gemini batch results.
    """
    job_id = "batches/test_competitor-job-id"
    # Mock the JSONL response from the API
    mock_jsonl_response = [
        json.dumps({"key": "post-0", "response": {"candidates": [{"content": {"parts": [{"text": json.dumps(mock_gemini_response_data)}]}}]}}),
        json.dumps({"key": "post-1", "response": {"candidates": [{"content": {"parts": [{"text": json.dumps(mock_gemini_response_data)}]}}]}})
    ]
    
    respx.get(f"https://generativelanguage.googleapis.com/v1beta/files/gs://genai-batch-processing/test_competitor-results.jsonl").mock(
        return_value=httpx.Response(200, content='\n'.join(mock_jsonl_response))
    )
    
    transformed_posts = await download_gemini_batch_results(job_id, mock_posts)
    
    assert len(transformed_posts) == len(mock_posts)
    assert transformed_posts[0]['summary'] == mock_gemini_response_data['summary']
    assert transformed_posts[0]['seo_keywords'] == ', '.join(mock_gemini_response_data['seo_keywords'])

