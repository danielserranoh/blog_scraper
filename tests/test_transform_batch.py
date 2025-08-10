# tests/test_transform_batch.py
# This file contains unit tests for the batch processing logic.

import pytest
import os
import json
import logging
from unittest.mock import MagicMock, PropertyMock

# Import the module we are testing to patch it directly
from src.transform import batch
from google.genai.errors import APIError

# Configure a simple logger for tests
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Set a mock API key for testing environment
os.environ['GEMINI_API_KEY'] = "test_api_key"

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing."""
    return [
        {'title': 'Test Post 1', 'url': 'https://example.com/post-1', 'publication_date': '2025-07-30', 'content': 'Content 1.', 'seo_meta_keywords': 'k1, k2'},
        {'title': 'Test Post 2', 'url': 'https://example.com/post-2', 'publication_date': '2025-07-29', 'content': 'Content 2.', 'seo_meta_keywords': 'k3, k4'}
    ]

def test_create_jsonl_from_posts(mock_posts):
    """
    Tests if the _create_jsonl_from_posts function correctly formats posts.
    """
    jsonl_data = batch._create_jsonl_from_posts(mock_posts)
    lines = jsonl_data.strip().split('\n')
    assert len(lines) == 2
    line1 = json.loads(lines[0])
    assert line1['key'] == 'post-0'

def test_create_gemini_batch_job_success(mocker, mock_posts):
    """
    Tests a successful batch job creation.
    """
    mock_files_service = MagicMock()
    mock_batches_service = MagicMock()
    mock_client = MagicMock(files=mock_files_service, batches=mock_batches_service)

    mock_uploaded_file = MagicMock(spec=['name'])
    mock_uploaded_file.name = 'files/mock-file-id'
    mock_files_service.upload.return_value = mock_uploaded_file

    mock_created_job = MagicMock(spec=['name'])
    mock_created_job.name = 'batches/mock-job-id'
    mock_batches_service.create.return_value = mock_created_job

    mocker.patch.object(batch.genai, 'Client', return_value=mock_client)
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mocker.mock_open())
    mocker.patch('os.remove')

    job_id = batch.create_gemini_batch_job(mock_posts, "test_competitor", "gemini-model")

    assert job_id == "batches/mock-job-id"

def test_create_gemini_batch_job_api_error(mocker, mock_posts):
    """
    Tests that the function returns None when any exception occurs.
    """
    mock_files_service = MagicMock()
    mock_files_service.upload.side_effect = APIError("Failed to upload", response_json={})
    mock_client = MagicMock(files=mock_files_service)

    mocker.patch.object(batch.genai, 'Client', return_value=mock_client)
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mocker.mock_open())
    
    job_id = batch.create_gemini_batch_job(mock_posts, "test_competitor", "gemini-model")
    assert job_id is None

def test_check_gemini_batch_job_succeeded(mocker):
    """
    Tests a successful check on a Gemini batch job.
    """
    mock_state = MagicMock(name='JobState')
    mock_state.name = 'JOB_STATE_SUCCEEDED'
    mock_job = MagicMock(name='BatchJob')
    type(mock_job).state = PropertyMock(return_value=mock_state)
    
    mock_batches_service = MagicMock()
    mock_batches_service.get.return_value = mock_job
    mock_client = MagicMock(batches=mock_batches_service)

    mocker.patch.object(batch.genai, 'Client', return_value=mock_client)
    status = batch.check_gemini_batch_job("batches/mock-job-id")
    assert status == "JOB_STATE_SUCCEEDED"

def test_download_gemini_batch_results_success(mocker, mock_posts):
    """
    Tests a successful download and parsing of results.
    """
    response1_data = json.dumps({"summary": "Perfect summary.", "seo_keywords": ["k1", "k2"]})
    
    mock_job = MagicMock(name='BatchJob')
    mock_state = MagicMock(name='JobState')
    mock_state.name = 'JOB_STATE_SUCCEEDED'
    type(mock_job).state = PropertyMock(return_value=mock_state)
    type(mock_job).dest = PropertyMock(return_value=MagicMock(file_name="results.jsonl"))

    mock_batches_service = MagicMock()
    mock_batches_service.get.return_value = mock_job
    
    mock_files_service = MagicMock()
    mock_client = MagicMock(batches=mock_batches_service, files=mock_files_service)

    mocker.patch.object(batch.genai, 'Client', return_value=mock_client)
    
    # --- FIX: Define result_json BEFORE using it ---
    result_json = {
        "key": "post-0",
        "response": { "candidates": [{"content": {"parts": [{"text": response1_data}]}}]}
    }
    # Now that it's defined, we can set the return value for the mock.
    mock_files_service.download.return_value = json.dumps(result_json).encode('utf-8')

    transformed_posts = batch.download_gemini_batch_results("batches/mock-job-id", [mock_posts[0]])
    
    assert transformed_posts[0]['summary'] == "Perfect summary."
    assert transformed_posts[0]['seo_keywords'] == "k1, k2"