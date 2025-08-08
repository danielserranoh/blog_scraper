# tests/test_transform_batch.py
# This file contains unit tests for the batch processing logic.

import pytest
import os
import json
import logging
from unittest.mock import MagicMock, PropertyMock

# Import the module we are testing to patch it directly
from src.transform import batch


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

def test_create_gemini_batch_job_success(mocker, mock_posts, model_name):
    """
    Tests a successful batch job creation.
    """
    mock_files_service = MagicMock()
    mock_batches_service = MagicMock()
    # This is the mock for the main client object
    mock_client = MagicMock(files=mock_files_service, batches=mock_batches_service)

    mock_uploaded_file = MagicMock(spec=['name'])
    mock_uploaded_file.name = 'files/mock-file-id'
    mock_files_service.upload.return_value = mock_uploaded_file

    mock_created_job = MagicMock(spec=['name'])
    mock_created_job.name = 'batches/mock-job-id'
    mock_batches_service.create.return_value = mock_created_job

    # Patch the Client class in the 'batch' module to return our mock
    mocker.patch.object(batch, 'genai', Client=lambda: mock_client)
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mocker.mock_open())
    mocker.patch('os.remove')

    job_id = batch.create_gemini_batch_job(mock_posts, "test_competitor")

    assert job_id == "batches/mock-job-id"

def test_create_gemini_batch_job_api_error(mocker, mock_posts, model_name):
    """
    Tests that the function returns None when the API raises an error.
    """
    # --- FIX: We test two things:
    # 1. That an exception inside the function is caught.
    # 2. That the function returns None as a result.
    
    # Let's simulate the API call failing, not the client creation
    mock_files_service = MagicMock()
    # Make the 'upload' call raise an error
    mock_files_service.upload.side_effect = Exception("Failed to upload")
    mock_client = MagicMock(files=mock_files_service)

    mocker.patch.object(batch.genai, 'Client', return_value=mock_client)
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mocker.mock_open())
    
    # The function should catch the exception and return None
    # We add a dummy model name to match the function's signature
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
    mock_part1 = MagicMock(text=response1_data)
    mock_candidate1 = MagicMock(content=MagicMock(parts=[mock_part1]))
    mock_response_item_1 = MagicMock(metadata={'key': 'post-0'}, candidates=[mock_candidate1])

    mock_job = MagicMock(name='BatchJob')
    type(mock_job).inlined_responses = PropertyMock(return_value=[mock_response_item_1])
    
    mock_batches_service = MagicMock()
    mock_batches_service.get.return_value = mock_job
    mock_client = MagicMock(batches=mock_batches_service)

    # We only need to mock the Client, as 'configure' is no longer used
    mocker.patch.object(batch.genai, 'Client', return_value=mock_client)
    
    transformed_posts = batch.download_gemini_batch_results("batches/mock-job-id", [mock_posts[0]])
    
    assert transformed_posts[0]['summary'] == "Perfect summary."
    assert transformed_posts[0]['seo_keywords'] == "k1, k2"