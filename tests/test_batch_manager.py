# tests/test_batch_manager.py
# This file contains unit tests for the batch processing logic.

import pytest
import os
import json
import logging
from unittest.mock import MagicMock, AsyncMock, PropertyMock
from types import SimpleNamespace # <--- ADD THIS

# Import the new BatchJobManager to test its methods
from src.transform.batch_manager import BatchJobManager
# The API connector is now mocked in conftest.py, so we don't need to import it here.
from google.genai.errors import APIError

# Configure a simple logger for tests
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Set a mock API key for testing environment
os.environ['GEMINI_API_KEY'] = "test_api_key"

@pytest.fixture
def mock_app_config():
    """Provides a mock application configuration."""
    return {"batch_threshold": 10}

@pytest.fixture
def mock_competitor_config():
    """Provides a mock competitor configuration."""
    return {"name": "test_competitor"}

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing."""
    return [
        {'title': 'Test Post 1', 'url': 'https://example.com/post-1', 'publication_date': '2025-07-30', 'content': 'Content 1.', 'seo_meta_keywords': 'k1, k2'},
        {'title': 'Test Post 2', 'url': 'https://example.com/post-2', 'publication_date': '2025-07-29', 'content': 'Content 2.', 'seo_meta_keywords': 'k3, k4'}
    ]

@pytest.mark.asyncio
async def test_submit_new_jobs_calls_connector_create_job(mocker, mock_app_config, mock_competitor_config, mock_posts):
    """
    Tests that the BatchJobManager correctly calls the API connector
    to submit a new batch job.
    """
    mock_api_connector = mocker.patch('src.transform.batch_manager.GeminiAPIConnector', autospec=True).return_value
    mock_api_connector.create_batch_job.return_value = "batches/mock-job-id"
    
    mocker.patch.object(BatchJobManager, '_split_posts_into_chunks', return_value=[mock_posts])
    mocker.patch.object(BatchJobManager, '_save_raw_posts', return_value="workspace/mock_path.jsonl")
    mocker.patch.object(BatchJobManager, '_save_pending_jobs')
    mocker.patch('os.rename')
    mocker.patch.object(BatchJobManager, '_prompt_to_wait_for_job', new_callable=AsyncMock)

    manager = BatchJobManager(mock_app_config)
    # <--- UPDATED: Added the `wait` flag. --->
    await manager.submit_new_jobs(mock_competitor_config, mock_posts, "gemini-model", mock_app_config, "raw_filepath", wait=False)
    
    mock_api_connector.create_batch_job.assert_called_once()
    assert mock_api_connector.create_batch_job.call_args[0][2] == "gemini-model"

@pytest.mark.asyncio
async def test_check_and_load_results_success(mocker, mock_app_config, mock_competitor_config, mock_posts):
    """
    Tests that the BatchJobManager correctly handles a successful job check
    and consolidates the results.
    """
    pending_jobs_data = {"source_raw_filepath": "raw_filepath", "jobs": [
        {"job_id": "batches/job-1", "raw_posts_file": "chunk1.jsonl", "num_posts": 2},
    ]}
    
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('builtins.open', mocker.mock_open(read_data=json.dumps(pending_jobs_data)))
    
    mock_api_connector = mocker.patch('src.transform.batch_manager.GeminiAPIConnector', autospec=True).return_value
    mock_api_connector.check_batch_job.return_value = 'JOB_STATE_SUCCEEDED'
    
    mocker.patch.object(BatchJobManager, '_consolidate_and_save_results', new_callable=AsyncMock)
    mocker.patch.object(BatchJobManager, '_cleanup_workspace')
    mocker.patch('src.utils.get_job_status_summary', return_value=("All jobs succeeded.", True))
    
    manager = BatchJobManager(mock_app_config)
    await manager.check_and_load_results(mock_competitor_config, mock_app_config)

    # Assert that the consolidate and cleanup methods were called for a successful run
    mock_api_connector.check_batch_job.assert_called_once()
    manager._consolidate_and_save_results.assert_called_once()
    manager._cleanup_workspace.assert_called_once()
    
@pytest.mark.asyncio
async def test_consolidate_and_save_results_merges_data(mocker, mock_app_config, mock_competitor_config, mock_posts):
    """
    Tests that the BatchJobManager correctly downloads and merges results
    from the API connector and saves them using the StateManager.
    """
    pending_jobs_data = [{"job_id": "batches/job-1", "raw_posts_file": "chunk1.jsonl", "num_posts": 2}]
    
    # Create mock content for the two different files the method reads
    jsonl_content = json.dumps(mock_posts[0]) + "\n" + json.dumps(mock_posts[1]) + "\n"
    csv_content = "title,publication_date,url,summary,seo_keywords,seo_meta_keywords,content\n" \
                  "Test Post 1,2025-07-30,https://example.com/post-1,N/A,N/A,k1, k2,Content 1.\n" \
                  "Test Post 2,2025-07-29,https://example.com/post-2,N/A,N/A,k3, k4,Content 2.\n"

    # Mock os.path.exists to return True for both file paths
    mocker.patch('os.path.exists', side_effect=[True, True])
    
    # Use a mock open with a side_effect to return different content for each file open call
    mock_file_handle = mocker.mock_open()
    mock_file_handle.side_effect = [
        mocker.mock_open(read_data=jsonl_content).return_value,
        mocker.mock_open(read_data=csv_content).return_value
    ]
    mocker.patch('builtins.open', mock_file_handle)
    
    mock_api_connector = mocker.patch('src.transform.batch_manager.GeminiAPIConnector', autospec=True).return_value
    mock_api_connector.download_batch_results.return_value = [
        {'title': 'Test Post 1', 'url': 'https://example.com/post-1', 'summary': 'Summary 1', 'seo_keywords': 'k1, k2'},
        {'title': 'Test Post 2', 'url': 'https://example.com/post-2', 'summary': 'Summary 2', 'seo_keywords': 'k3, k4'}
    ]
    
    mock_state_manager = mocker.patch('src.transform.batch_manager.StateManager')
    mocker.patch('src.utils.update_performance_log')

    manager = BatchJobManager(mock_app_config)
    await manager._consolidate_and_save_results(mock_competitor_config, pending_jobs_data, mock_app_config, "raw_filepath")
    
    # Assert that the download was called and the save method on StateManager was called
    mock_api_connector.download_batch_results.assert_called_once()
    mock_state_manager().save_processed_data.assert_called_once()