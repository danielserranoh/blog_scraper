# tests/test_orchestrator.py
# This file contains unit tests for the high-level orchestration logic.

import pytest
import json
from unittest.mock import MagicMock, AsyncMock, PropertyMock

# Import the orchestrator module to test its functions
from src import orchestrator

pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_competitor_config():
    """Provides a mock competitor configuration for tests."""
    return {
        "name": "test_competitor",
        "structure_pattern": "single_page",
        "pagination_pattern": None,
        "base_url": "https://example.com",
        "category_paths": ["blog"],
        "post_list_selector": "a.post",
        "date_selector": "span.date",
        "content_selector": "div.content",
        "content_filter_selector": None
    }

def test_split_posts_into_chunks_correctly():
    """
    Tests that the _split_posts_into_chunks function correctly splits a list
    of posts based on their estimated size.
    """
    # Create mock posts where each is roughly 100 bytes
    mock_posts = [{"content": "x" * 80}] * 10
    
    # Set a max size of ~300 bytes, which should result in 4 chunks
    # (3 posts in the first chunk, 3 in the second, 3 in the third, 1 in the last)
    chunks = orchestrator._split_posts_into_chunks(mock_posts, max_size_mb=0.0003)
    
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[1]) == 3
    assert len(chunks[2]) == 3
    assert len(chunks[3]) == 1

async def test_submit_chunks_for_processing_single_chunk(mocker, mock_competitor_config):
    """
    Tests that for a small number of posts, only one job is created.
    """
    mock_posts = [{"content": "small job"}] * 5
    
    # Mock the functions that are called by the orchestrator
    mock_create_job = mocker.patch('src.orchestrator.create_gemini_batch_job', return_value="batches/job-123")
    mock_save_jobs = mocker.patch('src.orchestrator._save_pending_jobs')
    mock_save_raw = mocker.patch('src.orchestrator._save_raw_posts', return_value="workspace/fake_path.jsonl")
    mocker.patch('src.orchestrator._prompt_to_wait_for_job', new_callable=AsyncMock)

    await orchestrator._submit_chunks_for_processing(mock_competitor_config, mock_posts, "gemini-model", {})

    # Assert that the job creation was only called once
    mock_create_job.assert_called_once()
    mock_save_jobs.assert_called_once()

async def test_submit_chunks_for_processing_multiple_chunks(mocker, mock_competitor_config):
    """
    Tests that for a large number of posts, the job is split into multiple chunks.
    """
    mock_posts = [{"content": "x" * 80}] * 10 # Same as the chunking test
    
    mocker.patch('src.orchestrator._split_posts_into_chunks', return_value=[mock_posts[:5], mock_posts[5:]])
    mock_create_job = mocker.patch('src.orchestrator.create_gemini_batch_job', return_value="batches/job-123")
    mock_save_jobs = mocker.patch('src.orchestrator._save_pending_jobs')
    mocker.patch('src.orchestrator._save_raw_posts', return_value="workspace/fake_path.jsonl")
    mocker.patch('src.orchestrator._prompt_to_wait_for_job', new_callable=AsyncMock)

    await orchestrator._submit_chunks_for_processing(mock_competitor_config, mock_posts, "gemini-model", {})

    # Assert that job creation was called twice (once for each chunk)
    assert mock_create_job.call_count == 2
    mock_save_jobs.assert_called_once()

async def test_check_and_load_results_all_succeeded(mocker, mock_competitor_config, caplog):
    """
    Tests that results are processed only when all pending jobs have succeeded.
    """
    # 1. Mock the pending jobs file
    pending_jobs_data = [
        {"job_id": "batches/job-1", "raw_posts_file": "chunk1.jsonl", "num_posts": 10},
        {"job_id": "batches/job-2", "raw_posts_file": "chunk2.jsonl", "num_posts": 8}
    ]
    mocker.patch('builtins.open', mocker.mock_open(read_data=json.dumps(pending_jobs_data)))
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('os.remove')

    # 2. Mock the API and other function calls
    mock_check_job = mocker.patch('src.orchestrator.check_gemini_batch_job', return_value="JOB_STATE_SUCCEEDED")
    mock_download = mocker.patch('src.orchestrator.download_gemini_batch_results', return_value=[{"summary": "ok"}])
    mock_save = mocker.patch('src.orchestrator.get_storage_adapter')

    # 3. Run the function
    await orchestrator.check_and_load_results(mock_competitor_config, {})

    # 4. Assert that the core logic was executed
    assert mock_check_job.call_count == 2 # Called for both jobs
    assert mock_download.call_count == 2 # Called for both jobs
    mock_save().save.assert_called_once() # Called once with the consolidated results
    assert "All 2 jobs for 'test_competitor' succeeded!" in caplog.text

async def test_check_and_load_results_one_pending(mocker, mock_competitor_config, caplog):
    """
    Tests that if one job is still pending, no results are processed.
    """
    pending_jobs_data = [{"job_id": "batches/job-1"}, {"job_id": "batches/job-2"}]
    mocker.patch('builtins.open', mocker.mock_open(read_data=json.dumps(pending_jobs_data)))
    mocker.patch('os.path.exists', return_value=True)
    
    # Make the first job succeeded, but the second one is still running
    mock_check_job = mocker.patch('src.orchestrator.check_gemini_batch_job', side_effect=["JOB_STATE_SUCCEEDED", "JOB_STATE_RUNNING"])
    mock_download = mocker.patch('src.orchestrator.download_gemini_batch_results')

    await orchestrator.check_and_load_results(mock_competitor_config, {})

    # Assert that the download and save functions were NEVER called
    mock_download.assert_not_called()
    assert "Not all jobs have succeeded. Please check again later." in caplog.text