# tests/test_orchestrator.py
# This file contains unit tests for the high-level orchestration logic.

import pytest
import json
from unittest.mock import MagicMock, AsyncMock

# Import the orchestrator module to test its functions
from src import orchestrator

@pytest.fixture
def mock_competitor_config():
    """Provides a mock competitor configuration for tests."""
    return {"name": "test_competitor"}

# This test is a standard function, so it does not need an async decorator.
def test_split_posts_into_chunks_correctly():
    """
    Tests that the _split_posts_into_chunks function correctly splits a list
    of posts based on their estimated size.
    """
    # Create mock posts where each is roughly 100 bytes as a JSON line
    mock_posts = [{"content": "x" * 80}] * 10
    
    # Set a max size of ~350 bytes. This should result in 4 chunks:
    # 3 posts in the first chunk, 3 in the second, 3 in the third, and 1 in the last.
    chunks = orchestrator._split_posts_into_chunks(mock_posts, max_size_mb=0.00035)
    
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1

@pytest.mark.asyncio
async def test_submit_chunks_for_processing_multiple_chunks(mocker, mock_competitor_config):
    """
    Tests that for a large job, the orchestrator correctly splits the posts
    and calls the batch creation function for each chunk.
    """
    mock_posts = [{"content": "x" * 80}] * 10
    
    # Force the chunking function to return two separate chunks
    mocker.patch('src.orchestrator._split_posts_into_chunks', return_value=[mock_posts[:5], mock_posts[5:]])
    
    # Mock all the external functions that get called
    mock_create_job = mocker.patch('src.orchestrator.create_gemini_batch_job', return_value="batches/job-123")
    mock_save_jobs = mocker.patch('src.orchestrator._save_pending_jobs')
    mocker.patch('src.orchestrator._save_raw_posts', return_value="workspace/fake_path.jsonl")
    mocker.patch('src.orchestrator._prompt_to_wait_for_job', new_callable=AsyncMock)

    await orchestrator._submit_chunks_for_processing(mock_competitor_config, mock_posts, "gemini-model", {})

    # The main assertion: check that the job creation was called twice (once for each chunk)
    assert mock_create_job.call_count == 2
    mock_save_jobs.assert_called_once()

@pytest.mark.asyncio
async def test_check_and_load_results_all_succeeded(mocker, mock_competitor_config, caplog):
    """
    Tests that results are processed only when all pending jobs have succeeded.
    """
    # 1. Mock the 'pending_jobs.json' file that the function will read
    pending_jobs_data = [
        {"job_id": "batches/job-1", "raw_posts_file": "chunk1.jsonl", "num_posts": 10},
        {"job_id": "batches/job-2", "raw_posts_file": "chunk2.jsonl", "num_posts": 8}
    ]
    mocker.patch('builtins.open', mocker.mock_open(read_data=json.dumps(pending_jobs_data)))
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('os.remove')
    
    # 2. Mock the external functions that will be called
    mock_check_job = mocker.patch('src.orchestrator.check_gemini_batch_job', return_value="JOB_STATE_SUCCEEDED")
    mock_download = mocker.patch('src.orchestrator.download_gemini_batch_results', return_value=[{"summary": "ok"}])
    mock_save = mocker.patch('src.orchestrator.get_storage_adapter')
    mocker.patch('src.orchestrator._update_performance_log')

    # 3. Run the function, passing an empty dict for the app_config
    await orchestrator.check_and_load_results(mock_competitor_config, {})

    # 4. Assert that the core logic was executed correctly
    assert mock_check_job.call_count == 2 # Called for both jobs
    assert mock_download.call_count == 2 # Called for both jobs
    mock_save().save.assert_called_once() # Called