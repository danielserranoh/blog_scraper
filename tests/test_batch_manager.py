# tests/test_batch_manager.py
# This file contains unit tests for the batch processing logic.

import pytest
import os
import json
import logging
from unittest.mock import MagicMock, AsyncMock, mock_open

from src.transform.batch_manager import BatchJobManager
from src.exceptions import BatchJobError

@pytest.fixture
def mock_app_config():
    """Provides a mock application configuration."""
    return {"batch_threshold": 10}

@pytest.fixture
def mock_pending_jobs():
    """Sample pending jobs data."""
    return {
        "source_raw_filepath": "data/raw/test_competitor/test_file.json",
        "jobs": [
            {
                "job_id": "batches/test-job-1",
                "raw_posts_file": "temp_posts_chunk_1.jsonl",
                "num_posts": 5
            },
            {
                "job_id": "batches/test-job-2", 
                "raw_posts_file": "temp_posts_chunk_2.jsonl",
                "num_posts": 3
            }
        ]
    }

@pytest.mark.asyncio
class TestBatchJobManager:
    """Test suite for BatchJobManager functionality."""

    async def test_submit_new_jobs_single_chunk(self, mock_app_config, sample_competitor_config, sample_posts, mock_api_connector, mocker, tmp_path):
        """Tests submitting jobs when posts fit in single chunk."""
        # Mock workspace creation
        mocker.patch('os.makedirs')
        mocker.patch('os.rename')
        
        # Mock file operations
        mock_file = mocker.patch('builtins.open', mock_open())
        mocker.patch('src.transform.batch_manager.os.path.join', return_value=str(tmp_path / "test.jsonl"))
        
        # Mock job saving
        mock_save_jobs = mocker.patch.object(BatchJobManager, '_save_pending_jobs')
        mock_save_raw = mocker.patch.object(BatchJobManager, '_save_raw_posts', return_value=str(tmp_path / "posts.jsonl"))
        
        manager = BatchJobManager(mock_app_config)
        
        await manager.submit_new_jobs(
            competitor=sample_competitor_config,
            posts=sample_posts,
            batch_model="gemini-2.0-flash-lite",
            app_config=mock_app_config,
            source_raw_filepath="test_source.json",
            wait=False
        )

        # Should create one batch job
        mock_api_connector.create_batch_job.assert_called_once()
        mock_save_jobs.assert_called_once()
        
        # Verify job tracking data
        call_args = mock_save_jobs.call_args[0]
        job_list = call_args[1]  # job_tracking_list argument
        assert len(job_list) == 1

    async def test_submit_new_jobs_multiple_chunks(self, mock_app_config, sample_competitor_config, mock_api_connector, mocker, tmp_path):
        """Tests submitting jobs when posts need multiple chunks."""
        # Create posts that will be split into chunks
        large_posts = [{'title': f'Post {i}', 'url': f'https://test.com/post{i}', 'content': 'x' * 1000} for i in range(20)]
        
        # Mock chunking to return multiple chunks
        mocker.patch.object(BatchJobManager, '_split_posts_into_chunks', return_value=[large_posts[:10], large_posts[10:]])
        mocker.patch('os.makedirs')
        mocker.patch('os.rename')
        mocker.patch.object(BatchJobManager, '_save_raw_posts', side_effect=[f"chunk_{i}.jsonl" for i in range(2)])
        mock_save_jobs = mocker.patch.object(BatchJobManager, '_save_pending_jobs')
        
        manager = BatchJobManager(mock_app_config)
        
        await manager.submit_new_jobs(
            competitor=sample_competitor_config,
            posts=large_posts,
            batch_model="gemini-2.0-flash-lite",
            app_config=mock_app_config,
            source_raw_filepath="test_source.json",
            wait=False
        )

        # Should create two batch jobs
        assert mock_api_connector.create_batch_job.call_count == 2
        
        # Verify job tracking includes both jobs
        call_args = mock_save_jobs.call_args[0]
        job_list = call_args[1]
        assert len(job_list) == 2

    async def test_check_and_load_results_all_succeeded(self, mock_app_config, sample_competitor_config, mock_pending_jobs, mock_api_connector, mocker):
        """Tests checking and loading results when all jobs succeeded."""
        # Mock file existence and content
        mocker.patch('os.path.exists', return_value=True)
        mocker.patch('builtins.open', mock_open(read_data=json.dumps(mock_pending_jobs)))
        
        # Mock job status checking
        mock_api_connector.check_batch_job.return_value = "JOB_STATE_SUCCEEDED"
        
        # Mock job status summary
        mocker.patch('src.utils.get_job_status_summary', return_value=("All jobs succeeded", True))
        
        # Mock result consolidation
        mock_consolidate = mocker.patch.object(BatchJobManager, 'consolidate_results', new_callable=AsyncMock)
        mock_consolidate.return_value = [{'title': 'Test Result'}]
        
        mock_cleanup = mocker.patch.object(BatchJobManager, '_cleanup_workspace')

        manager = BatchJobManager(mock_app_config)
        
        result = await manager.check_and_load_results(sample_competitor_config, mock_app_config)

        mock_consolidate.assert_called_once()
        mock_cleanup.assert_called_once()
        assert result == [{'title': 'Test Result'}]

    async def test_check_and_load_results_jobs_pending(self, mock_app_config, sample_competitor_config, mock_pending_jobs, mock_api_connector, mocker):
        """Tests checking results when jobs are still pending."""
        mocker.patch('os.path.exists', return_value=True)
        mocker.patch('builtins.open', mock_open(read_data=json.dumps(mock_pending_jobs)))
        
        # Mock mixed job statuses
        mock_api_connector.check_batch_job.side_effect = ["JOB_STATE_SUCCEEDED", "JOB_STATE_RUNNING"]
        mocker.patch('src.utils.get_job_status_summary', return_value=("1/2 jobs completed", False))
        
        mock_consolidate = mocker.patch.object(BatchJobManager, 'consolidate_results', new_callable=AsyncMock)

        manager = BatchJobManager(mock_app_config)
        
        result = await manager.check_and_load_results(sample_competitor_config, mock_app_config)

        # Should not consolidate results when jobs are pending
        mock_consolidate.assert_not_called()
        assert result is None

    async def test_check_and_load_results_no_pending_jobs(self, mock_app_config, sample_competitor_config, mocker):
        """Tests checking results when no pending jobs file exists."""
        mocker.patch('os.path.exists', return_value=False)

        manager = BatchJobManager(mock_app_config)
        
        result = await manager.check_and_load_results(sample_competitor_config, mock_app_config)

        assert result is None

    async def test_consolidate_results_success(self, mock_app_config, sample_competitor_config, mock_pending_jobs, mock_api_connector, mocker):
        """Tests successful result consolidation."""
        # Mock file operations
        mocker.patch('os.path.exists', return_value=True)
        mocker.patch('builtins.open', mock_open(read_data='{"title": "Test Post"}'))
        
        # Mock API download
        mock_api_connector.download_batch_results.return_value = [
            {'title': 'Enriched Post', 'summary': 'Test summary', 'seo_keywords': 'test, keywords'}
        ]
        
        # Mock performance tracking
        mocker.patch('src.utils.update_performance_log')
        
        # Mock content preprocessor merge
        mocker.patch('src.content_preprocessor.ContentPreprocessor.merge_chunked_results', side_effect=lambda x: x)

        manager = BatchJobManager(mock_app_config)
        
        result = await manager.consolidate_results(
            sample_competitor_config, 
            mock_pending_jobs['jobs'], 
            mock_app_config, 
            "source_file.json"
        )

        assert len(result) == len(mock_pending_jobs['jobs'])  # One result per job
        mock_api_connector.download_batch_results.assert_called()

    def test_split_posts_into_chunks_small_posts(self, mock_app_config):
        """Tests chunking when posts are small enough for single chunk."""
        small_posts = [{'content': 'Small content'} for _ in range(5)]
        
        manager = BatchJobManager(mock_app_config)
        chunks = manager._split_posts_into_chunks(small_posts)
        
        assert len(chunks) == 1
        assert len(chunks[0]) == 5

    def test_split_posts_into_chunks_large_posts(self, mock_app_config):
        """Tests chunking when posts exceed size limits."""
        # Create posts that will exceed size limit
        large_content = 'x' * (50 * 1024 * 1024)  # 50MB content
        large_posts = [{'content': large_content} for _ in range(3)]
        
        manager = BatchJobManager(mock_app_config)
        chunks = manager._split_posts_into_chunks(large_posts)
        
        # Should split into multiple chunks
        assert len(chunks) > 1

    def test_save_raw_posts(self, mock_app_config, sample_posts, mocker, tmp_path):
        """Tests saving raw posts to JSONL file."""
        mocker.patch('os.makedirs')
        mock_file_path = tmp_path / "test_posts.jsonl"
        mocker.patch('os.path.join', return_value=str(mock_file_path))
        
        mock_file = mock_open()
        mocker.patch('builtins.open', mock_file)

        manager = BatchJobManager(mock_app_config)
        result = manager._save_raw_posts(sample_posts, "test_competitor", chunk_num=1)

        assert result is not None
        mock_file.assert_called_once()

    def test_save_pending_jobs(self, mock_app_config, mocker, tmp_path):
        """Tests saving pending job information."""
        job_list = [{"job_id": "test-job", "raw_posts_file": "test.jsonl", "num_posts": 5}]
        
        mocker.patch('os.makedirs')
        mocker.patch('os.path.join', return_value=str(tmp_path / "pending_jobs.json"))
        
        mock_file = mock_open()
        mocker.patch('builtins.open', mock_file)

        manager = BatchJobManager(mock_app_config)
        manager._save_pending_jobs("test_competitor", job_list, "source.json")

        mock_file.assert_called_once()

    def test_cleanup_workspace(self, mock_app_config, mock_pending_jobs, mocker):
        """Tests workspace cleanup after successful processing."""
        mocker.patch('os.path.exists', return_value=True)
        mocker.patch('os.remove')

        manager = BatchJobManager(mock_app_config)
        manager._cleanup_workspace(sample_competitor_config, mock_pending_jobs['jobs'])

        # Should remove job files and pending jobs file
        expected_removes = len(mock_pending_jobs['jobs']) + 1  # job files + pending_jobs.json
        assert mocker.patch('os.remove').call_count >= expected_removes