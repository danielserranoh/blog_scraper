# tests/test_orchestrator.py
# This file contains unit tests for the high-level orchestration logic.

import pytest
from unittest.mock import MagicMock, AsyncMock

# Import the orchestrator module to test its functions
from src.orchestrator import run_pipeline

@pytest.mark.asyncio
class TestOrchestrator:
    """Test suite for the orchestrator workflow management."""

    async def test_run_pipeline_scrape_workflow(self, mock_di_container, mock_args_scrape):
        """Tests that the orchestrator calls the scraper manager for scrape command."""
        # Setup mocks
        mock_di_container.scraper_manager.scrape_and_return_posts = AsyncMock(return_value=[{'test': 'post'}])
        mock_di_container.state_manager.save_raw_data = MagicMock(return_value='test_path.json')

        result = await run_pipeline(mock_args_scrape)

        mock_di_container.scraper_manager.scrape_and_return_posts.assert_called_once()
        mock_di_container.state_manager.save_raw_data.assert_called_once()
        assert result['success'] is True
        assert result['operation'] == 'scrape'

    async def test_run_pipeline_get_posts_workflow(self, mock_di_container, mock_args_get_posts, sample_posts):
        """Tests that the orchestrator handles full get-posts pipeline."""
        # Setup mocks
        mock_di_container.scraper_manager.scrape_and_return_posts = AsyncMock(return_value=sample_posts)
        mock_di_container.state_manager.save_raw_data = MagicMock(return_value='raw_file.json')
        mock_di_container.enrichment_manager.enrich_posts = AsyncMock(return_value=sample_posts)

        result = await run_pipeline(mock_args_get_posts)

        # Verify the full pipeline was executed
        mock_di_container.scraper_manager.scrape_and_return_posts.assert_called_once()
        mock_di_container.state_manager.save_raw_data.assert_called_once()
        mock_di_container.enrichment_manager.enrich_posts.assert_called_once()
        mock_di_container.state_manager.save_processed_data.assert_called_once()
        
        assert result['success'] is True
        assert result['operation'] == 'get_posts'
        assert result['posts_processed'] == len(sample_posts)

    async def test_run_pipeline_enrich_workflow(self, mock_di_container, mock_args_enrich, sample_enriched_posts):
        """Tests that the orchestrator handles enrich command properly."""
        # Setup mocks  
        mock_di_container.enrichment_manager._find_posts_to_enrich.return_value = (sample_enriched_posts, sample_enriched_posts)
        mock_di_container.enrichment_manager.enrich_posts = AsyncMock(return_value=sample_enriched_posts)

        result = await run_pipeline(mock_args_enrich)

        mock_di_container.enrichment_manager._find_posts_to_enrich.assert_called_once()
        mock_di_container.enrichment_manager.enrich_posts.assert_called_once()
        mock_di_container.state_manager.save_processed_data.assert_called_once()
        
        assert result['success'] is True
        assert result['operation'] == 'enrich'

    async def test_run_pipeline_check_job_workflow(self, mock_di_container):
        """Tests that the orchestrator calls batch manager for check-job command."""
        args = {
            'competitor': 'test_competitor',
            'check_job': True,
            'enrich': False,
            'enrich_raw': False,
            'export': None,
            'scrape': False,
            'get_posts': False,
            'wait': False
        }

        mock_di_container.batch_manager.check_and_load_results = AsyncMock()