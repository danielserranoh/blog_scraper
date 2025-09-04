# tests/test_orchestrator.py
# This file contains comprehensive tests for the high-level orchestration logic.

import pytest
import logging
from unittest.mock import MagicMock, AsyncMock

# Import the orchestrator module to test its functions
from src.orchestrator import run_pipeline
from src.exceptions import (
    ETLError, 
    ScrapingError, 
    EnrichmentError, 
    StateError,
    ConfigurationError,
    ExportError,
    BatchJobError
)

@pytest.mark.asyncio
class TestOrchestrator:
    """Comprehensive test suite for the orchestrator workflow management."""

    # =============================================================================
    # SCRAPE WORKFLOW TESTS
    # =============================================================================

    async def test_run_pipeline_scrape_workflow_success(self, mock_di_container, mock_args_scrape, sample_posts, caplog):
        """Tests successful scrape workflow execution."""
        # Setup mocks for successful scraping
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'scraped_data.json'

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(mock_args_scrape)

        # Verify the workflow was executed correctly
        mock_di_container.scraper_manager.scrape_and_return_posts.assert_called_once_with(
            mock_di_container.get_competitors_to_process.return_value[0],
            30,  # days
            False  # scrape_all
        )
        mock_di_container.state_manager.save_raw_data.assert_called_once_with(
            sample_posts, 
            'test_competitor'
        )

        # Verify result structure
        assert result['success'] is True
        assert result['operation'] == 'scrape'
        assert result['posts_scraped'] == len(sample_posts)

        # Verify logging
        assert "Starting scrape-only process" in caplog.text

    async def test_run_pipeline_scrape_workflow_no_posts_found(self, mock_di_container, mock_args_scrape, caplog):
        """Tests scrape workflow when no posts are found."""
        # Setup mocks for no posts found
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = None

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(mock_args_scrape)

        # Should still succeed but with 0 posts
        assert result['success'] is True
        assert result['operation'] == 'scrape'
        assert result['posts_scraped'] == 0

        # Should not attempt to save data
        mock_di_container.state_manager.save_raw_data.assert_not_called()

        # Verify logging
        assert "No new posts found" in caplog.text

    async def test_run_pipeline_scrape_workflow_scraper_exception(self, mock_di_container, mock_args_scrape):
        """Tests scrape workflow when scraper raises exception."""
        # Setup scraper to raise exception
        mock_di_container.scraper_manager.scrape_and_return_posts.side_effect = Exception("Network error")

        result = await run_pipeline(mock_args_scrape)

        # Should return structured error
        assert result['error'] is True
        assert result['error_code'] == 'SCRAPING_ERROR'
        assert 'Network error' in result['message']
        assert 'test_competitor' in result['details']['competitors']

    async def test_run_pipeline_scrape_workflow_state_save_failure(self, mock_di_container, mock_args_scrape, sample_posts):
        """Tests scrape workflow when state saving fails."""
        # Setup successful scraping but failed saving
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = None

        result = await run_pipeline(mock_args_scrape)

        # Should return structured error
        assert result['error'] is True
        assert result['error_code'] == 'STATE_ERROR'
        assert 'Failed to save raw data' in result['message']

    async def test_run_pipeline_scrape_workflow_all_competitors(self, mock_di_container, mock_competitor_config, sample_posts):
        """Tests scrape workflow processing all competitors."""
        # Setup args without specific competitor
        args = {
            'days': 30, 'all': False, 'competitor': None, 'scrape': True,
            'enrich': False, 'enrich_raw': False, 'check_job': False,
            'export': None, 'wait': False, 'get_posts': False
        }

        # Setup multiple competitors
        mock_di_container.get_competitors_to_process.return_value = mock_competitor_config['competitors']
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'test_file.json'

        result = await run_pipeline(args)

        # Should process both competitors
        assert mock_di_container.scraper_manager.scrape_and_return_posts.call_count == 2
        assert mock_di_container.state_manager.save_raw_data.call_count == 2
        assert result['posts_scraped'] == len(sample_posts) * 2

    # =============================================================================
    # GET-POSTS WORKFLOW TESTS (Full Pipeline)
    # =============================================================================

    async def test_run_pipeline_get_posts_workflow_success(self, mock_di_container, mock_args_get_posts, sample_posts, sample_enriched_posts, caplog):
        """Tests successful full pipeline execution (scrape + enrich + save)."""
        # Setup mocks for full pipeline
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'raw_file.json'
        mock_di_container.enrichment_manager.enrich_posts.return_value = sample_enriched_posts
        mock_di_container.state_manager.save_processed_data.return_value = 'processed_file.json'

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(mock_args_get_posts)

        # Verify the full pipeline was executed in order
        mock_di_container.scraper_manager.scrape_and_return_posts.assert_called_once()
        mock_di_container.state_manager.save_raw_data.assert_called_once_with(sample_posts, 'test_competitor')
        
        # Verify enrichment was called with correct parameters
        enrichment_call = mock_di_container.enrichment_manager.enrich_posts.call_args
        assert enrichment_call[1]['competitor']['name'] == 'test_competitor'
        assert enrichment_call[1]['posts_to_enrich'] == sample_posts
        assert enrichment_call[1]['all_posts_for_merge'] == sample_posts
        assert enrichment_call[1]['source_raw_filepath'] == 'raw_file.json'
        
        mock_di_container.state_manager.save_processed_data.assert_called_once_with(
            sample_enriched_posts, 
            'test_competitor', 
            'raw_file.json'
        )

        # Verify result structure
        assert result['success'] is True
        assert result['operation'] == 'get_posts'
        assert result['posts_processed'] == len(sample_enriched_posts)

        # Verify logging
        assert "Starting full pipeline" in caplog.text
        assert "Completed processing" in caplog.text

    async def test_run_pipeline_get_posts_workflow_no_scraping_results(self, mock_di_container, mock_args_get_posts, caplog):
        """Tests get-posts workflow when scraping returns no results."""
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = None

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(mock_args_get_posts)

        # Should succeed but with 0 posts processed
        assert result['success'] is True
        assert result['posts_processed'] == 0

        # Enrichment should not be called if no posts scraped
        mock_di_container.enrichment_manager.enrich_posts.assert_not_called()
        
        # Verify logging
        assert "No new posts found" in caplog.text

    async def test_run_pipeline_get_posts_workflow_enrichment_returns_none(self, mock_di_container, mock_args_get_posts, sample_posts):
        """Tests get-posts workflow when enrichment returns None (batch mode)."""
        # Setup successful scraping but enrichment returns None (batch processing)
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'raw_file.json'
        mock_di_container.enrichment_manager.enrich_posts.return_value = None

        result = await run_pipeline(mock_args_get_posts)

        # Should succeed but with 0 posts processed (batch mode)
        assert result['success'] is True
        assert result['posts_processed'] == 0

        # Raw data should still be saved
        mock_di_container.state_manager.save_raw_data.assert_called_once()
        
        # Processed data should not be saved when enrichment returns None
        mock_di_container.state_manager.save_processed_data.assert_not_called()

    async def test_run_pipeline_get_posts_workflow_with_wait_flag(self, mock_di_container, sample_posts, sample_enriched_posts):
        """Tests get-posts workflow with wait flag enabled."""
        args = {
            'days': 30, 'all': False, 'competitor': 'test_competitor', 'scrape': False,
            'enrich': False, 'enrich_raw': False, 'check_job': False,
            'export': None, 'wait': True, 'get_posts': True
        }

        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'raw_file.json'
        mock_di_container.enrichment_manager.enrich_posts.return_value = sample_enriched_posts

        result = await run_pipeline(args)

        # Verify wait flag was passed to enrichment
        enrichment_call = mock_di_container.enrichment_manager.enrich_posts.call_args
        assert enrichment_call[1]['wait'] is True

        assert result['success'] is True

    # =============================================================================
    # ENRICH WORKFLOW TESTS
    # =============================================================================

    async def test_run_pipeline_enrich_workflow_success(self, mock_di_container, mock_args_enrich, sample_posts, sample_enriched_posts, caplog):
        """Tests successful enrich workflow for existing processed data."""
        # Setup mocks for enrichment workflow
        mock_di_container.enrichment_manager._find_posts_to_enrich.return_value = (sample_posts, sample_posts)
        mock_di_container.enrichment_manager.enrich_posts.return_value = sample_enriched_posts

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(mock_args_enrich)

        # Verify enrichment workflow
        mock_di_container.enrichment_manager._find_posts_to_enrich.assert_called_once_with('test_competitor')
        mock_di_container.enrichment_manager.enrich_posts.assert_called_once()
        mock_di_container.state_manager.save_processed_data.assert_called_once_with(
            sample_enriched_posts, 
            'test_competitor', 
            'enrichment_update.json'
        )

        # Verify result
        assert result['success'] is True
        assert result['operation'] == 'enrich'
        assert result['posts_enriched'] == len(sample_enriched_posts)

        # Verify logging
        assert "Will enrich" in caplog.text

    async def test_run_pipeline_enrich_workflow_no_posts_need_enrichment(self, mock_di_container, mock_args_enrich, sample_enriched_posts, caplog):
        """Tests enrich workflow when no posts need enrichment."""
        # Setup: all posts already enriched
        mock_di_container.enrichment_manager._find_posts_to_enrich.return_value = (sample_enriched_posts, [])

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(mock_args_enrich)

        # Should not call enrichment if no posts need it
        mock_di_container.enrichment_manager.enrich_posts.assert_not_called()
        mock_di_container.state_manager.save_processed_data.assert_not_called()

        # Should still succeed
        assert result['success'] is True
        assert result['posts_enriched'] == 0

        # Verify logging
        assert "No posts found that require enrichment" in caplog.text

    async def test_run_pipeline_enrich_raw_workflow_success(self, mock_di_container, sample_posts, sample_enriched_posts):
        """Tests successful enrich-raw workflow for raw data."""
        args = {
            'competitor': 'test_competitor', 'enrich': False, 'enrich_raw': True,
            'check_job': False, 'export': None, 'wait': False,
            'get_posts': False, 'scrape': False, 'days': None, 'all': False
        }

        # Setup: raw data exists, some not processed yet
        mock_di_container.state_manager.load_raw_data.return_value = sample_posts
        mock_di_container.state_manager.load_processed_data.return_value = []
        mock_di_container.state_manager.get_latest_raw_filepath.return_value = 'latest_raw.json'
        mock_di_container.enrichment_manager.enrich_posts.return_value = sample_enriched_posts

        result = await run_pipeline(args)

        # Verify raw data processing
        mock_di_container.state_manager.load_raw_data.assert_called_once_with('test_competitor')
        mock_di_container.state_manager.load_processed_data.assert_called_once_with('test_competitor')
        mock_di_container.enrichment_manager.enrich_posts.assert_called_once()

        assert result['success'] is True
        assert result['operation'] == 'enrich_raw'

    async def test_run_pipeline_enrich_raw_workflow_all_already_processed(self, mock_di_container, sample_enriched_posts, caplog):
        """Tests enrich-raw workflow when all raw posts are already processed."""
        args = {
            'competitor': 'test_competitor', 'enrich': False, 'enrich_raw': True,
            'check_job': False, 'export': None, 'wait': False,
            'get_posts': False, 'scrape': False, 'days': None, 'all': False
        }

        # Setup: raw data exists but all already processed
        mock_di_container.state_manager.load_raw_data.return_value = sample_enriched_posts
        mock_di_container.state_manager.load_processed_data.return_value = sample_enriched_posts

        with caplog.at_level(logging.INFO):
            result = await run_pipeline(args)

        # Should not enrich if all already processed
        mock_di_container.enrichment_manager.enrich_posts.assert_not_called()

        assert result['success'] is True
        assert result['posts_enriched'] == 0

        # Verify logging
        assert "All raw posts" in caplog.text and "already been processed" in caplog.text

    # =============================================================================
    # CHECK-JOB WORKFLOW TESTS
    # =============================================================================

    async def test_run_pipeline_check_job_workflow_success(self, mock_di_container, mock_args_check_job, sample_enriched_posts):
        """Tests successful check-job workflow."""
        mock_di_container.batch_manager.check_and_load_results.return_value = sample_enriched_posts

        result = await run_pipeline(mock_args_check_job)

        mock_di_container.batch_manager.check_and_load_results.assert_called_once_with(
            mock_di_container.get_competitors_to_process.return_value[0],
            mock_di_container.app_config
        )

        assert result['success'] is True
        assert result['operation'] == 'check_job'
        assert result['results_count'] == len(sample_enriched_posts)

    async def test_run_pipeline_check_job_workflow_no_results(self, mock_di_container, mock_args_check_job):
        """Tests check-job workflow when no results are available."""
        mock_di_container.batch_manager.check_and_load_results.return_value = None

        result = await run_pipeline(mock_args_check_job)

        assert result['success'] is True
        assert result['results_count'] == 0

    async def test_run_pipeline_check_job_workflow_multiple_competitors(self, mock_di_container, mock_competitor_config, sample_enriched_posts):
        """Tests check-job workflow with multiple competitors."""
        args = {
            'competitor': None, 'check_job': True, 'enrich': False, 'enrich_raw': False,
            'export': None, 'wait': False, 'get_posts': False, 'scrape': False
        }

        mock_di_container.get_competitors_to_process.return_value = mock_competitor_config['competitors']
        mock_di_container.batch_manager.check_and_load_results.return_value = sample_enriched_posts

        result = await run_pipeline(args)

        # Should check jobs for both competitors
        assert mock_di_container.batch_manager.check_and_load_results.call_count == 2
        assert result['results_count'] == len(sample_enriched_posts) * 2

    # =============================================================================
    # EXPORT WORKFLOW TESTS
    # =============================================================================

    async def test_run_pipeline_export_workflow_success(self, mock_di_container, mock_args_export):
        """Tests successful export workflow."""
        result = await run_pipeline(mock_args_export)

        # Should check jobs first, then export
        mock_di_container.batch_manager.check_and_load_results.assert_called_once()
        mock_di_container.export_manager.run_export_process.assert_called_once_with(
            mock_di_container.get_competitors_to_process.return_value,
            'json',
            mock_di_container.app_config
        )

        assert result['success'] is True
        assert result['operation'] == 'export'
        assert result['format'] == 'json'

    async def test_run_pipeline_export_workflow_different_formats(self, mock_di_container):
        """Tests export workflow with different formats."""
        formats = ['csv', 'md', 'txt', 'gsheets']
        
        for fmt in formats:
            args = {
                'competitor': 'test_competitor', 'export': True, 'export_format': fmt,
                'enrich': False, 'enrich_raw': False, 'check_job': False,
                'wait': False, 'get_posts': False, 'scrape': False
            }

            result = await run_pipeline(args)

            assert result['success'] is True
            assert result['format'] == fmt

    # =============================================================================
    # ERROR HANDLING AND EXCEPTION TESTS
    # =============================================================================

    async def test_run_pipeline_configuration_error_no_competitors(self, mock_di_container):
        """Tests handling when no competitors are found."""
        mock_di_container.get_competitors_to_process.return_value = []

        result = await run_pipeline({'competitor': 'nonexistent', 'scrape': True})

        assert result['error'] is True
        assert result['error_code'] == 'CONFIG_ERROR'
        assert 'No competitors found' in result['message']

    async def test_run_pipeline_configuration_error_invalid_command(self, mock_di_container):
        """Tests handling when no valid command is specified."""
        args = {
            'competitor': 'test_competitor', 'scrape': False, 'enrich': False,
            'enrich_raw': False, 'check_job': False, 'export': None,
            'get_posts': False, 'wait': False
        }

        result = await run_pipeline(args)

        assert result['error'] is True
        assert result['error_code'] == 'CONFIG_ERROR'
        assert 'No valid command specified' in result['message']
        assert 'available_commands' in result['details']

    async def test_run_pipeline_structured_exception_handling(self, mock_di_container, mock_args_scrape):
        """Tests that ETL exceptions are properly structured for LLM consumption."""
        # Setup enrichment manager to raise a structured exception
        mock_di_container.scraper_manager.scrape_and_return_posts.side_effect = ScrapingError(
            "Test scraping error",
            competitor="test_competitor",
            urls=["https://test.com/failed"],
            details={"retry_count": 3}
        )

        result = await run_pipeline(mock_args_scrape)

        # Verify structured error response
        assert result['error'] is True
        assert result['error_code'] == 'SCRAPING_ERROR'
        assert result['message'] == 'Test scraping error'
        assert result['details']['competitor'] == 'test_competitor'
        assert result['details']['failed_urls'] == ["https://test.com/failed"]
        assert result['details']['retry_count'] == 3
        assert result['error_type'] == 'ScrapingError'

    async def test_run_pipeline_unexpected_exception_handling(self, mock_di_container, mock_args_scrape, caplog):
        """Tests handling of unexpected (non-ETL) exceptions."""
        # Setup to raise unexpected exception
        mock_di_container.scraper_manager.scrape_and_return_posts.side_effect = ValueError("Unexpected error")

        with caplog.at_level(logging.ERROR):
            result = await run_pipeline(mock_args_scrape)

        # Should wrap in generic ETL error
        assert result['error'] is True
        assert result['error_code'] == 'PIPELINE_ERROR'
        assert 'Unexpected pipeline error' in result['message']
        assert 'ValueError' in result['message']

        # Should log the error
        assert "Unexpected error in pipeline" in caplog.text

    async def test_run_pipeline_enrichment_error_propagation(self, mock_di_container, mock_args_enrich):
        """Tests proper propagation of enrichment errors."""
        mock_di_container.enrichment_manager._find_posts_to_enrich.return_value = ([], [])
        mock_di_container.enrichment_manager.enrich_posts.side_effect = EnrichmentError(
            "API timeout",
            posts_count=5,
            model="gemini-2.0-flash",
            details={"timeout_duration": 30}
        )

        result = await run_pipeline(mock_args_enrich)

        assert result['error'] is True
        assert result['error_code'] == 'ENRICHMENT_ERROR'
        assert result['details']['posts_count'] == 5
        assert result['details']['model'] == "gemini-2.0-flash"
        assert result['details']['timeout_duration'] == 30

    # =============================================================================
    # COMPLEX WORKFLOW INTEGRATION TESTS
    # =============================================================================

    async def test_run_pipeline_competitor_filtering_logic(self, mock_di_container, mock_competitor_config):
        """Tests that competitor filtering works correctly for different scenarios."""
        # Test with specific competitor
        args_specific = {'competitor': 'test_competitor', 'scrape': True}
        await run_pipeline(args_specific)
        
        call_args = mock_di_container.get_competitors_to_process.call_args[0]
        assert call_args[0] == 'test_competitor'

        # Test with no competitor (should process all)
        args_all = {'competitor': None, 'scrape': True}
        await run_pipeline(args_all)
        
        call_args = mock_di_container.get_competitors_to_process.call_args[0]
        assert call_args[0] is None

    async def test_run_pipeline_parameter_passing_consistency(self, mock_di_container, sample_posts, sample_enriched_posts):
        """Tests that parameters are passed consistently through the pipeline."""
        args = {
            'days': 15, 'all': True, 'competitor': 'test_competitor',
            'wait': True, 'get_posts': True, 'scrape': False, 'enrich': False,
            'enrich_raw': False, 'check_job': False, 'export': None
        }

        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'raw_file.json'
        mock_di_container.enrichment_manager.enrich_posts.return_value = sample_enriched_posts

        result = await run_pipeline(args)

        # Verify scraper parameters - the actual implementation passes days even when all=True
        scraper_call = mock_di_container.scraper_manager.scrape_and_return_posts.call_args
        assert scraper_call[0][1] == 15  # days parameter is passed as-is
        assert scraper_call[0][2] is True  # scrape_all

        # Verify enrichment parameters - check positional arguments based on actual signature
        enrichment_call = mock_di_container.enrichment_manager.enrich_posts.call_args
        assert enrichment_call[0][7] is True  # wait parameter (8th position, index 7)
        assert enrichment_call[0][3] == 10  # batch_threshold (4th position, index 3)
        assert enrichment_call[0][4] == 'gemini-2.0-flash'  # live_model (5th position, index 4)
        assert enrichment_call[0][5] == 'gemini-2.0-flash-lite'  # batch_model (6th position, index 5)

        assert result['success'] is True

    async def test_run_pipeline_state_consistency_across_operations(self, mock_di_container, sample_posts, sample_enriched_posts):
        """Tests that state is managed consistently across different operations."""
        # Test get-posts workflow for state consistency
        mock_di_container.scraper_manager.scrape_and_return_posts.return_value = sample_posts
        mock_di_container.state_manager.save_raw_data.return_value = 'test_raw_file.json'
        mock_di_container.enrichment_manager.enrich_posts.return_value = sample_enriched_posts

        args = {
            'competitor': 'test_competitor', 'get_posts': True, 'days': 30,
            'all': False, 'wait': False, 'scrape': False, 'enrich': False,
            'enrich_raw': False, 'check_job': False, 'export': None
        }

        result = await run_pipeline(args)

        # Verify state operations are called in correct order
        assert mock_di_container.state_manager.save_raw_data.called
        assert mock_di_container.state_manager.save_processed_data.called

        # Verify processed data save uses the raw filename
        processed_save_call = mock_di_container.state_manager.save_processed_data.call_args
        assert processed_save_call[0][2] == 'test_raw_file.json'  # source_filename

        assert result['success'] is True