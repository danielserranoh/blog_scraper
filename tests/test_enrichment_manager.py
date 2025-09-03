# tests/test_enrichment_manager.py
# This file contains unit tests for the EnrichmentManager logic.

import pytest
from unittest.mock import MagicMock, AsyncMock

from src.transform.enrichment_manager import EnrichmentManager
from src.exceptions import EnrichmentError

@pytest.mark.asyncio
class TestEnrichmentManager:
    """Test suite for EnrichmentManager functionality."""

    async def test_enrich_posts_live_mode(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mock_batch_manager, mock_content_preprocessor, mocker):
        """Tests that posts below batch threshold use live mode."""
        # Mock live enrichment
        mock_transform_live = mocker.patch('src.transform.enrichment_manager.transform_posts_live', new_callable=AsyncMock)
        mock_transform_live.return_value = sample_posts

        manager = EnrichmentManager(mock_app_config, mock_state_manager, mock_batch_manager)
        
        result = await manager.enrich_posts(
            competitor=sample_competitor_config,
            posts_to_enrich=sample_posts,
            all_posts_for_merge=sample_posts,
            batch_threshold=10,  # Higher than sample_posts count
            live_model="gemini-2.0-flash",
            batch_model="gemini-2.0-flash-lite",
            wait=False,
            source_raw_filepath=None
        )

        # Should preprocess posts
        mock_content_preprocessor.prepare_posts_for_enrichment.assert_called_once_with(sample_posts)
        
        # Should call live enrichment
        mock_transform_live.assert_called_once()
        
        # Should not call batch manager
        mock_batch_manager.submit_new_jobs.assert_not_called()
        
        # Should merge results
        mock_content_preprocessor.merge_chunked_results.assert_called_once()
        
        assert result is not None

    async def test_enrich_posts_batch_mode(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mock_batch_manager, mock_content_preprocessor, mocker):
        """Tests that posts above batch threshold use batch mode."""
        # Mock live enrichment (should not be called)
        mock_transform_live = mocker.patch('src.transform.enrichment_manager.transform_posts_live', new_callable=AsyncMock)

        manager = EnrichmentManager(mock_app_config, mock_state_manager, mock_batch_manager)
        
        result = await manager.enrich_posts(
            competitor=sample_competitor_config,
            posts_to_enrich=sample_posts,
            all_posts_for_merge=sample_posts,
            batch_threshold=1,  # Lower than sample_posts count
            live_model="gemini-2.0-flash",
            batch_model="gemini-2.0-flash-lite",
            wait=False,
            source_raw_filepath="test_file.json"
        )

        # Should preprocess posts
        mock_content_preprocessor.prepare_posts_for_enrichment.assert_called_once_with(sample_posts)
        
        # Should call batch manager
        mock_batch_manager.submit_new_jobs.assert_called_once()
        
        # Should not call live enrichment
        mock_transform_live.assert_not_called()
        
        # Batch mode returns None (async processing)
        assert result is None

    async def test_enrich_posts_content_preprocessing_affects_routing(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mock_batch_manager, mock_content_preprocessor, mocker):
        """Tests that content preprocessing (chunking) can affect live vs batch routing."""
        # Mock preprocessing to return more items (simulating chunking)
        chunked_posts = sample_posts * 10  # 20 items
        mock_content_preprocessor.prepare_posts_for_enrichment.return_value = chunked_posts
        
        mock_transform_live = mocker.patch('src.transform.enrichment_manager.transform_posts_live', new_callable=AsyncMock)

        manager = EnrichmentManager(mock_app_config, mock_state_manager, mock_batch_manager)
        
        await manager.enrich_posts(
            competitor=sample_competitor_config,
            posts_to_enrich=sample_posts,  # Original 2 posts
            all_posts_for_merge=sample_posts,
            batch_threshold=10,  # Would normally be live mode
            live_model="gemini-2.0-flash",
            batch_model="gemini-2.0-flash-lite",
            wait=False,
            source_raw_filepath=None
        )

        # Should switch to batch mode due to chunking creating more items
        mock_batch_manager.submit_new_jobs.assert_called_once()
        mock_transform_live.assert_not_called()

    async def test_enrich_posts_handles_exceptions(self, mock_app_config, sample_competitor_config, sample_posts, mock_