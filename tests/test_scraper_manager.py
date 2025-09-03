# tests/test_scraper_manager.py
# This file contains unit tests for the ScraperManager logic.

import pytest
from unittest.mock import MagicMock, AsyncMock
import logging

from src.extract.scraper_manager import ScraperManager
from src.exceptions import ScrapingError

# Asynchronous generator to correctly mock the extract_posts_in_batches function
async def async_gen(items):
    for item in items:
        yield item

@pytest.mark.asyncio
class TestScraperManager:
    """Test suite for ScraperManager functionality."""

    async def test_scrape_and_return_posts_success(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mocker):
        """Tests successful scraping and post return."""
        # Mock the extract_posts_in_batches to return an async generator
        mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([sample_posts]))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        result = await manager.scrape_and_return_posts(sample_competitor_config, 30, False)

        mock_state_manager.load_raw_urls.assert_called_once_with("test_competitor")
        assert result == sample_posts

    async def test_scrape_and_return_posts_no_posts(self, mock_app_config, sample_competitor_config, mock_state_manager, mocker):
        """Tests handling when no posts are found during scraping."""
        # Mock extract_posts_in_batches to return an empty async generator
        mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([]))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        result = await manager.scrape_and_return_posts(sample_competitor_config, 30, False)

        mock_state_manager.load_raw_urls.assert_called_once_with("test_competitor")
        assert result is None

    async def test_scrape_and_return_posts_with_existing_urls(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mocker):
        """Tests that existing URLs are properly passed to the extraction process."""
        existing_urls = {'https://test.com/existing1', 'https://test.com/existing2'}
        mock_state_manager.load_raw_urls.return_value = existing_urls
        
        mock_extract = mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([sample_posts]))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        result = await manager.scrape_and_return_posts(sample_competitor_config, None, True)

        # Verify extract was called with existing URLs
        call_args = mock_extract.call_args
        assert call_args[0][4] == existing_urls  # existing_urls parameter
        assert result == sample_posts

    async def test_scrape_and_return_posts_uses_batch_size_from_config(self, mock_state_manager, sample_competitor_config, sample_posts, mocker):
        """Tests that batch size is taken from app config."""
        app_config = {'batch_threshold': 25}
        
        mock_extract = mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([sample_posts]))
        
        manager = ScraperManager(app_config, mock_state_manager)
        
        await manager.scrape_and_return_posts(sample_competitor_config, 30, False)

        # Verify batch_size parameter is from config
        call_args = mock_extract.call_args
        assert call_args[0][3] == 25  # batch_size parameter

    async def test_scrape_and_return_posts_handles_exceptions(self, mock_app_config, sample_competitor_config, mock_state_manager, mocker):
        """Tests that scraping exceptions are properly handled and wrapped."""
        # Mock extract_posts_in_batches to raise an exception
        mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', side_effect=Exception("Network error"))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        with pytest.raises(ScrapingError) as exc_info:
            await manager.scrape_and_return_posts(sample_competitor_config, 30, False)

        assert "Network error" in str(exc_info.value)
        assert exc_info.value.error_code == "SCRAPING_ERROR"
        assert exc_info.value.details['competitor'] == "test_competitor"

    async def test_scrape_and_return_posts_logs_appropriately(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mocker, caplog):
        """Tests that appropriate logging occurs during scraping."""
        mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([sample_posts]))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        with caplog.at_level(logging.INFO):
            result = await manager.scrape_and_return_posts(sample_competitor_config, 30, False)

        assert "Starting scrape for 'test_competitor'" in caplog.text
        assert f"Successfully scraped {len(sample_posts)} posts" in caplog.text

    async def test_scrape_and_return_posts_parameter_mapping(self, mock_app_config, sample_competitor_config, sample_posts, mock_state_manager, mocker):
        """Tests that parameters are correctly mapped to extract function."""
        mock_extract = mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([sample_posts]))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        await manager.scrape_and_return_posts(sample_competitor_config, 15, True)

        call_args = mock_extract.call_args
        assert call_args[0][0] == sample_competitor_config  # competitor
        assert call_args[0][1] == 15  # days
        assert call_args[0][2] is True  # scrape_all

    async def test_scrape_and_return_posts_multiple_batches(self, mock_app_config, sample_competitor_config, mock_state_manager, mocker):
        """Tests handling of multiple batches from extraction."""
        batch1 = [{'title': 'Post 1', 'url': 'https://test.com/1'}]
        batch2 = [{'title': 'Post 2', 'url': 'https://test.com/2'}]
        
        mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([batch1, batch2]))
        
        manager = ScraperManager(mock_app_config, mock_state_manager)
        
        result = await manager.scrape_and_return_posts(sample_competitor_config, 30, False)

        # Should combine all batches
        assert len(result) == 2
        assert result[0]['title'] == 'Post 1'
        assert result[1]['title'] == 'Post 2'