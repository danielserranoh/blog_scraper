# tests/test_scraper_manager.py
# This file contains unit tests for the high-level ScraperManager logic.

import pytest
from unittest.mock import MagicMock, AsyncMock
import logging

# Import the manager and its dependencies to mock
from src.extract.scraper_manager import ScraperManager
from src.extract import extract_posts_in_batches

@pytest.fixture
def mock_app_config():
    """Provides a mock application configuration."""
    return {"batch_threshold": 10, "storage": {"adapter": "csv"}}

@pytest.fixture
def mock_competitor_config():
    """Provides a mock competitor configuration."""
    return {"name": "test_competitor"}

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing."""
    return [
        {'title': 'Post 1', 'url': 'https://example.com/p1', 'content': 'Content 1.'},
        {'title': 'Post 2', 'url': 'https://example.com/p2', 'content': 'Content 2.'}
    ]

# Asynchronous generator to correctly mock the extract_posts_in_batches function
async def async_gen(items):
    for item in items:
        yield item

@pytest.mark.asyncio
async def test_run_scrape_and_submit_success(mocker, mock_app_config, mock_competitor_config, mock_posts, mock_state_manager, mock_enrichment_manager):
    """
    Tests that a successful scrape results in calls to save raw data
    and trigger the enrichment process.
    """
    # Mock the extract_posts_in_batches to return an async generator
    mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([mock_posts]))
    
    mock_state_manager.save_raw_data.return_value = "data/raw/test_competitor_timestamp.csv"
    mock_enrichment_manager.enrich_posts.return_value = AsyncMock()

    manager = ScraperManager(mock_app_config)
    await manager.run_scrape_and_submit(mock_competitor_config, 30, False, 10, "live_model", "batch_model", mock_app_config)

    # Assertions
    mock_state_manager.save_raw_data.assert_called_once_with(mock_posts, "test_competitor")
    mock_enrichment_manager.enrich_posts.assert_called_once()
    
@pytest.mark.asyncio
async def test_run_scrape_and_submit_no_posts(mocker, mock_app_config, mock_competitor_config, mock_state_manager, mock_enrichment_manager):
    """
    Tests that if no posts are scraped, no further actions are taken.
    """
    # Mock extract_posts_in_batches to return an empty async generator
    mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([]))
    
    manager = ScraperManager(mock_app_config)
    await manager.run_scrape_and_submit(mock_competitor_config, 30, False, 10, "live_model", "batch_model", mock_app_config)

    # Assertions
    mock_state_manager.save_raw_data.assert_not_called()
    mock_enrichment_manager.enrich_posts.assert_not_called()

@pytest.mark.asyncio
async def test_run_scrape_and_submit_save_fail(mocker, mock_app_config, mock_competitor_config, mock_posts, caplog, mock_state_manager, mock_enrichment_manager):
    """
    Tests that the process aborts gracefully if saving the raw data fails.
    """
    # Mock extract_posts_in_batches to return an async generator
    mocker.patch('src.extract.scraper_manager.extract_posts_in_batches', return_value=async_gen([mock_posts]))
    
    mock_state_manager.save_raw_data.return_value = None

    manager = ScraperManager(mock_app_config)
    with caplog.at_level(logging.ERROR):
        await manager.run_scrape_and_submit(mock_competitor_config, 30, False, 10, "live_model", "batch_model", mock_app_config)
    
    # Assertions
    mock_state_manager.save_raw_data.assert_called_once_with(mock_posts, "test_competitor")
    mock_enrichment_manager.enrich_posts.assert_not_called()
    assert "Failed to save raw data" in caplog.text