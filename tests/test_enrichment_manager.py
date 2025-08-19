# tests/test_enrichment_manager.py
# This file contains unit tests for the high-level EnrichmentManager logic.

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import logging

# Import the manager and its dependencies to mock
from src.transform.enrichment_manager import EnrichmentManager
from src.state_management.state_manager import StateManager
from src.transform.batch_manager import BatchJobManager
from src.transform.live import transform_posts_live


@pytest.fixture
def mock_app_config():
    """Provides a mock application configuration."""
    return {"batch_threshold": 10, "storage": {"adapter": "csv"}}

@pytest.fixture
def mock_competitor_config():
    """Provides a mock competitor configuration."""
    return {"name": "test_competitor"}

@pytest.fixture
def mock_posts_needing_enrichment():
    """Returns a list of mock posts that need enrichment."""
    return [
        {'title': 'Post 1', 'url': 'https://example.com/p1', 'summary': 'N/A', 'seo_keywords': 'N/A', 'content': 'Content 1.'},
        {'title': 'Post 2', 'url': 'https://example.com/p2', 'summary': 'Existing Summary', 'seo_keywords': 'N/A', 'content': 'Content 2.'}
    ]

@pytest.fixture
def mock_fully_enriched_posts():
    """Returns a list of mock posts that are already enriched."""
    return [
        {'title': 'Post 3', 'url': 'https://example.com/p3', 'summary': 'Summary 3', 'seo_keywords': 'k3', 'content': 'Content 3.'},
    ]

@pytest.mark.asyncio
async def test_run_enrichment_process_finds_posts(mocker, mock_app_config, mock_competitor_config, mock_posts_needing_enrichment):
    """
    Tests that the enrichment process correctly finds and submits posts
    with missing enrichment data.
    """
    # Combine posts that need enrichment with already enriched posts
    all_posts = mock_posts_needing_enrichment + [
        {'title': 'Post 3', 'url': 'https://example.com/p3', 'summary': 'Summary 3', 'seo_keywords': 'k3'}
    ]
    
    # Mock the StateManager class and instance locally
    mock_state_manager_instance = mocker.Mock(spec=StateManager)
    mock_state_manager_instance.load_processed_data.return_value = all_posts
    mocker.patch('src.transform.enrichment_manager.StateManager', return_value=mock_state_manager_instance)
    
    # Mock the EnrichmentManager instance to isolate the run_enrichment_process method
    manager = EnrichmentManager(mock_app_config)
    manager.enrich_posts = AsyncMock()

    await manager.run_enrichment_process(mock_competitor_config, 5, "live_model", "batch_model", mock_app_config)
    
    # Assert that enrich_posts was called with only the posts that need enriching
    manager.enrich_posts.assert_called_once()
    args, _ = manager.enrich_posts.call_args
    posts_sent_for_enrichment = args[1]
    assert len(posts_sent_for_enrichment) == 2
    assert posts_sent_for_enrichment[0]['url'] == 'https://example.com/p1'
    assert posts_sent_for_enrichment[1]['url'] == 'https://example.com/p2'


@pytest.mark.asyncio
async def test_run_enrichment_process_no_posts_to_enrich(mocker, mock_app_config, mock_competitor_config, mock_fully_enriched_posts):
    """
    Tests that if no posts need enrichment, the process stops gracefully.
    """
    # Mock the StateManager class and instance locally
    mock_state_manager_instance = mocker.Mock(spec=StateManager)
    mock_state_manager_instance.load_processed_data.return_value = mock_fully_enriched_posts
    mocker.patch('src.transform.enrichment_manager.StateManager', return_value=mock_state_manager_instance)
    
    # Mock the EnrichmentManager instance to isolate the run_enrichment_process method
    manager = EnrichmentManager(mock_app_config)
    manager.enrich_posts = AsyncMock()

    await manager.run_enrichment_process(mock_competitor_config, 5, "live_model", "batch_model", mock_app_config)

    # Assert that enrich_posts was never called
    manager.enrich_posts.assert_not_called()
    

@pytest.mark.asyncio
async def test_enrich_posts_live_mode(mocker, mock_app_config, mock_competitor_config, mock_posts_needing_enrichment):
    """
    Tests that posts are submitted to live mode if below the batch threshold.
    """
    # Set a low batch threshold to force live mode
    mock_app_config['batch_threshold'] = 5
    
    # Mock the dependencies locally
    mocker.patch('src.transform.enrichment_manager.StateManager', autospec=True)
    mock_batch_manager_instance = mocker.Mock(spec=BatchJobManager)
    mocker.patch('src.transform.enrichment_manager.BatchJobManager', return_value=mock_batch_manager_instance)
    mock_transform_live = mocker.patch('src.transform.enrichment_manager.transform_posts_live', new_callable=AsyncMock)
    
    manager = EnrichmentManager(mock_app_config)
    await manager.enrich_posts(
        competitor=mock_competitor_config,
        posts=mock_posts_needing_enrichment,
        all_posts_from_file=mock_posts_needing_enrichment,
        batch_threshold=mock_app_config['batch_threshold'],
        live_model="live_model",
        batch_model="batch_model",
        app_config=mock_app_config
    )
    
    # Assert that live enrichment was called and batch was not
    mock_transform_live.assert_called_once()
    mock_batch_manager_instance.submit_new_jobs.assert_not_called()
    

@pytest.mark.asyncio
async def test_enrich_posts_batch_mode(mocker, mock_app_config, mock_competitor_config, mock_posts_needing_enrichment):
    """
    Tests that posts are submitted to batch mode if above the batch threshold.
    """
    # Set a high batch threshold to force batch mode
    mock_app_config['batch_threshold'] = 1
    
    # Mock the dependencies locally
    mocker.patch('src.transform.enrichment_manager.StateManager', autospec=True)
    mock_batch_manager_instance = mocker.Mock(spec=BatchJobManager)
    mock_batch_manager_instance.submit_new_jobs = AsyncMock()
    mocker.patch('src.transform.enrichment_manager.BatchJobManager', return_value=mock_batch_manager_instance)
    mock_transform_live = mocker.patch('src.transform.enrichment_manager.transform_posts_live', new_callable=AsyncMock)
    
    manager = EnrichmentManager(mock_app_config)
    await manager.enrich_posts(
        competitor=mock_competitor_config,
        posts=mock_posts_needing_enrichment,
        all_posts_from_file=mock_posts_needing_enrichment,
        batch_threshold=mock_app_config['batch_threshold'],
        live_model="live_model",
        batch_model="batch_model",
        app_config=mock_app_config
    )

    # Assert that batch enrichment was called and live was not
    mock_batch_manager_instance.submit_new_jobs.assert_called_once()
    mock_transform_live.assert_not_called()