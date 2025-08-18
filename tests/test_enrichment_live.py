# tests/test_enrichment_live.py
# This file contains unit tests for the live (async) processing logic.

import pytest
import json
from unittest.mock import MagicMock, AsyncMock, PropertyMock

# Import the module to test and the connector to mock
from src.transform import live
from src.api_connector import GeminiAPIConnector

pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing the live transformation."""
    return [
        {'title': 'Live Test Post 1', 'publication_date': '2025-08-10', 'content': 'Content 1.'},
        {'title': 'Live Test Post 2', 'publication_date': '2025-08-09', 'content': 'Content 2.'}
    ]

@pytest.fixture
def mock_api_connector(mocker):
    """Mocks the GeminiAPIConnector for isolated testing of the manager."""
    mock_connector = MagicMock(spec=GeminiAPIConnector)
    # The 'client' attribute is set during the real class's __init__; we need to mock it.
    mock_connector.client = MagicMock()
    mocker.patch('src.transform.live.GeminiAPIConnector', return_value=mock_connector)
    return mock_connector

async def test_transform_posts_live_success(mocker, mock_posts, mock_api_connector):
    """
    Tests a successful live transformation using the connector.
    """
    mock_api_connector.batch_enrich_posts_live = AsyncMock(return_value=[
        {'title': 'Live Test Post 1', 'publication_date': '2025-08-10', 'content': 'Content 1.', 'summary': 'Mock summary 1', 'seo_keywords': 'mock1, mock2'},
        {'title': 'Live Test Post 2', 'publication_date': '2025-08-09', 'content': 'Content 2.', 'summary': 'Mock summary 2', 'seo_keywords': 'mock3, mock4'}
    ])
    
    # Run the function
    model_name_to_test = "gemini-2.0-flash"
    transformed_posts = await live.transform_posts_live(mock_posts, model_name_to_test)

    # Assert the results
    assert len(transformed_posts) == 2
    mock_api_connector.batch_enrich_posts_live.assert_called_once()
    
    assert transformed_posts[0]['summary'] == "Mock summary 1"
    assert transformed_posts[0]['seo_keywords'] == "mock1, mock2"