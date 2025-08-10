# tests/test_transform_live.py
# This file contains unit tests for the live (async) processing logic.

import pytest
import json
from unittest.mock import MagicMock, AsyncMock, PropertyMock

# Import the module to test
from src.transform import live

pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing the live transformation."""
    return [
        {'title': 'Live Test Post 1', 'publication_date': '2025-08-10', 'content': 'Content 1.'},
        {'title': 'Live Test Post 2', 'publication_date': '2025-08-09', 'content': 'Content 2.'}
    ]

async def test_transform_posts_live_success(mocker, mock_posts):
    """
    Tests a successful live transformation using the client.aio pattern.
    """
    # 1. Prepare the mock response
    mock_api_response = MagicMock()
    mock_api_response.text = json.dumps({
        "summary": "This is a mock summary.",
        "seo_keywords": ["live_keyword1", "live_keyword2"]
    })

    # 2. Mock the client.aio structure
    mock_async_models_service = MagicMock()
    # The async method must be mocked with AsyncMock
    mock_async_models_service.generate_content = AsyncMock(return_value=mock_api_response)

    # Use a PropertyMock to attach the .aio attribute which holds our async service
    mock_client = MagicMock()
    type(mock_client).aio = PropertyMock(return_value=MagicMock(models=mock_async_models_service))
    
    # 3. Patch the Client class to return our fully configured mock
    mocker.patch.object(live.genai, 'Client', return_value=mock_client)

    # 4. Run the function
    model_name_to_test = "gemini-2.0-flash"
    transformed_posts = await live.transform_posts_live(mock_posts, model_name_to_test)

    # 5. Assert the results
    assert len(transformed_posts) == 2
    mock_async_models_service.generate_content.assert_called() # Check that the async method was called
    
    assert transformed_posts[0]['summary'] == "This is a mock summary."
    assert transformed_posts[0]['seo_keywords'] == "live_keyword1, live_keyword2"

async def test_transform_posts_live_api_error(mocker, mock_posts, caplog):
    """
    Tests that the live transformation handles an API error gracefully.
    """
    # 1. Mock the client.aio structure to raise an error
    mock_async_models_service = MagicMock()
    mock_async_models_service.generate_content = AsyncMock(side_effect=Exception("Async API Error"))
    
    mock_client = MagicMock()
    type(mock_client).aio = PropertyMock(return_value=MagicMock(models=mock_async_models_service))
    
    # 2. Patch the client
    mocker.patch.object(live.genai, 'Client', return_value=mock_client)

    # 3. Run the function
    transformed_posts = await live.transform_posts_live(mock_posts, "gemini-2.0-flash")

    # 4. Assert the results
    assert len(transformed_posts) == 2
    assert transformed_posts[0]['summary'] == "N/A"
    assert "Failed to process API response" in caplog.text
    assert "Async API Error" in caplog.text