# tests/test_extraction.py
# This file contains unit tests for the scraping patterns.

import pytest
import httpx
from unittest.mock import AsyncMock

# Import the specific scraper and stats object we want to test
from src.extract.blog_patterns import single_list
from src.extract._common import ScrapeStats

@pytest.fixture
def mock_stats():
    """A simple fixture to provide a stats object for tests."""
    return ScrapeStats()

@pytest.mark.asyncio
async def test_single_list_scraper_prevents_infinite_loop(mocker, mock_stats):
    """
    Tests that the single_list scraper correctly stops and prevents an infinite loop.
    """
    mock_config = {
        "name": "loop_test_site",
        "base_url": "https://loopy.com",
        "structure_pattern": "single_list",
        "pagination_pattern": { "type": "numeric_query", "query_param": "page" },
        "category_paths": ["blog"],
        "post_list_selector": "a.post",
        "next_page_selector": "a.next"
    }

    page_1_html = '<html><body><a class="post" href="/post1">Post 1</a><a class="next" href="/blog?page=2">Next</a></body></html>'
    page_2_html = '<html><body><h1>No more posts</h1></body></html>'

    # --- FIX: Provide a dummy request object to the mock responses ---
    mock_request = httpx.Request("GET", "https://loopy.com")
    mock_get = AsyncMock(side_effect=[
        httpx.Response(200, html=page_1_html, request=mock_request),
        httpx.Response(200, html=page_2_html, request=mock_request),
    ])
    mocker.patch('httpx.AsyncClient.get', mock_get)
    
    mocker.patch('src.extract.blog_patterns.single_list._get_post_details', new_callable=AsyncMock, return_value={"title": "Mock Post"})

    # Run the scraper by consuming the async generator
    scraped_posts = [post async for batch in single_list.scrape(mock_config, 30, False, 10, mock_stats) for post in batch]

    assert mock_stats.successful == 1
    assert mock_get.call_count == 2