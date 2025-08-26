# tests/test_state_manager.py

import pytest
import os
import json
from unittest.mock import MagicMock
from datetime import datetime

from src.state_management.json_adapter import JsonAdapter

@pytest.fixture
def mock_posts():
    """Returns a list of mock posts for testing."""
    return [
        {'title': 'Test Post 1', 'url': 'https://example.com/p1', 'content': 'Content 1.', 'headings': [{'tag': 'h2', 'text': 'Intro'}]},
        {'title': 'Test Post 2', 'url': 'https://example.com/p2', 'content': 'Content 2.', 'headings': []}
    ]

@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory structure for testing."""
    raw_dir = tmp_path / "data" / "raw" / "test_competitor"
    raw_dir.mkdir(parents=True)
    processed_dir = tmp_path / "data" / "processed" / "test_competitor"
    processed_dir.mkdir(parents=True)
    return tmp_path

def test_json_adapter_save(temp_dir, mock_posts):
    """Tests that the JsonAdapter correctly saves data to a file."""
    adapter = JsonAdapter()
    
    # We no longer need to mock os.path.join.
    # The temp_dir fixture provides the path directly.
    filepath = adapter.save(mock_posts, "test_competitor", "raw")
    
    assert os.path.exists(filepath)
    with open(filepath, 'r') as f:
        data = json.load(f)
        assert len(data) == len(mock_posts)
        assert data[0]['title'] == mock_posts[0]['title']

def test_json_adapter_read(temp_dir, mock_posts, mocker):
    """Tests that the JsonAdapter correctly reads data from a file."""
    adapter = JsonAdapter()
    
    # The file path is correctly managed by the fixture.
    filepath = os.path.join(temp_dir, "data", "raw", "test_competitor", "testfile.json")
    with open(filepath, 'w') as f:
        json.dump(mock_posts, f)
        
    mocker.patch('os.listdir', return_value=['testfile.json'])
    mocker.patch('os.path.isdir', return_value=True)
    mocker.patch('builtins.open', mocker.mock_open(read_data=json.dumps(mock_posts)))
    
    posts = adapter.read("test_competitor", "raw")
    
    assert len(posts) == len(mock_posts)
    assert posts[0]['title'] == mock_posts[0]['title']