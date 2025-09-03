# tests/test_state_manager.py
# This file contains unit tests for the state management system.

import pytest
import os
import json
from unittest.mock import MagicMock, mock_open
from datetime import datetime

from src.state_management.state_manager import StateManager
from src.state_management.json_adapter import JsonAdapter
from src.state_management.csv_adapter import CsvAdapter

class TestStateManager:
    """Test suite for StateManager functionality."""

    def test_init_with_json_adapter(self):
        """Tests StateManager initialization with JSON adapter."""
        config = {'storage': {'adapter': 'json'}}
        
        manager = StateManager(config)
        
        assert isinstance(manager.adapter, JsonAdapter)

    def test_init_with_csv_adapter(self):
        """Tests StateManager initialization with CSV adapter."""
        config = {'storage': {'adapter': 'csv'}}
        
        manager = StateManager(config)
        
        assert isinstance(manager.adapter, CsvAdapter)

    def test_init_with_invalid_adapter(self):
        """Tests StateManager initialization with invalid adapter."""
        config = {'storage': {'adapter': 'invalid'}}
        
        with pytest.raises(ValueError) as exc_info:
            StateManager(config)
        
        assert "Unknown storage adapter" in str(exc_info.value)

    def test_save_raw_data(self, mock_app_config, sample_posts):
        """Tests saving raw data through StateManager."""
        manager = StateManager(mock_app_config)
        
        # Mock the adapter
        manager.adapter = MagicMock()
        manager.adapter.save.return_value = "test_file_path.json"
        
        result = manager.save_raw_data(sample_posts, "test_competitor")
        
        manager.adapter.save.assert_called_once_with(sample_posts, "test_competitor", file_type='raw')
        assert result == "test_file_path.json"

    def test_save_processed_data(self, mock_app_config, sample_enriched_posts):
        """Tests saving processed data through StateManager."""
        manager = StateManager(mock_app_config)
        
        # Mock the adapter
        manager.adapter = MagicMock()
        manager.adapter.save.return_value = "processed_file.json"
        
        result = manager.save_processed_data(sample_enriched_posts, "test_competitor", "source_file.json")
        
        manager.adapter.save.assert_called_once_with(
            sample_enriched_posts, 
            "test_competitor", 
            file_type='processed', 
            source_filename="source_file.json"
        )
        assert result == "processed_file.json"

    def test_load_raw_data(self, mock_app_config, sample_posts):
        """Tests loading raw data through StateManager."""
        manager = StateManager(mock_app_config)
        
        # Mock the adapter
        manager.adapter = MagicMock()
        manager.adapter.read.return_value = sample_posts
        
        result = manager.load_raw_data("test_competitor")
        
        manager.adapter.read.assert_called_once_with("test_competitor", file_type='raw')
        assert result == sample_posts

    def test_load_processed_data(self, mock_app_config, sample_enriched_posts):
        """Tests loading processed data through StateManager."""
        manager = StateManager(mock_app_config)
        
        # Mock the adapter
        manager.adapter = MagicMock()
        manager.adapter.read.return_value = sample_enriched_posts
        
        result = manager.load_processed_data("test_competitor")
        
        manager.adapter.read.assert_called_once_with("test_competitor", file_type='processed')
        assert result == sample_enriched_posts

    def test_load_raw_urls(self, mock_app_config):
        """Tests loading raw URLs through StateManager."""
        expected_urls = {'https://test.com/post1', 'https://test.com/post2'}
        
        manager = StateManager(mock_app_config)
        
        # Mock the adapter
        manager.adapter = MagicMock()
        manager.adapter.read_urls.return_value = expected_urls
        
        result = manager.load_raw_urls("test_competitor")
        
        manager.adapter.read_urls.assert_called_once_with("test_competitor", file_type='raw')
        assert result == expected_urls

    def test_get_latest_raw_filepath(self, mock_app_config, mocker, tmp_path):
        """Tests getting the latest raw file path."""
        # Setup test directory structure
        competitor_dir = tmp_path / "data" / "raw" / "test_competitor"
        competitor_dir.mkdir(parents=True)
        
        # Create test files with different timestamps
        file1 = competitor_dir / "file1.json"
        file2 = competitor_dir / "file2.json"
        file1.write_text("{}")
        file2.write_text("{}")
        
        # Mock os.path.getctime to return different times
        mocker.patch('os.path.getctime', side_effect=lambda x: 100 if 'file1' in x else 200)
        mocker.patch('os.path.isdir', return_value=True)
        mocker.patch('os.listdir', return_value=['file1.json', 'file2.json'])
        
        manager = StateManager(mock_app_config)
        
        result = manager.get_latest_raw_filepath("test_competitor")
        
        # Should return the newer file (file2)
        assert 'file2.json' in result

    def test_get_latest_raw_filepath_no_directory(self, mock_app_config, mocker):
        """Tests getting latest raw file path when directory doesn't exist."""
        mocker.patch('os.path.isdir', return_value=False)
        
        manager = StateManager(mock_app_config)
        
        result = manager.get_latest_raw_filepath("test_competitor")
        
        assert result is None

    def test_get_latest_raw_filepath_empty_directory(self, mock_app_config, mocker):
        """Tests getting latest raw file path from empty directory."""
        mocker.patch('os.path.isdir', return_value=True)
        mocker.patch('os.listdir', return_value=[])
        
        manager = StateManager(mock_app_config)
        
        result = manager.get_latest_raw_filepath("test_competitor")
        
        assert result is None


class TestJsonAdapter:
    """Test suite for JsonAdapter functionality."""

    def test_save_raw_data_success(self, sample_posts, mocker, tmp_path):
        """Tests successful saving of raw data to JSON."""
        mocker.patch('os.makedirs')
        mocker.patch('os.path.join', return_value=str(tmp_path / "test_file.json"))
        
        mock_file = mock_open()
        mocker.patch('builtins.open', mock_file)
        mocker.patch('json.dump')
        
        adapter = JsonAdapter()
        
        result = adapter.save(sample_posts, "test_competitor", "raw")
        
        assert result is not None
        mock_file.assert_called_once()

    def test_save_processed_data_success(self, sample_enriched_posts, mocker, tmp_path):
        """Tests successful saving of processed data to JSON."""
        mocker.patch('os.makedirs')
        mocker.patch('os.path.join', return_value=str(tmp_path / "processed_file.json"))
        
        mock_file = mock_open()
        mocker.patch('builtins.open', mock_file)
        mocker.patch('json.dump')
        
        adapter = JsonAdapter()
        
        result = adapter.save(sample_enriched_posts, "test_competitor", "processed", source_filename="source.json")
        
        assert result is not None
        mock_file.assert_called_once()

    def test_save_no_posts(self, mocker):
        """Tests saving when no posts are provided."""
        adapter = JsonAdapter()
        
        result = adapter.save([], "test_competitor", "raw")
        
        assert result is None

    def test_save_io_error(self, sample_posts, mocker, tmp_path):
        """Tests handling of IO errors during save."""
        mocker.patch('os.makedirs')
        mocker.patch('os.path.join', return_value=str(tmp_path / "test_file.json"))
        mocker.patch('builtins.open', side_effect=IOError("Disk full"))
        
        adapter = JsonAdapter()
        
        result = adapter.save(sample_posts, "test_competitor", "raw")
        
        assert result is None

    def test_read_success(self, sample_posts, mocker):
        """Tests successful reading of JSON data."""
        mocker.patch('os.path.isdir', return_value=True)
        mocker.patch('os.listdir', return_value=['file1.json', 'file2.json'])
        mocker.patch('builtins.open', mock_open())
        mocker.patch('json.load', return_value=sample_posts)
        
        adapter = JsonAdapter()
        
        result = adapter.read("test_competitor", "raw")
        
        # Should return posts for each file (doubled)
        assert len(result) == len(sample_posts) * 2

    def test_read_no_directory(self, mocker):
        """Tests reading when directory doesn't exist."""
        mocker.patch('os.path.isdir', return_value=False)
        
        adapter = JsonAdapter()
        
        result = adapter.read("test_competitor", "raw")
        
        assert result == []

    def test_read_json_decode_error(self, mocker):
        """Tests handling of JSON decode errors during read."""
        mocker.patch('os.path.isdir', return_value=True)
        mocker.patch('os.listdir', return_value=['corrupt.json'])
        mocker.patch('builtins.open', mock_open())
        mocker.patch('json.load', side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
        
        adapter = JsonAdapter()
        
        result = adapter.read("test_competitor", "raw")
        
        assert result == []

    def test_read_urls_success(self, mocker):
        """Tests successful reading of URLs from JSON files."""
        test_data = [
            {'url': 'https://test.com/post1', 'title': 'Post 1'},
            {'url': 'https://test.com/post2', 'title': 'Post 2'}
        ]
        
        mocker.patch('os.path.isdir', return_value=True)
        mocker.patch('os.listdir', return_value=['file.json'])
        mocker.patch('builtins.open', mock_open())
        mocker.patch('json.load', return_value=test_data)
        
        adapter = JsonAdapter()
        
        result = adapter.read_urls("test_competitor", "raw")
        
        expected_urls = {'https://test.com/post1', 'https://test.com/post2'}
        assert result == expected_urls

    def test_read_urls_no_directory(self, mocker):
        """Tests reading URLs when directory doesn't exist."""
        mocker.patch('os.path.isdir', return_value=False)
        
        adapter = JsonAdapter()
        
        result = adapter.read_urls("test_competitor", "raw")
        
        assert result == set()

    def test_read_urls_missing_url_field(self, mocker):
        """Tests reading URLs when some posts don't have URL field."""
        test_data = [
            {'url': 'https://test.com/post1', 'title': 'Post 1'},
            {'title': 'Post 2'},  # Missing URL
            {'url': '', 'title': 'Post 3'}  # Empty URL
        ]
        
        mocker.patch('os.path.isdir', return_value=True)
        mocker.patch('os.listdir', return_value=['file.json'])
        mocker.patch('builtins.open', mock_open())
        mocker.patch('json.load', return_value=test_data)
        
        adapter = JsonAdapter()
        
        result = adapter.read_urls("test_competitor", "raw")
        
        # Should only include valid URLs
        assert result == {'https://test.com/post1'}