# conftest.py
# This file is used to configure pytest and define shared fixtures.

import sys
import os
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from types import SimpleNamespace

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.di_container import DIContainer
from src.extract.scraper_manager import ScraperManager
from src.transform.enrichment_manager import EnrichmentManager
from src.transform.batch_manager import BatchJobManager
from src.load.export_manager import ExportManager
from src.state_management.state_manager import StateManager
from src.api_connector import GeminiAPIConnector
from src.content_preprocessor import ContentPreprocessor

# Test Data Fixtures
@pytest.fixture
def sample_competitor_config():
    """Provides a realistic competitor configuration for testing."""
    return {
        'name': 'test_competitor',
        'base_url': 'https://test.com',
        'structure_pattern': 'single_list',
        'pagination_pattern': {
            'type': 'numeric_query',
            'query_param': 'page'
        },
        'category_paths': ['blog/'],
        'post_list_selector': '.post-link',
        'date_selector': 'time[datetime]',
        'content_selector': '.post-content'
    }

@pytest.fixture
def sample_posts():
    """Returns sample posts with realistic data."""
    return [
        {
            'title': 'Test Post 1',
            'url': 'https://test.com/post1',
            'content': 'This is test content for the first post.',
            'publication_date': '2025-01-01',
            'seo_meta_keywords': 'test, content',
            'headings': [{'tag': 'h2', 'text': 'Introduction'}],
            'schemas': []
        },
        {
            'title': 'Test Post 2',
            'url': 'https://test.com/post2',
            'content': 'This is test content for the second post.',
            'publication_date': '2025-01-02',
            'seo_meta_keywords': 'test, example',
            'headings': [{'tag': 'h2', 'text': 'Overview'}],
            'schemas': []
        }
    ]

@pytest.fixture
def sample_enriched_posts():
    """Returns sample posts with enrichment data."""
    return [
        {
            'title': 'Test Post 1',
            'url': 'https://test.com/post1',
            'content': 'This is test content for the first post.',
            'publication_date': '2025-01-01',
            'summary': 'A comprehensive overview of test content.',
            'seo_keywords': 'test, content, overview',
            'funnel_stage': 'ToFu',
            'enrichment_status': 'completed'
        }
    ]

@pytest.fixture
def sample_long_content_post():
    """Returns a post with very long content to test chunking."""
    long_content = "This is a very long post content. " * 500  # ~17,000 characters
    return {
        'title': 'Long Content Post',
        'url': 'https://test.com/long-post',
        'content': long_content,
        'publication_date': '2025-01-01',
        'seo_meta_keywords': 'long, content',
        'headings': [],
        'schemas': []
    }

@pytest.fixture
def mock_app_config():
    """Provides a mock application configuration."""
    return {
        'batch_threshold': 10,
        'models': {
            'live': 'gemini-2.0-flash',
            'batch': 'gemini-2.0-flash-lite'
        },
        'storage': {
            'adapter': 'json'
        },
        'processed_data': {
            'adapter': 'json'
        }
    }

@pytest.fixture
def mock_competitor_config():
    """Provides mock competitor configuration."""
    return {
        'competitors': [
            {
                'name': 'test_competitor',
                'base_url': 'https://test.com',
                'structure_pattern': 'single_list'
            }
        ]
    }

# Manager Mocks
@pytest.fixture
def mock_di_container(mocker, mock_app_config, mock_competitor_config):
    """Mocks the DIContainer with proper configuration."""
    mock_container = MagicMock(spec=DIContainer)
    mock_container.app_config = mock_app_config
    mock_container.competitor_config = mock_competitor_config
    mock_container.get_competitors_to_process.return_value = mock_competitor_config['competitors']
    mock_container.get_models.return_value = ('gemini-2.0-flash', 'gemini-2.0-flash-lite')
    mock_container.get_batch_threshold.return_value = 10
    
    # Mock manager properties
    mock_container.state_manager = MagicMock()
    mock_container.scraper_manager = MagicMock()
    mock_container.enrichment_manager = MagicMock()
    mock_container.batch_manager = MagicMock()
    mock_container.export_manager = MagicMock()
    
    mocker.patch('src.orchestrator.DIContainer', return_value=mock_container)
    return mock_container

@pytest.fixture
def mock_state_manager(mocker):
    """Mocks the StateManager class and its methods."""
    mock_manager = MagicMock(spec=StateManager)
    mock_manager.load_raw_urls.return_value = set()
    mock_manager.load_raw_data.return_value = []
    mock_manager.load_processed_data.return_value = []
    mock_manager.save_raw_data.return_value = "test_file.json"
    mock_manager.save_processed_data.return_value = "processed_file.json"
    mock_manager.get_latest_raw_filepath.return_value = "/test/path.json"
    
    mocker.patch('src.state_management.state_manager.StateManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_scraper_manager(mocker):
    """Mocks the ScraperManager class."""
    mock_manager = MagicMock(spec=ScraperManager)
    mock_manager.scrape_and_return_posts = AsyncMock(return_value=None)
    
    mocker.patch('src.extract.scraper_manager.ScraperManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_enrichment_manager(mocker):
    """Mocks the EnrichmentManager class."""
    mock_manager = MagicMock(spec=EnrichmentManager)
    mock_manager.enrich_posts = AsyncMock(return_value=None)
    mock_manager._find_posts_to_enrich.return_value = ([], [])
    
    mocker.patch('src.transform.enrichment_manager.EnrichmentManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_batch_manager(mocker):
    """Mocks the BatchJobManager class."""
    mock_manager = MagicMock(spec=BatchJobManager)
    mock_manager.check_and_load_results = AsyncMock(return_value=None)
    mock_manager.submit_new_jobs = AsyncMock(return_value=None)
    
    mocker.patch('src.transform.batch_manager.BatchJobManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_export_manager(mocker):
    """Mocks the ExportManager class."""
    mock_manager = MagicMock(spec=ExportManager)
    mock_manager.run_export_process = MagicMock()
    
    mocker.patch('src.load.export_manager.ExportManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_api_connector(mocker):
    """Mocks the GeminiAPIConnector for isolated testing."""
    mock_connector = MagicMock(spec=GeminiAPIConnector)
    mock_connector.client = MagicMock()
    mock_connector.enrich_post_live = AsyncMock(return_value=("Test summary", "test, keywords", "ToFu"))
    mock_connector.batch_enrich_posts_live = AsyncMock(return_value=[])
    mock_connector.create_batch_job = MagicMock(return_value="batches/mock-job-id")
    mock_connector.check_batch_job = MagicMock(return_value="JOB_STATE_SUCCEEDED")
    mock_connector.download_batch_results = MagicMock(return_value=[])
    
    mocker.patch('src.api_connector.GeminiAPIConnector', return_value=mock_connector)
    mocker.patch('src.transform.live.GeminiAPIConnector', return_value=mock_connector)
    return mock_connector

@pytest.fixture
def mock_content_preprocessor(mocker):
    """Mocks the ContentPreprocessor class."""
    mock_preprocessor = MagicMock(spec=ContentPreprocessor)
    mock_preprocessor.prepare_posts_for_enrichment = MagicMock(side_effect=lambda posts: posts)
    mock_preprocessor.merge_chunked_results = MagicMock(side_effect=lambda posts: posts)
    
    mocker.patch('src.content_preprocessor.ContentPreprocessor', mock_preprocessor)
    mocker.patch('src.transform.enrichment_manager.ContentPreprocessor', mock_preprocessor)
    return mock_preprocessor

# CLI Argument Fixtures
@pytest.fixture
def mock_args_scrape():
    """Provides mock CLI arguments for scrape command."""
    return {
        'days': 30,
        'all': False,
        'competitor': 'test_competitor',
        'scrape': True,
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'wait': False,
        'get_posts': False
    }

@pytest.fixture
def mock_args_get_posts():
    """Provides mock CLI arguments for get-posts command."""
    return {
        'days': 30,
        'all': False,
        'competitor': 'test_competitor',
        'scrape': False,
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'wait': False,
        'get_posts': True
    }

@pytest.fixture
def mock_args_enrich():
    """Provides mock CLI arguments for enrich command."""
    return {
        'competitor': 'test_competitor',
        'enrich': True,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'wait': False,
        'get_posts': False,
        'scrape': False
    }

# Utility Fixtures
@pytest.fixture
def temp_workspace(tmp_path):
    """Creates a temporary workspace directory."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return workspace

@pytest.fixture
def temp_data_dir(tmp_path):
    """Creates temporary data directories."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    raw_dir = data_dir / "raw" / "test_competitor"
    raw_dir.mkdir(parents=True)
    
    processed_dir = data_dir / "processed" / "test_competitor"  
    processed_dir.mkdir(parents=True)
    
    return data_dir