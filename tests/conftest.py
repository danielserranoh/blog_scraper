# conftest.py
# This file is used to configure pytest and define shared fixtures.

import sys
import os
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, mock_open
from types import SimpleNamespace
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.di_container import DIContainer
from src.extract.scraper_manager import ScraperManager
from src.transform.enrichment_manager import EnrichmentManager
from src.transform.batch_manager import BatchJobManager
from src.load.export_manager import ExportManager
from src.state_management.state_manager import StateManager
from src.api_connector import GeminiAPIConnector
from src.transform.content_preprocessor import ContentPreprocessor  # Fixed import path

# =============================================================================
# REALISTIC TEST DATA FIXTURES
# =============================================================================

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
        'date_strip_prefix': None,
        'content_selector': '.post-content',
        'content_filter_selector': None,
        'next_page_selector': 'a.next-page'
    }

@pytest.fixture
def sample_posts():
    """Returns sample posts with realistic data structure."""
    return [
        {
            'title': 'Test Post 1',
            'url': 'https://test.com/post1',
            'content': 'This is test content for the first post. It includes multiple sentences to test processing.',
            'publication_date': '2025-01-01',
            'seo_meta_keywords': 'test, content, blog',
            'headings': [
                {'tag': 'h1', 'text': 'Main Title'},
                {'tag': 'h2', 'text': 'Introduction'}
            ],
            'schemas': [
                {'@type': 'Article', 'headline': 'Test Post 1'}
            ],
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'funnel_stage': 'N/A',
            'enrichment_status': 'pending'
        },
        {
            'title': 'Test Post 2',
            'url': 'https://test.com/post2',
            'content': 'This is test content for the second post. It also contains comprehensive information.',
            'publication_date': '2025-01-02',
            'seo_meta_keywords': 'test, example, guide',
            'headings': [
                {'tag': 'h1', 'text': 'Second Post Title'},
                {'tag': 'h2', 'text': 'Overview'}
            ],
            'schemas': [],
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'funnel_stage': 'N/A',
            'enrichment_status': 'pending'
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
            'seo_meta_keywords': 'test, content, blog',
            'headings': [{'tag': 'h1', 'text': 'Main Title'}],
            'schemas': [{'@type': 'Article'}],
            'summary': 'A comprehensive overview of test content for beginners.',
            'seo_keywords': 'test, content, overview, beginners',
            'funnel_stage': 'ToFu',
            'enrichment_status': 'completed'
        },
        {
            'title': 'Test Post 2',
            'url': 'https://test.com/post2',
            'content': 'Advanced test content for experienced users.',
            'publication_date': '2025-01-02',
            'seo_meta_keywords': 'advanced, test, guide',
            'headings': [{'tag': 'h2', 'text': 'Advanced Topics'}],
            'schemas': [],
            'summary': 'Deep dive into advanced testing methodologies.',
            'seo_keywords': 'advanced, testing, methodologies, expert',
            'funnel_stage': 'MoFu',
            'enrichment_status': 'completed'
        }
    ]

@pytest.fixture
def sample_long_content_post():
    """Returns a post with very long content to test chunking."""
    # Create content that will definitely exceed chunking threshold
    long_content = ("This is a very long post content that will be used to test the chunking functionality. " * 200 + 
                   "It contains multiple sentences with proper punctuation. " * 100 +
                   "The content should be split intelligently at sentence boundaries when processed.")
    
    return {
        'title': 'Long Content Post',
        'url': 'https://test.com/long-post',
        'content': long_content,
        'publication_date': '2025-01-01',
        'seo_meta_keywords': 'long, content, chunking',
        'headings': [
            {'tag': 'h1', 'text': 'Very Long Article'},
            {'tag': 'h2', 'text': 'Section 1'},
            {'tag': 'h2', 'text': 'Section 2'}
        ],
        'schemas': [{'@type': 'Article', 'wordCount': len(long_content)}]
    }

@pytest.fixture
def sample_failed_enrichment_posts():
    """Returns posts with failed enrichment status for retry testing."""
    return [
        {
            'title': 'Failed Post 1',
            'url': 'https://test.com/failed1',
            'content': 'Content that failed to enrich previously.',
            'publication_date': '2025-01-01',
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'funnel_stage': 'N/A',
            'enrichment_status': 'failed'
        },
        {
            'title': 'Partial Post 2',
            'url': 'https://test.com/partial2',
            'content': 'Content with partial enrichment.',
            'publication_date': '2025-01-02',
            'summary': 'Good summary exists',
            'seo_keywords': 'N/A',  # Missing keywords
            'funnel_stage': 'N/A',  # Missing funnel stage
            'enrichment_status': 'pending'
        }
    ]

@pytest.fixture
def multilingual_content_post():
    """Post with multilingual content for edge case testing."""
    return {
        'title': 'Multilingual Test Post',
        'url': 'https://test.com/multilingual',
        'content': 'This post contains "smart quotes" and —em dashes— and…ellipsis characters that need cleaning.',
        'publication_date': '2025-01-01',
        'seo_meta_keywords': 'multilingual, unicode, testing',
        'headings': [],
        'schemas': []
    }

# =============================================================================
# CONFIGURATION FIXTURES
# =============================================================================

@pytest.fixture
def mock_app_config():
    """Provides a comprehensive mock application configuration."""
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
        },
        'google_sheets': {
            'spreadsheet_name': 'Test Blog Scraper Export'
        },
        'dxp_competitors': [
            'WordPress', 'Drupal', 'Sitecore'
        ],
        'prompts': {
            'enrichment_instruction': 'Test enrichment prompt with {content}'
        }
    }

@pytest.fixture
def mock_competitor_config():
    """Provides comprehensive mock competitor configuration."""
    return {
        'competitors': [
            {
                'name': 'test_competitor',
                'base_url': 'https://test.com',
                'structure_pattern': 'single_list',
                'pagination_pattern': {'type': 'numeric_query', 'query_param': 'page'},
                'category_paths': ['blog/'],
                'post_list_selector': '.post-link',
                'date_selector': 'time[datetime]',
                'content_selector': '.post-content'
            }
        ]
    }

@pytest.fixture
def mock_multiple_competitor_config():
    """Provides multiple competitors for testing multiple competitor scenarios."""
    return {
        'competitors': [
            {
                'name': 'test_competitor',
                'base_url': 'https://test.com',
                'structure_pattern': 'single_list',
                'pagination_pattern': {'type': 'numeric_query', 'query_param': 'page'},
                'category_paths': ['blog/'],
                'post_list_selector': '.post-link',
                'date_selector': 'time[datetime]',
                'content_selector': '.post-content'
            },
            {
                'name': 'secondary_competitor',
                'base_url': 'https://secondary.com',
                'structure_pattern': 'multi_category',
                'pagination_pattern': {'type': 'linked_path', 'selector': 'a.next'},
                'category_paths': ['blog/', 'news/'],
                'post_list_selector': '.article-link',
                'date_selector': '.date',
                'content_selector': '.article-content'
            }
        ]
    }

# =============================================================================
# REALISTIC MOCK FACTORIES
# =============================================================================

@pytest.fixture
def realistic_api_response_factory():
    """Factory for creating realistic Gemini API responses."""
    def _create_response(
        content_type="success", 
        summary="Test summary", 
        keywords=None, 
        funnel_stage="ToFu",
        error_type=None
    ):
        if keywords is None:
            keywords = ["test", "content", "keywords"]
        
        if error_type == "api_timeout":
            return None
        elif error_type == "invalid_json":
            return MagicMock(text="Invalid JSON response content")
        elif error_type == "afc_message":
            return MagicMock(text="AFC is enabled for this request")
        else:
            # Successful response
            response_data = {
                "summary": summary,
                "seo_keywords": keywords,
                "funnel_stage": funnel_stage
            }
            mock_response = MagicMock()
            mock_response.text = json.dumps(response_data)
            return mock_response
    
    return _create_response

@pytest.fixture
def batch_job_response_factory():
    """Factory for creating realistic batch job responses."""
    def _create_batch_response(job_state="JOB_STATE_SUCCEEDED", job_id="batches/test-job-123"):
        mock_job = MagicMock()
        mock_job.name = job_id
        mock_job.state.name = job_state
        
        if job_state == "JOB_STATE_SUCCEEDED":
            mock_job.dest.file_name = f"result-{job_id.split('/')[-1]}"
        
        return mock_job
    
    return _create_batch_response

@pytest.fixture
def async_http_context_manager(mocker):
    """Proper async context manager for HTTP operations."""
    async def _async_context():
        mock_client = MagicMock()
        mock_client.get = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        return mock_client
    
    return _async_context

# =============================================================================
# DI CONTAINER AND MANAGER MOCKS
# =============================================================================

@pytest.fixture
def mock_di_container(mocker, mock_app_config, mock_competitor_config):
    """Comprehensive DIContainer mock with proper manager initialization."""
    mock_container = MagicMock(spec=DIContainer)
    
    # Configuration properties
    mock_container.app_config = mock_app_config
    mock_container.competitor_config = mock_competitor_config
    
    # Configuration methods
    mock_container.get_competitors_to_process.return_value = mock_competitor_config['competitors']
    mock_container.get_models.return_value = ('gemini-2.0-flash', 'gemini-2.0-flash-lite')
    mock_container.get_batch_threshold.return_value = 10
    
    # Manager properties with proper specs
    mock_container.state_manager = MagicMock(spec=StateManager)
    mock_container.scraper_manager = MagicMock(spec=ScraperManager)
    mock_container.enrichment_manager = MagicMock(spec=EnrichmentManager)
    mock_container.batch_manager = MagicMock(spec=BatchJobManager)
    mock_container.export_manager = MagicMock(spec=ExportManager)
    mock_container.api_connector = MagicMock(spec=GeminiAPIConnector)
    
    # Setup default behaviors
    mock_container.state_manager.load_raw_urls.return_value = set()
    mock_container.state_manager.load_raw_data.return_value = []
    mock_container.state_manager.load_processed_data.return_value = []
    mock_container.state_manager.save_raw_data.return_value = "test_file.json"
    mock_container.state_manager.save_processed_data.return_value = "processed_file.json"
    mock_container.state_manager.get_latest_raw_filepath.return_value = "/test/path.json"
    
    # Setup async methods
    mock_container.scraper_manager.scrape_and_return_posts = AsyncMock()
    mock_container.enrichment_manager.enrich_posts = AsyncMock()
    mock_container.enrichment_manager._find_posts_to_enrich = MagicMock(return_value=([], []))
    mock_container.batch_manager.check_and_load_results = AsyncMock()
    mock_container.batch_manager.submit_new_jobs = AsyncMock()
    
    # Mock the DIContainer class instantiation
    mocker.patch('src.orchestrator.DIContainer', return_value=mock_container)
    
    return mock_container

@pytest.fixture
def mock_state_manager(mocker):
    """Enhanced StateManager mock with realistic behaviors."""
    mock_manager = MagicMock(spec=StateManager)
    
    # Default return values
    mock_manager.load_raw_urls.return_value = set()
    mock_manager.load_raw_data.return_value = []
    mock_manager.load_processed_data.return_value = []
    mock_manager.save_raw_data.return_value = "test_file.json"
    mock_manager.save_processed_data.return_value = "processed_file.json"
    mock_manager.get_latest_raw_filepath.return_value = "/test/path.json"
    
    # Add call tracking
    mock_manager.save_calls = []
    
    def track_save_calls(*args, **kwargs):
        mock_manager.save_calls.append((args, kwargs))
        return "saved_file.json"
    
    mock_manager.save_raw_data.side_effect = track_save_calls
    mock_manager.save_processed_data.side_effect = track_save_calls
    
    mocker.patch('src.state_management.state_manager.StateManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_scraper_manager(mocker):
    """Enhanced ScraperManager mock."""
    mock_manager = MagicMock(spec=ScraperManager)
    mock_manager.scrape_and_return_posts = AsyncMock(return_value=None)
    
    mocker.patch('src.extract.scraper_manager.ScraperManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_enrichment_manager(mocker):
    """Enhanced EnrichmentManager mock."""
    mock_manager = MagicMock(spec=EnrichmentManager)
    mock_manager.enrich_posts = AsyncMock(return_value=None)
    mock_manager._find_posts_to_enrich.return_value = ([], [])
    
    mocker.patch('src.transform.enrichment_manager.EnrichmentManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_batch_manager(mocker):
    """Enhanced BatchJobManager mock."""
    mock_manager = MagicMock(spec=BatchJobManager)
    mock_manager.check_and_load_results = AsyncMock(return_value=None)
    mock_manager.submit_new_jobs = AsyncMock(return_value=None)
    mock_manager.consolidate_results = AsyncMock(return_value=[])
    
    mocker.patch('src.transform.batch_manager.BatchJobManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_export_manager(mocker):
    """Enhanced ExportManager mock."""
    mock_manager = MagicMock(spec=ExportManager)
    mock_manager.run_export_process = MagicMock()
    
    mocker.patch('src.load.export_manager.ExportManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_api_connector(mocker):
    """Comprehensive GeminiAPIConnector mock with realistic behaviors."""
    mock_connector = MagicMock(spec=GeminiAPIConnector)
    
    # Setup client
    mock_connector.client = MagicMock()
    
    # Setup async methods with realistic return values
    mock_connector.enrich_post_live = AsyncMock(
        return_value=("Test summary", "test, keywords", "ToFu")
    )
    mock_connector.batch_enrich_posts_live = AsyncMock(return_value=[])
    
    # Setup batch methods
    mock_connector.create_batch_job = MagicMock(return_value="batches/mock-job-id")
    mock_connector.check_batch_job = MagicMock(return_value="JOB_STATE_SUCCEEDED")
    mock_connector.download_batch_results = MagicMock(return_value=[])
    mock_connector.list_batch_jobs = MagicMock(return_value=[])
    mock_connector.cancel_batch_job = MagicMock()
    mock_connector.delete_batch_job_file = MagicMock()
    
    # Mock in multiple locations where it might be imported
    mocker.patch('src.api_connector.GeminiAPIConnector', return_value=mock_connector)
    mocker.patch('src.transform.live.GeminiAPIConnector', return_value=mock_connector)
    
    return mock_connector

@pytest.fixture
def mock_content_preprocessor(mocker):
    """Enhanced ContentPreprocessor mock with realistic behaviors."""
    mock_preprocessor = MagicMock(spec=ContentPreprocessor)
    
    # Default behavior: return posts unchanged unless specified
    mock_preprocessor.prepare_posts_for_enrichment = MagicMock(side_effect=lambda posts: posts)
    mock_preprocessor.merge_chunked_results = MagicMock(side_effect=lambda posts: posts)
    
    # Add methods for testing chunking behavior
    def simulate_chunking(posts):
        """Simulates chunking for testing"""
        result = []
        for post in posts:
            if len(post.get('content', '')) > 6000:  # Simulate chunking threshold
                # Create 2 chunks
                for i in range(2):
                    chunk_post = post.copy()
                    chunk_post['title'] = f"{post['title']} (Part {i+1}/2)"
                    chunk_post['original_title'] = post['title']
                    chunk_post['chunk_index'] = i
                    chunk_post['total_chunks'] = 2
                    chunk_post['content_processing'] = {
                        'chunked': True,
                        'chunk_number': i + 1,
                        'total_chunks': 2
                    }
                    result.append(chunk_post)
            else:
                result.append(post)
        return result
    
    mock_preprocessor.simulate_chunking = simulate_chunking
    
    # Mock in the locations where it's imported
    mocker.patch('src.transform.content_preprocessor.ContentPreprocessor', mock_preprocessor)
    mocker.patch('src.transform.enrichment_manager.ContentPreprocessor', mock_preprocessor)
    
    return mock_preprocessor

# =============================================================================
# CLI ARGUMENT FIXTURES
# =============================================================================

@pytest.fixture
def mock_args_scrape():
    """Mock CLI arguments for scrape command."""
    return {
        'days': 30,
        'all': False,
        'competitor': 'test_competitor',
        'scrape': True,
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'export_format': None,
        'wait': False,
        'get_posts': False
    }

@pytest.fixture
def mock_args_get_posts():
    """Mock CLI arguments for get-posts command."""
    return {
        'days': 30,
        'all': False,
        'competitor': 'test_competitor',
        'scrape': False,
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'export_format': None,
        'wait': False,
        'get_posts': True
    }

@pytest.fixture
def mock_args_enrich():
    """Mock CLI arguments for enrich command."""
    return {
        'competitor': 'test_competitor',
        'enrich': True,
        'enrich_raw': False,
        'check_job': False,
        'export': None,
        'export_format': None,
        'wait': False,
        'get_posts': False,
        'scrape': False,
        'days': None,
        'all': False
    }

@pytest.fixture
def mock_args_export():
    """Mock CLI arguments for export command."""
    return {
        'competitor': 'test_competitor',
        'export': True,
        'export_format': 'json',
        'enrich': False,
        'enrich_raw': False,
        'check_job': False,
        'wait': False,
        'get_posts': False,
        'scrape': False,
        'days': None,
        'all': False
    }

@pytest.fixture
def mock_args_check_job():
    """Mock CLI arguments for check-job command."""
    return {
        'competitor': 'test_competitor',
        'check_job': True,
        'enrich': False,
        'enrich_raw': False,
        'export': None,
        'export_format': None,
        'wait': False,
        'get_posts': False,
        'scrape': False,
        'days': None,
        'all': False
    }

# =============================================================================
# UTILITY AND WORKSPACE FIXTURES
# =============================================================================

@pytest.fixture
def temp_workspace(tmp_path):
    """Creates a temporary workspace directory structure."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    
    # Create competitor subdirectories
    (workspace / "test_competitor").mkdir()
    (workspace / "secondary_competitor").mkdir()
    
    return workspace

@pytest.fixture
def temp_data_dir(tmp_path):
    """Creates comprehensive temporary data directories."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    # Create raw data directories
    raw_dir = data_dir / "raw"
    raw_dir.mkdir()
    (raw_dir / "test_competitor").mkdir()
    (raw_dir / "secondary_competitor").mkdir()
    
    # Create processed data directories
    processed_dir = data_dir / "processed"
    processed_dir.mkdir()
    (processed_dir / "test_competitor").mkdir()
    (processed_dir / "secondary_competitor").mkdir()
    
    return data_dir

@pytest.fixture
def temp_config_dir(tmp_path):
    """Creates temporary config directory with mock config files."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    
    # Create mock config.json
    config_json = {
        "batch_threshold": 10,
        "models": {"live": "gemini-2.0-flash", "batch": "gemini-2.0-flash-lite"},
        "storage": {"adapter": "json"}
    }
    with open(config_dir / "config.json", 'w') as f:
        json.dump(config_json, f)
    
    # Create mock competitor_data.json
    competitor_json = {
        "competitors": [
            {
                "name": "test_competitor",
                "base_url": "https://test.com",
                "structure_pattern": "single_list"
            }
        ]
    }
    with open(config_dir / "competitor_data.json", 'w') as f:
        json.dump(competitor_json, f)
    
    return config_dir

@pytest.fixture
def mock_file_operations(mocker):
    """Comprehensive file operation mocking."""
    # Mock os operations
    mocker.patch('os.makedirs')
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('os.path.isdir', return_value=True)
    mocker.patch('os.listdir', return_value=['test_file.json'])
    mocker.patch('os.path.join', side_effect=lambda *args: '/'.join(args))
    mocker.patch('os.remove')
    mocker.patch('os.rename')
    
    # Mock file operations
    mock_file = mock_open(read_data='{"test": "data"}')
    mocker.patch('builtins.open', mock_file)
    
    return {
        'open': mock_file,
        'makedirs': mocker.patch('os.makedirs'),
        'exists': mocker.patch('os.path.exists'),
        'isdir': mocker.patch('os.path.isdir'),
        'listdir': mocker.patch('os.listdir'),
        'remove': mocker.patch('os.remove'),
        'rename': mocker.patch('os.rename')
    }