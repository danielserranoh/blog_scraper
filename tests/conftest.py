# conftest.py
# This file is used to configure pytest and define shared fixtures.

import sys
import os
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from types import SimpleNamespace # <--- ADD THIS

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.state_management.state_manager import StateManager
from src.transform.enrichment_manager import EnrichmentManager
from src.transform.batch_manager import BatchJobManager
from src.load.export_manager import ExportManager
from src.api_connector import GeminiAPIConnector


@pytest.fixture
def mock_state_manager(mocker):
    """Mocks the StateManager class and its methods."""
    mock_manager = MagicMock(spec=StateManager)
    mocker.patch('src.state_management.state_manager.StateManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_enrichment_manager(mocker):
    """Mocks the EnrichmentManager class and its methods."""
    mock_manager = MagicMock(spec=EnrichmentManager)
    mocker.patch('src.transform.enrichment_manager.EnrichmentManager', return_value=mock_manager)
    
    # <--- UPDATED: Add the 'wait' argument to the AsyncMock methods. --->
    mock_manager.enrich_posts = AsyncMock()
    mock_manager.enrich_raw_data = AsyncMock()
    return mock_manager

@pytest.fixture
def mock_batch_manager(mocker):
    """Mocks the BatchJobManager class and its methods."""
    mock_manager = MagicMock(spec=BatchJobManager)
    mocker.patch('src.transform.batch_manager.BatchJobManager', return_value=mock_manager)
    
    # <--- UPDATED: Add the 'wait' argument to the AsyncMock methods. --->
    mock_manager.check_and_load_results = AsyncMock()
    mock_manager.submit_new_jobs = AsyncMock()
    return mock_manager

@pytest.fixture
def mock_export_manager(mocker):
    """Mocks the ExportManager class and its methods."""
    mock_manager = MagicMock(spec=ExportManager)
    mocker.patch('src.load.export_manager.ExportManager', return_value=mock_manager)
    return mock_manager

@pytest.fixture
def mock_api_connector(mocker):
    """Mocks the GeminiAPIConnector for isolated testing."""
    mock_connector = MagicMock(spec=GeminiAPIConnector)
    mocker.patch('src.api_connector.GeminiAPIConnector', return_value=mock_connector)
    mock_connector.create_batch_job = MagicMock(return_value="batches/mock-job-id")
    mock_connector.check_batch_job = MagicMock(return_value="JOB_STATE_SUCCEEDED")
    mock_connector.download_batch_results = MagicMock(return_value=[])
    return mock_connector

@pytest.fixture
def mock_args():
    """Provides a mock SimpleNamespace object for CLI arguments."""
    return SimpleNamespace(
        days=30,
        all=False,
        competitor=None,
        wait=False,
        scrape=True,
        enrich=False,
        enrich_raw=False,
        check_job=False,
        export=None
    )