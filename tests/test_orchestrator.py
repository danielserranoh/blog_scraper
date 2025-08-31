# tests/test_orchestrator.py
# This file contains unit tests for the high-level orchestration logic.

import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
from types import SimpleNamespace # <--- ADD THIS

# Import the orchestrator module to test its functions
from src.orchestrator import run_pipeline

# We need to create a simple mock for the new manager classes
class MockScraperManager:
    async def run_scrape_and_submit(self, *args, **kwargs):
        pass

class MockEnrichmentManager:
    async def run_enrichment_process(self, *args, **kwargs):
        pass
    async def enrich_raw_data(self, *args, **kwargs):
        pass
    
class MockBatchJobManager:
    async def check_and_load_results(self, *args, **kwargs):
        pass

class MockExportManager:
    def run_export_process(self, *args, **kwargs):
        pass

@pytest.fixture
def mock_managers(mocker):
    """Mocks the new manager classes with our simple mock objects."""
    mocker.patch('src.orchestrator.ScraperManager', return_value=MockScraperManager())
    mocker.patch('src.orchestrator.EnrichmentManager', return_value=MockEnrichmentManager())
    mocker.patch('src.orchestrator.BatchJobManager', return_value=MockBatchJobManager())
    mocker.patch('src.orchestrator.ExportManager', return_value=MockExportManager())

@pytest.fixture
def mock_config(mocker):
    """Mocks the configuration loading to avoid file I/O."""
    mock_app_config = {"batch_threshold": 10}
    mock_comp_config = {"competitors": [{"name": "test_competitor"}]}
    mocker.patch('src.orchestrator.load_configuration', return_value=(mock_app_config, mock_comp_config))

@pytest.mark.asyncio
async def test_run_pipeline_calls_scraper_manager(mocker, mock_managers, mock_config):
    """Tests that the orchestrator calls the scraper manager for a default run."""
    # <--- UPDATED: Use a SimpleNamespace object to mock args. --->
    mock_args = SimpleNamespace(competitor=None, days=30, all=False, check_job=False, enrich=False, enrich_raw=False, export=None, wait=False)
    mock_scrape = mocker.patch.object(MockScraperManager, 'run_scrape_and_submit', new_callable=AsyncMock)

    await run_pipeline(mock_args)

    mock_scrape.assert_called_once()

@pytest.mark.asyncio
async def test_run_pipeline_calls_enrichment_manager_for_enrich(mocker, mock_managers, mock_config):
    """Tests that the orchestrator calls the enrichment manager for the --enrich flag."""
    # <--- UPDATED: Use a SimpleNamespace object. --->
    mock_args = SimpleNamespace(competitor="test_competitor", enrich=True, check_job=False, enrich_raw=False, export=None, wait=False)
    mock_check_job = mocker.patch.object(MockBatchJobManager, 'check_and_load_results', new_callable=AsyncMock)
    mock_enrich = mocker.patch.object(MockEnrichmentManager, 'run_enrichment_process', new_callable=AsyncMock)

    await run_pipeline(mock_args)
    
    # We assert that the orchestrator correctly calls the check-job and then the enrichment manager
    mock_check_job.assert_called_once()
    mock_enrich.assert_called_once()
    

@pytest.mark.asyncio
async def test_run_pipeline_calls_batch_manager_for_check_job(mocker, mock_managers, mock_config):
    """Tests that the orchestrator calls the batch manager for the check-job command."""
    # <--- UPDATED: Use a SimpleNamespace object. --->
    mock_args = SimpleNamespace(competitor="test_competitor", check_job=True, enrich=False, enrich_raw=False, export=None, wait=False)
    mock_check_job = mocker.patch.object(MockBatchJobManager, 'check_and_load_results', new_callable=AsyncMock)

    await run_pipeline(mock_args)
    
    mock_check_job.assert_called_once()

@pytest.mark.asyncio
async def test_run_pipeline_calls_enrichment_manager_for_enrich_raw(mocker, mock_managers, mock_config):
    """Tests that the orchestrator calls the enrichment manager for the --enrich-raw flag."""
    # <--- UPDATED: Use a SimpleNamespace object. --->
    mock_args = SimpleNamespace(competitor="test_competitor", enrich_raw=True, enrich=False, check_job=False, export=None, wait=False)
    mock_enrich_raw = mocker.patch.object(MockEnrichmentManager, 'enrich_raw_data', new_callable=AsyncMock)

    await run_pipeline(mock_args)

    mock_enrich_raw.assert_called_once()

@pytest.mark.asyncio
async def test_run_pipeline_calls_export_manager_for_export(mocker, mock_managers, mock_config):
    """Tests that the orchestrator calls the export manager for the --export flag."""
    # <--- UPDATED: Use a SimpleNamespace object. --->
    mock_args = SimpleNamespace(competitor="test_competitor", export='csv', check_job=False, enrich=False, enrich_raw=False, wait=False)
    mock_export = mocker.patch.object(MockExportManager, 'run_export_process')
    mock_check_job = mocker.patch.object(MockBatchJobManager, 'check_and_load_results', new_callable=AsyncMock)

    await run_pipeline(mock_args)

    mock_check_job.assert_called_once()
    mock_export.assert_called_once()