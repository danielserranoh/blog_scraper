# src/state_management/state_manager.py
# This module contains the centralized manager for all data persistence.

import logging
from .json_adapter import JsonAdapter
from .csv_adapter import CsvAdapter
from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)

class StateManager:
    """
    A centralized manager that handles all data persistence for the ETL pipeline.
    It acts as a factory for the appropriate storage adapter based on the configuration.
    """
    def __init__(self, config):
        adapter_name = config.get('storage', {}).get('adapter', 'csv')
        self.adapter = self._get_adapter(adapter_name)

    def _get_adapter(self, adapter_name) -> BaseAdapter:
        """
        Returns an instance of the appropriate storage adapter.
        """
        if adapter_name == "csv":
            return CsvAdapter()
        elif adapter_name == "json":
            return JsonAdapter()
        # Add a new adapter here when you need to switch storage
        # elif adapter_name == "gsheets":
        #    return GoogleSheetsAdapter()
        else:
            raise ValueError(f"Unknown storage adapter configured: '{adapter_name}'")

    def save_raw_data(self, posts, competitor_name):
        """Saves raw scraped data."""
        return self.adapter.save(posts, competitor_name, file_type='raw')

    def save_processed_data(self, posts, competitor_name, source_filename):
        """Saves final, enriched data."""
        return self.adapter.save(posts, competitor_name, file_type='processed', source_filename=source_filename)
        
    def load_raw_data(self, competitor_name):
        """Loads all raw scraped data."""
        return self.adapter.read(competitor_name, file_type='raw')

    def load_processed_data(self, competitor_name):
        """Loads all processed data."""
        return self.adapter.read(competitor_name, file_type='processed')
    
    def load_raw_urls(self, competitor_name):
        """Loads all post URLs from the raw data files."""
        return self.adapter.read_urls(competitor_name, file_type='raw')
    
    def get_latest_raw_filepath(self, competitor_name):
        """
        Finds and returns the full path of the most recently created raw data file.
        """
        raw_data_dir = os.path.join('data', 'raw', competitor_name)
        if not os.path.isdir(raw_data_dir):
            return None
        
        files = [os.path.join(raw_data_dir, f) for f in os.listdir(raw_data_dir)]
        if not files:
            return None
            
        latest_file = max(files, key=os.path.getctime)
        return latest_file