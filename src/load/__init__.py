# src/load/__init__.py
# This file contains the factories for our data adapters.

from .processed_data_adapter import CsvProcessedDataAdapter
from .export_manager import ExportManager
# Future adapters can be imported here, e.g.:
# from .processed_data_adapter import JsonProcessedDataAdapter

def get_processed_data_adapter(config):
    """
    Reads the configuration and returns an instance of the appropriate
    adapter for saving final, processed data.
    """
    # Look for a new 'processed_data' section in the config, default to 'csv'
    adapter_name = config.get('processed_data', {}).get('adapter', 'csv')
    
    if adapter_name == "csv":
        return CsvProcessedDataAdapter()
    # Example of how easy it would be to add a new adapter:
    # elif adapter_name == "json":
    #     return JsonProcessedDataAdapter()
    else:
        raise ValueError(f"Unknown processed data adapter configured: '{adapter_name}'")