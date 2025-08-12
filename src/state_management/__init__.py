# src/load/__init__.py
# This file contains the factory for creating storage adapters.

from .csv_adapter import CsvAdapter
# from .sqlite_adapter import SqliteAdapter # Example for the future

def get_storage_adapter(config):
    """
    Reads the configuration and returns an instance of the
    appropriate storage adapter.
    """
    # Default to 'csv' if no configuration is provided
    adapter_name = config.get('storage', {}).get('adapter', 'csv')
    
    if adapter_name == "csv":
        return CsvAdapter()
    # Example of how easy it would be to add a new adapter:
    # elif adapter_name == "sqlite":
    #     return SqliteAdapter()
    else:
        # Raise an error if the configured adapter is unknown
        raise ValueError(f"Unknown storage adapter configured: '{adapter_name}'")