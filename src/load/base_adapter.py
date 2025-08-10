# src/load/base_adapter.py
# This file defines the abstract interface for all storage adapters.

from abc import ABC, abstractmethod

class BaseAdapter(ABC):
    """
    Abstract Base Class for Storage Adapters.

    This class defines the 'contract' that all concrete storage adapters
    (e.g., CsvAdapter, SqliteAdapter) must follow.
    """
    @abstractmethod
    def save(self, posts, competitor_name):
        """
        The core method to save a list of post dictionaries.

        Args:
            posts (list): A list of dictionaries, where each dictionary is a post.
            competitor_name (str): The name of the competitor being processed.
        """
        pass