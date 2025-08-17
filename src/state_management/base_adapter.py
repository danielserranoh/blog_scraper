# src/state_management/base_adapter.py
# This file defines the abstract interface for all storage adapters.

from abc import ABC, abstractmethod

class BaseAdapter(ABC):
    """
    Abstract Base Class for Storage Adapters.

    This class defines the 'contract' that all concrete storage adapters
    (e.g., CsvAdapter, SqliteAdapter) must follow.
    """
    @abstractmethod
    def save(self, posts, competitor_name, file_type, source_filename=None):
        """
        The core method to save a list of post dictionaries.

        Args:
            posts (list): A list of dictionaries, where each dictionary is a post.
            competitor_name (str): The name of the competitor being processed.
            file_type (str): The type of data being saved ('raw' or 'processed').
            source_filename (str, optional): The filename of the raw data file that
                                             was the source of these posts.
        """
        pass

    @abstractmethod
    def read(self, competitor_name, file_type):
        """
        The core method to read a list of post dictionaries.

        Args:
            competitor_name (str): The name of the competitor being processed.
            file_type (str): The type of data to read ('raw' or 'processed').
        """
        pass