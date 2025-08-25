# src/state_management/__init__.py
# This file serves as the factory for our data adapters.

from .state_manager import StateManager
from .json_adapter import JsonAdapter
from .csv_adapter import CsvAdapter
from .base_adapter import BaseAdapter