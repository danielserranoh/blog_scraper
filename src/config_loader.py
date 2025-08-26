# src/config_loader.py
# This module contains all logic for loading and processing
# the application and competitor configurations.

import os
import json
import logging

logger = logging.getLogger(__name__)

def load_configuration():
    """Loads and returns the application and competitor configurations."""
    try:
        with open('config/config.json', 'r') as f:
            app_config = json.load(f)
        with open('config/competitor_data.json', 'r') as f:
            competitor_config = json.load(f)
        
        # --- FIX: Ensure consistency in the configuration file ---
        # The 'modern campus' entry uses 'scraping_pattern' instead of 'structure_pattern'.
        # We will fix this by checking and updating the key if it exists.
        for competitor in competitor_config.get('competitors', []):
            if 'scraping_pattern' in competitor:
                competitor['structure_pattern'] = competitor.pop('scraping_pattern')
        
        return app_config, competitor_config
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return None, None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing configuration file: {e}")
        return None, None

def get_competitors_to_process(competitor_config, selected_competitor_name):
    """Filters and returns the list of competitors to be processed."""
    all_competitors = competitor_config.get('competitors', [])
    if not selected_competitor_name:
        return all_competitors
    for comp in all_competitors:
        if comp['name'].lower() == selected_competitor_name.lower():
            return [comp]
    logger.error(f"Competitor '{selected_competitor_name}' not found.")
    return []