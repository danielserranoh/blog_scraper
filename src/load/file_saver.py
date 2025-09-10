# src/load/file_saver.py
# This module contains helpers for saving exported data to files.

import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def save_export_file(formatted_data, export_format, competitors_exported):
    """
    Saves the formatted data string to a file with a descriptive name.
    """
    try:
        # Define format mapping for proper file extensions and naming
        format_config = {
            'md': {'extension': 'md', 'suffix': ''},
            'csv': {'extension': 'csv', 'suffix': ''},
            'json': {'extension': 'json', 'suffix': ''},
            'txt': {'extension': 'txt', 'suffix': ''},
            'strategy-brief': {'extension': 'md', 'suffix': '-strategy-brief'},
            'content-gaps': {'extension': 'md', 'suffix': '-content-gaps'},
            'gsheets': {'extension': 'gsheets', 'suffix': ''}  # This doesn't save to file anyway
        }
        
        config = format_config.get(export_format, {'extension': export_format, 'suffix': ''})
        file_extension = config['extension']
        suffix = config['suffix']
        
        # Create a filename that reflects the content with proper format
        timestamp = datetime.now().strftime('%y%m%d')
        if len(competitors_exported) > 1:
            base_filename = f"all_competitors{suffix}-{timestamp}"
        else:
            competitor_name = competitors_exported[0]['name'].replace(' ', '_')
            base_filename = f"{competitor_name}{suffix}-{timestamp}"
        
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        output_filepath = os.path.join(export_dir, f"{base_filename}.{file_extension}")

        with open(output_filepath, "w", encoding="utf-8") as f:
            f.write(formatted_data)
        logger.info(f"Successfully exported data to: {output_filepath}")
    except IOError as e:
        logger.error(f"Failed to write export file: {e}")
    except IndexError:
        logger.error("Failed to generate filename for export as competitor list was empty.")