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
        # Create a filename that reflects the content
        if len(competitors_exported) > 1:
            base_filename = f"all_competitors-{datetime.now().strftime('%y%m%d')}"
        else:
            base_filename = f"{competitors_exported[0]['name']}-{datetime.now().strftime('%y%m%d')}"
        
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        output_filepath = os.path.join(export_dir, f"{base_filename}.{export_format}")

        with open(output_filepath, "w", encoding="utf-8") as f:
            f.write(formatted_data)
        logger.info(f"Successfully exported data to: {output_filepath}")
    except IOError as e:
        logger.error(f"Failed to write export file: {e}")
    except IndexError:
        logger.error("Failed to generate filename for export as competitor list was empty.")