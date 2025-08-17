# src/load/export_manager.py
# This module contains the high-level logic for managing the data export process.

import os
import csv
import logging
from . import exporters
from .file_saver import save_export_file

logger = logging.getLogger(__name__)

class ExportManager:
    """
    Manages the process of exporting data from the processed directory
    to user-facing files in various formats.
    """

    def __init__(self):
        pass

    def run_export_process(self, competitors_to_export, export_format, app_config):
        """
        Reads from the 'processed' data directory to create user-facing exports.
        """
        logger.info(f"--- Starting export process to {export_format.upper()} ---")
        all_posts_to_export = []
        
        for competitor in competitors_to_export:
            competitor_name = competitor['name']
            # Read from the 'processed' data directory
            processed_folder = os.path.join("data", "processed", competitor_name)
            
            if not os.path.isdir(processed_folder):
                logger.warning(f"No processed data found for '{competitor_name}'. Skipping.")
                continue
            
            # Read all CSVs in the processed folder for that competitor
            for filename in os.listdir(processed_folder):
                if filename.endswith('.csv'):
                    filepath = os.path.join(processed_folder, filename)
                    logger.info(f"Reading data for '{competitor_name}' from: {filename}")
                    with open(filepath, mode='r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for post in reader:
                            post['competitor'] = competitor_name
                            all_posts_to_export.append(post)

        if not all_posts_to_export:
            logger.warning("‼️ No data found to export.")
            return

        try:
            formatted_data = exporters.export_data(all_posts_to_export, export_format, app_config)
        except ValueError as e:
            logger.error(e)
            return
        # For gsheets, the returned data is a success message, not file content
        if export_format == 'gsheets':
            # For Google Sheets, the return value is a status message, so we just log it.
            logger.info(formatted_data)
        else:
            # For all other formats, call our new dedicated saver function.
            save_export_file(formatted_data, export_format, competitors_to_export)