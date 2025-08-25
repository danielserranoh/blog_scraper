import csv
import os
import json
import re
import argparse
import logging
from termcolor import colored

def setup_logger():
    """Configures a logger with colored output."""
    class ColorFormatter(logging.Formatter):
        COLORS = { 'INFO': 'blue', 'WARNING': 'yellow', 'ERROR': 'red' }
        def format(self, record):
            log_message = super().format(record)
            log_color = self.COLORS.get(record.levelname)
            level_name = record.levelname.lower()
            log_level = colored(f"{level_name}:", color=log_color, attrs=['bold'])
            return f"{log_level} {log_message}"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColorFormatter('%(message)s'))
    if root_logger.hasHandlers(): root_logger.handlers.clear()
    root_logger.addHandler(console_handler)

def clean_json_string(json_string):
    """
    Cleans a string to prepare it for JSON deserialization by:
    1. Replacing single quotes with double quotes.
    2. Fixing escaped double quotes.
    """
    if not isinstance(json_string, str) or not json_string.strip():
        return '[]'  # Return a valid empty JSON array for non-strings or empty values

    # Correct single quotes and handle escaped double quotes
    cleaned_string = json_string.replace("'", '"')
    
    # Check for instances of escaped double quotes from CSV writing.
    # If the string starts and ends with a double quote, it's likely wrapped
    # and has internal escaped quotes that need to be unescaped.
    if cleaned_string.startswith('"') and cleaned_string.endswith('"'):
        # This regex un-escapes duplicated double quotes while preserving
        # the JSON format.
        return re.sub(r'(?<!\\)\\"', '"', cleaned_string[1:-1]).replace('""', '"')
        
    return cleaned_string

def process_and_clean_files(competitor_name, file_type):
    """
    Reads CSV files from the specified data directory, cleans stringified JSON fields,
    and writes the corrected data to a new file.
    """
    input_folder = os.path.join('data', file_type, competitor_name)
    original_output_folder = os.path.join(input_folder, 'original_output')
    os.makedirs(original_output_folder, exist_ok=True)
    
    if not os.path.isdir(input_folder):
        logging.error(f"Input folder not found: {input_folder}")
        return

    processed_files_count = 0
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            input_filepath = os.path.join(input_folder, filename)
            original_filepath = os.path.join(original_output_folder, filename)
            
            try:
                # Move the original file to the 'original_output' folder first
                os.rename(input_filepath, original_filepath)
                
                with open(original_filepath, mode='r', newline='', encoding='utf-8-sig') as infile:
                    reader = csv.DictReader(infile)
                    fieldnames = reader.fieldnames
                    
                    if not fieldnames:
                        logging.warning(f"Skipping empty file: {filename}")
                        continue
                        
                    # Save the cleaned data to the original input folder
                    with open(input_filepath, mode='w', newline='', encoding='utf-8') as outfile:
                        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                        writer.writeheader()
                        
                        for row in reader:
                            for field in ['headings', 'schemas']:
                                if field in row:
                                    row[field] = clean_json_string(row[field])
                            writer.writerow(row)
                
                logging.info(f"Successfully cleaned and saved: {filename}")
                processed_files_count += 1
            except Exception as e:
                logging.error(f"Error processing file {filename}: {e}")
    
    if processed_files_count == 0:
        logging.warning("No CSV files found in the specified directory.")
    else:
        logging.info(f"\n--- Cleaning process completed. {processed_files_count} file(s) processed. ---")


if __name__ == "__main__":
    setup_logger()
    parser = argparse.ArgumentParser(description="Clean stringified JSON in CSV files.")
    parser.add_argument('competitor', type=str, help="The name of the competitor to process.")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--raw', action='store_true', help='Clean the raw data directory.')
    group.add_argument('--processed', action='store_true', help='Clean the processed data directory.')

    args = parser.parse_args()
    
    file_type = 'raw' if args.raw else 'processed'

    process_and_clean_files(args.competitor, file_type)