import os
import csv
import logging

# Basic logger for feedback
logging.basicConfig(level=logging.INFO, format='INFO: %(message)s')

def clean_state_files():
    """
    Reads all competitor state CSVs, removes the empty 'competitor' column,
    and overwrites the files with the cleaned data.
    """
    state_dir = 'state'
    if not os.path.isdir(state_dir):
        logging.error(f"'{state_dir}/' directory not found. Nothing to clean.")
        return

    logging.info(f"Starting cleanup of CSV files in '{state_dir}/'...")

    # Loop through each competitor's sub-directory in the state folder
    for competitor_name in os.listdir(state_dir):
        competitor_dir = os.path.join(state_dir, competitor_name)
        if not os.path.isdir(competitor_dir):
            continue

        state_filepath = os.path.join(competitor_dir, f"{competitor_name}_state.csv")

        if not os.path.exists(state_filepath):
            logging.warning(f"No state file found for '{competitor_name}'. Skipping.")
            continue

        logging.info(f"Processing: {state_filepath}")
        
        cleaned_rows = []
        try:
            with open(state_filepath, mode='r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                # Read all data into memory, excluding the 'competitor' key
                for row in reader:
                    row.pop('competitor', None) # Safely remove the key
                    cleaned_rows.append(row)

            if not cleaned_rows:
                logging.info(f"  File for '{competitor_name}' is empty. No changes needed.")
                continue

            # Now, overwrite the original file with the cleaned data
            fieldnames = list(cleaned_rows[0].keys())
            with open(state_filepath, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(cleaned_rows)
            
            logging.info(f"  Successfully cleaned and saved file for '{competitor_name}'.")

        except Exception as e:
            logging.error(f"  Could not process file for '{competitor_name}': {e}")

if __name__ == "__main__":
    clean_state_files()
    logging.info("--- Cleanup process completed ---")