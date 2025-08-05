# main.py
# This file serves as the main orchestrator for the ETL pipeline.

from dotenv import load_dotenv
import os
import json

# Import functions from other files in the same directory
from extract import extract_posts
from transform import transform_posts
from load import load_posts

def main():
    """
    Main synchronous function to run the ETL pipeline for all competitors.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load competitor data from the JSON file
    with open('competitor_seed_data.json', 'r') as f:
        config = json.load(f)

    for competitor in config['competitors']:
        name = competitor['name']
        url = competitor['url']
        base_url = competitor['base_url']
        post_list_selector = competitor['post_list_selector']
        date_selector = competitor['date_selector']

        print(f"\n--- Starting ETL process for {name} ---")
        
        # 1. EXTRACT: Scrape the blog and get recent posts
        # The extract_posts function now accepts all config details
        extracted_posts = extract_posts(url, base_url, post_list_selector, date_selector)

        # 2. TRANSFORM: Sort the data (a simple transformation)
        transformed_posts = transform_posts(extracted_posts)

        # 3. LOAD: Save the data to files
        if transformed_posts:
            load_posts(transformed_posts, filename_prefix=f"{name}_blog_posts")
        else:
            print(f"No recent posts found for {name}. No files will be created.")
            
    print("\n--- ETL process completed for all competitors ---")

if __name__ == "__main__":
    main()
