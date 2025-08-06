# src/load.py
# This file contains the data loading logic to save data to files.

import csv
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def load_posts(posts, filename_prefix="blog_posts"):
    """
    Saves the list of transformed post data to both a plain text file and a CSV file.
    
    The function appends to the files if they already exist, ensuring that
    newly scraped batches are added without overwriting previous data.

    Args:
        posts (list): A list of dictionaries with post data.
        filename_prefix (str): The prefix for the output filenames.
    """
    competitor_name = filename_prefix.replace('_blog_posts', '')
    current_date = datetime.now().strftime('%y%m%d')
    output_folder = os.path.join('scraped', competitor_name)
    os.makedirs(output_folder, exist_ok=True)
    text_filename = f"{competitor_name}-{current_date}.txt"
    csv_filename = f"{competitor_name}-{current_date}.csv"
    text_filepath = os.path.join(output_folder, text_filename)
    csv_filepath = os.path.join(output_folder, csv_filename)

    # Check if files exist to determine if we should write headers
    text_file_exists = os.path.exists(text_filepath)
    csv_file_exists = os.path.exists(csv_filepath)

    # Save to text file
    with open(text_filepath, 'a', encoding='utf-8') as f:
        if not text_file_exists:
            f.write(f"Recent Blog Posts for {competitor_name} (Last 30 Days)\n")
            f.write("="*40 + "\n\n")
        
        for post in posts:
            f.write(f"Title: {post['title']}\n")
            f.write(f"Publication Date: {post['publication_date']}\n")
            f.write(f"URL: {post['url']}\n")
            f.write(f"Summary: {post['summary']}\n")
            f.write(f"SEO Keywords (from Meta): {post['seo_meta_keywords']}\n")
            f.write(f"SEO Keywords (from LLM): {post['seo_keywords']}\n")
            f.write(f"Content: {post['content'][:300]}...\n") # Print a snippet of content
            f.write("-" * 40 + "\n\n")
    logger.info(f"Successfully saved text data to {text_filepath}")

    # Save to CSV file
    fieldnames = ['title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
    with open(csv_filepath, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not csv_file_exists:
            writer.writeheader()
        writer.writerows(posts)
    logger.info(f"Successfully saved CSV data to {csv_filepath}")