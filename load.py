# load.py
# This file contains the data loading logic to save data to files.

import csv
import os
from datetime import datetime

def load_posts(posts, filename_prefix="blog_posts"):
    """
    Saves the list of transformed post data to both a plain text file and a CSV file.

    Args:
        posts (list): A list of dictionaries with post data.
        filename_prefix (str): The prefix for the output filenames.
    """
    # Extract the competitor name from the filename_prefix.
    # Assumes the format is "competitor_name_blog_posts"
    competitor_name = filename_prefix.replace('_blog_posts', '')

    # Get the current date for the filename
    current_date = datetime.now().strftime('%y%m%d')

    # Create the folder path and ensure the directory exists
    output_folder = os.path.join('scraped', competitor_name)
    os.makedirs(output_folder, exist_ok=True)

    # Create the new filenames using the competitor name and date
    text_filename = f"{competitor_name}-{current_date}.txt"
    csv_filename = f"{competitor_name}-{current_date}.csv"

    # Construct the full file paths
    text_filepath = os.path.join(output_folder, text_filename)
    csv_filepath = os.path.join(output_folder, csv_filename)

    # Save to text file
    with open(text_filepath, 'w', encoding='utf-8') as f:
        f.write(f"Recent Blog Posts for {competitor_name} (Last 30 Days)\n")
        f.write("="*40 + "\n\n")
        for post in posts:
            # Reordered output for the text file to include new fields
            f.write(f"Title: {post['title']}\n")
            f.write(f"Publication Date: {post['publication_date']}\n")
            f.write(f"URL: {post['url']}\n")
            f.write(f"Summary: {post['summary']}\n")
            f.write(f"SEO Keywords (from Meta): {post['seo_meta_keywords']}\n")
            f.write(f"SEO Keywords (from LLM): {post['seo_keywords']}\n")
            f.write(f"Content: {post['content'][:300]}...\n") # Print a snippet of content
            f.write("-" * 40 + "\n\n")
    print(f"Successfully saved text data to {text_filepath}")

    # Save to CSV file
    # Reordered fieldnames for the CSV file to include all new fields
    fieldnames = ['title', 'publication_date', 'url', 'summary', 'seo_keywords', 'seo_meta_keywords', 'content']
    with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(posts)
    print(f"Successfully saved CSV data to {csv_filepath}")
