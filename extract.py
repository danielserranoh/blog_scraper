# extract.py
# This file contains the data extraction logic for the blog scraper.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import json
import time
import os
import csv

def is_recent(post_date, days=30):
    """
    Checks if a post's publication date is within the last `days` from today.
    
    This version uses timezone-naive datetime objects for a simpler comparison,
    avoiding the need for external libraries like pytz.
    """
    # Create a naive `now` object for a clean comparison
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=days)
    
    return post_date >= thirty_days_ago

def _get_existing_urls(competitor_name):
    """
    Reads the existing CSV file to get a list of all scraped post URLs.
    This prevents re-scraping the same articles.
    """
    existing_urls = set()
    output_folder = os.path.join('scraped', competitor_name)

    # Construct the file name using the naming convention, but without the date
    # as we want to find the latest file.
    filename_prefix = f"{competitor_name}_blog_posts"
    
    # Find the most recent CSV file for the competitor
    latest_file = None
    if os.path.isdir(output_folder):
        files = os.listdir(output_folder)
        csv_files = [f for f in files if f.endswith('.csv') and f.startswith(competitor_name)]
        if csv_files:
            # Sort files by name to find the most recent one (due to YYMMDD format)
            csv_files.sort(reverse=True)
            latest_file = os.path.join(output_folder, csv_files[0])
    
    if latest_file and os.path.exists(latest_file):
        with open(latest_file, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_urls.add(row['url'])
        print(f"Found {len(existing_urls)} existing URLs in {latest_file}.")
    else:
        print("No previous CSV file found. Scraping all posts.")

    return existing_urls


def _get_post_details(base_url, post_url, date_selector):
    """
    Scrapes an individual blog post page to find the title, URL, publication date,
    content, summary, and SEO keywords.
    """
    full_url = base_url + post_url if post_url.startswith('/') else post_url
    print(f"  Scraping details from: {full_url}")
    
    try:
        response = requests.get(full_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Extract Publication Date ---
        pub_date = None
        # Check for TerminalFour's <time> tag first
        date_element_time = soup.find('time')
        if date_element_time and 'datetime' in date_element_time.attrs:
            pub_date_str = date_element_time['datetime']
            # Try parsing with multiple formats for robustness
            date_formats_to_try = ['%Y-%m-%d %H:%M', '%Y-%m-%d']
            for fmt in date_formats_to_try:
                try:
                    pub_date = datetime.strptime(pub_date_str, fmt)
                    break
                except ValueError:
                    continue
        else:
            # Check for Modern Campus's "<p>Last updated: Month DD, YYYY</p>"
            date_element_p = soup.find('p', string=lambda text: text and "Last updated:" in text)
            if date_element_p:
                date_text = date_element_p.text.replace('Last updated:', '').strip()
                try:
                    pub_date = datetime.strptime(date_text, '%B %d, %Y')
                except ValueError:
                    pub_date = None

        # --- Extract Title ---
        title_element = soup.find('h1') or soup.find('h2')
        title = title_element.text.strip() if title_element else 'No Title Found'
        
        # --- Extract Meta Keywords ---
        # Find the meta tag for keywords and extract its content
        keywords_meta = soup.find('meta', {'name': 'keywords'})
        seo_meta_keywords = keywords_meta['content'] if keywords_meta and 'content' in keywords_meta.attrs else 'N/A'

        # --- Extract Content ---
        content_container = soup.find('div', class_=['article-content__main', 'post-content', 'blog-post-body', 'item-content'])
        content_text = ""
        if content_container:
            # More robust way to get and clean up text
            raw_text = content_container.get_text(separator=' ', strip=True)
            content_text = re.sub(r'\s+', ' ', raw_text)
        
        return {
            'title': title,
            'url': full_url,
            'publication_date': pub_date.strftime('%Y-%m-%d') if pub_date else 'N/A',
            'content': content_text.strip() if content_text else 'N/A',
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'seo_meta_keywords': seo_meta_keywords
        }

    except requests.exceptions.RequestException as e:
        print(f"Error fetching post details from {full_url}: {e}")
        return None

def _extract_from_terminalfour(url, base_url, post_list_selector, date_selector, days):
    """
    Scrapes the TerminalFour blog by first getting a list of post URLs
    and then visiting each one to get full details.
    """
    all_recent_posts = []
    page_number = 1
    found_older_post = False
    
    # Get a list of URLs already processed
    existing_urls = _get_existing_urls('terminalfour')

    while not found_older_post:
        print(f"Scanning page {page_number} for post URLs on TerminalFour blog...")
        current_page_url = f"{url}?page={page_number}"
        
        try:
            response = requests.get(current_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            post_links = soup.find_all('article', class_=['masthead__featured-article', 'article-card'])
            if not post_links:
                break

            for post in post_links:
                link_element = post.find('a')
                if link_element and link_element.get('href'):
                    post_url = link_element['href']
                    full_post_url = base_url + post_url
                    
                    if full_post_url in existing_urls:
                        print(f"  Skipping existing post: {full_post_url}")
                        continue
                    
                    # Call the new details scraper for each post
                    post_details = _get_post_details(base_url, post_url, date_selector)

                    if post_details:
                        # Check if the scraped date is recent
                        if post_details['publication_date'] != 'N/A':
                             pub_date = datetime.strptime(post_details['publication_date'], '%Y-%m-%d')
                             if is_recent(pub_date, days):
                                 all_recent_posts.append(post_details)
                             else:
                                 found_older_post = True
                                 break # Stop scraping, articles are in chronological order
                        else:
                            # If no date is found, we might as well keep it, assuming it's a recent post
                            all_recent_posts.append(post_details)
            
            if found_older_post:
                break
            
            page_number += 1

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {current_page_url}: {e}")
            break

    return all_recent_posts

def _extract_from_modern_campus(url, base_url, post_list_selector, date_selector, days):
    """
    Scrapes the Modern Campus blog by first getting a list of post URLs
    and then visiting each one to get full details.
    """
    all_recent_posts = []
    page_number = 1
    max_pages_to_scrape = 5 # Arbitrarily chosen to limit requests
    
    # Get a list of URLs already processed
    existing_urls = _get_existing_urls('modern campus')

    while page_number <= max_pages_to_scrape:
        print(f"Scanning page {page_number} for post URLs on Modern Campus blog...")
        current_page_url = f"{url.split('index.html')[0]}index.html?page={page_number}"
        
        try:
            response = requests.get(current_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Corrected CSS selector to only get the link from the h5 tag,
            # which is the most reliable and unique link for each post.
            post_link_elements = soup.select(post_list_selector)

            if not post_link_elements:
                break
            
            for link_element in post_link_elements:
                post_url = link_element['href']
                full_post_url = base_url + post_url
                
                if full_post_url in existing_urls:
                    print(f"  Skipping existing post: {full_post_url}")
                    continue
                
                post_details = _get_post_details(base_url, post_url, date_selector)

                if post_details and post_details['publication_date'] != 'N/A':
                    pub_date = datetime.strptime(post_details['publication_date'], '%Y-%m-%d')
                    if is_recent(pub_date, days):
                        all_recent_posts.append(post_details)
            
            page_number += 1

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {current_page_url}: {e}")
            break
            
    # Modern Campus posts are not in chronological order on the main page,
    # so we can't break early. We will rely on the `is_recent` check for each post.
    return all_recent_posts

def extract_posts(url, base_url, post_list_selector, date_selector, days=30):
    """
    A router function that selects the correct scraping logic based on the URL.
    """
    if 'terminalfour' in url:
        return _extract_from_terminalfour(url, base_url, post_list_selector, date_selector, days)
    elif 'moderncampus' in url:
        return _extract_from_modern_campus(url, base_url, post_list_selector, date_selector, days)
    else:
        print(f"No specific scraping logic found for {url}. Skipping.")
        return []
