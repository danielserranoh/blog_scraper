# transform.py
# This file contains the data transformation logic.

from datetime import datetime
import json
import requests
import os
import time

def _get_gemini_details(content):
    """
    Calls the Gemini API to get a summary and keywords from the content.
    """
    summary = "N/A"
    seo_keywords = "N/A"
    
    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        print("GEMINI_API_KEY not set in environment variables. Skipping API call.")
        return summary, seo_keywords

    if content:
        prompt = f"Given the following blog post content, please provide a summary (no more than 350 characters) and a list of key SEO keywords. Return a JSON object with 'summary' and 'seo_keywords' keys.\n\nContent: {content}"

        chatHistory = []
        chatHistory.append({ "role": "user", "parts": [{ "text": prompt }] })
        
        payload = {
            "contents": chatHistory,
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "OBJECT",
                    "properties": {
                        "summary": { "type": "STRING" },
                        "seo_keywords": { 
                            "type": "ARRAY", 
                            "items": { "type": "STRING" }
                        }
                    }
                }
            }
        }

        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={gemini_api_key}"
        
        # Simple exponential backoff for retries
        for i in range(3):
            try:
                response_gen = requests.post(
                    api_url,
                    headers={ 'Content-Type': 'application/json' },
                    data=json.dumps(payload)
                )
                response_gen.raise_for_status()
                result = response_gen.json()
                
                if result.get('candidates'):
                    json_result = result['candidates'][0]['content']['parts'][0]['text']
                    parsed_json = json.loads(json_result)
                    summary = parsed_json['summary']
                    seo_keywords = ', '.join(parsed_json['seo_keywords'])
                
                break
            except requests.exceptions.RequestException as e:
                print(f"API call failed, retrying in {2**i} seconds... Error: {e}")
                time.sleep(2**i)
            except Exception as e:
                print(f"Failed to process API response: {e}")
                break
    
    return summary, seo_keywords

def transform_posts(posts):
    """
    Transforms the extracted post data. This is a placeholder for future
    transformations, such as adding SEO keywords or cleaning text.
    For now, it simply sorts the posts by date.

    Args:
        posts (list): A list of dictionaries with post data.

    Returns:
        list: The transformed and sorted list of dictionaries.
    """
    transformed_posts = []

    # Filter out any posts that might have an 'N/A' date if the scraper fails
    valid_posts = [p for p in posts if p['publication_date'] != 'N/A']

    for post in valid_posts:
        # Get summary and keywords from the API
        summary, seo_keywords = _get_gemini_details(post.get('content'))
        post['summary'] = summary
        post['seo_keywords'] = seo_keywords
        transformed_posts.append(post)

    # Sort the posts by date in descending order (most recent first)
    transformed_posts.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
    
    return transformed_posts
