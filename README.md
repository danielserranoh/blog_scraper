

````markdown
# Python Blog Scraper and Analyzer

### Project Description

This Python project is an ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, meta keywords, and content.

The project then enriches this data using the Gemini API to generate a concise summary and a list of SEO keywords for each article. The pipeline intelligently chooses between two processing methods:
* **Live Processing**: For a small number of new articles, it uses asynchronous API calls for real-time enrichment.
* **Batch Processing**: For a large number of articles, it submits them to the Gemini Batch API for cost-effective, asynchronous processing.

The final, enriched data is saved to a clean `.txt` file and a `.csv` file. The project is structured to be modular and easily expandable, allowing you to add new competitors by simply updating a single JSON configuration file.

## Project Setup

**Clone or download** the project files to your local machine.

### Prerequisites

To run this project, you will need:

* Python 3.6 or newer
* The libraries listed in the `requirements.txt` file.

You can install all the required libraries by running the following command in your terminal:

```bash
pip install -r requirements.txt
````

You will also need a Gemini API key. You can get one for free at the Google AI Studio website:
[https://aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)

Create a `.env` file in the project's root directory and add your API key like this:

```
GEMINI_API_KEY=your_api_key_here
```

## How to Run the Scraper

The scraper is run from the command line and offers several flags to control its behavior.

  * **Default (last 30 days for all competitors):**
    `python main.py`
  * **Specify a number of days for all competitors:**
    `python main.py 7`
  * **Scrape all posts (no date filter) for all competitors:**
    `python main.py --all`
  * **Scrape a specific competitor (default 30 days):**
    `python main.py --competitor "terminalfour"`
  * **Check the status of a previously submitted batch job:**
    `python main.py --check-job --competitor "modern campus"`
  * **Show the help message:**
    `python main.py --help`

The script will print its progress to the console. When it's finished, the generated files will be saved in the `scraped/<competitor_name>/` directory.

-----

### Project Workflow: A Step-by-Step Breakdown

The scraper operates in a clear, modular ETL (Extract, Transform, Load) workflow.

#### 1\. The Extraction Phase (`src/extract/`)

  * **Configuration Reading**: `main.py` reads the `config/competitor_seed_data.json` file to get the URLs and selectors for each competitor.
  * **Existing Data Check (`src/extract/_common.py`)**: The `_get_existing_urls` function checks for a previously generated CSV file to avoid re-processing old articles.
  * **Routing (`src/extract/__init__.py`)**: The `extract_posts_in_batches` function acts as a router, dispatching the scraping task to the correct competitor-specific function (e.g., `extract_from_terminalfour`).
  * **Competitor-Specific Scraping (`src/extract/competitors/*.py`)**: Each file in this directory contains the unique logic for navigating a specific blog's structure (handling pagination, etc.).
  * **Deep Scraping (`src/extract/_common.py`)**: For each new article URL, the `_get_post_details` function performs an asynchronous request to that page to extract the title, publication date, content, and meta keywords.

#### 2\. The Transformation Phase (`src/transform/`)

This phase enriches the extracted data using one of two methods, determined by the `batch_threshold` setting in the configuration file.

  * **Live Processing (`src/transform/live.py`)**:
      * Triggered when the number of new posts is **below** the `batch_threshold`.
      * Uses `asyncio` and `httpx` to send concurrent requests to the Gemini API for each post.
      * This provides fast, real-time enrichment for small jobs.
  * **Batch Processing (`src/transform/batch.py`)**:
      * Triggered when the number of new posts is **equal to or above** the `batch_threshold`.
      * The script creates a JSONL file containing all the posts and uploads it to the Gemini API.
      * A batch job is created, and the job ID is saved locally. You can check the job's status later using the `--check-job` flag.
      * This method is slower but significantly more cost-effective for large volumes of data.

#### 3\. The Loading Phase (`src/load.py`)

  * **File Creation**: This module receives the fully enriched and sorted data.
  * **Output**: It creates a new folder for the competitor (if it doesn't exist) and saves the data to both a formatted `.txt` file and a `.csv` file, ready for analysis.

### Project Files

  * **`main.py`**: The central orchestrator of the project.
  * **`config/competitor_seed_data.json`**: Configuration file for competitors, URLs, and selectors.
  * **`src/`**: Contains all core Python source code.
      * **`src/extract/`**: Module for the extraction phase.
          * `_common.py`: Shared helper functions for extraction.
          * `competitors/`: Directory for competitor-specific scrapers.
      * **`src/transform/`**: Module for the transformation phase.
          * `live.py`: Logic for real-time Gemini API processing.
          * `batch.py`: Logic for Gemini Batch API processing.
      * **`src/load.py`**: Handles the final output and file saving.
  * **`.env`**: Securely stores your Gemini API key.
  * **`.gitignore`**: Specifies files to be ignored by Git (e.g., `scraped/`, `.env`).
  * **`requirements.txt`**: A list of all Python dependencies for the project.

<!-- end list -->

````

---

### 2. Move `batch_threshold` to `config.json`

This is a great idea for making the script more flexible. Here are the two changes needed:

**A. Update `config/competitor_seed_data.json`**

Add the `batch_threshold` key to your main configuration object. A value of 10 is a sensible default.

```json
{
  "batch_threshold": 10,
  "competitors": [
    {
      "name": "terminalfour",
      "urls": [
        "blog/",
        "blog/cats/accessibility/"
      ],
      "base_url": "https://www.terminalfour.com",
      "post_list_selector": "article.masthead__featured-article a, article.article-card a",
      "date_selector": "time[datetime]"
    },
    {
      "name": "modern campus",
      "urls": [
        "blog/index.html"
      ],
      "base_url": "https://moderncampus.com",
      "post_list_selector": ".filter-grid .col .card-body h5 a",
      "date_selector": "p:contains('Last updated:')"
    },
    {
      "name": "squiz",
      "urls": [
        "blog"
      ],
      "base_url": "https://www.squiz.net",
      "post_list_selector": "a.cards-list__card",
      "date_selector": "span.blog-banner__contents-author__date"
    }
  ]
}
````

**B. Update `main.py`**

Modify the `main` function to read this value from the loaded config and remove the hardcoded variable.

```python
# In main.py, inside the main() function

# ... (around line 170)

    args = parser.parse_args()
    
    # ... (error handling for args)

    days_to_scrape = args.days
    scrape_all = args.all
    selected_competitor = args.competitor
    enrich_posts = args.enrich
    # REMOVE the line below
    # batch_threshold = 10 

    try:
        with open('config/competitor_seed_data.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("Error: 'config/competitor_seed_data.json' not found. Please ensure the file exists.")
        return

    # ADD this line to get the threshold from the config
    batch_threshold = config.get('batch_threshold', 10) # Default to 10 if not found

    competitors_to_process = []
    # ... (the rest of the file remains the same)
```

And update the `run_scrape_and_submit` function signature to accept the `batch_threshold` value.

```python
# In main.py

# Update the function definition
async def run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold):
    """
    Scrapes the blog and decides whether to use live or batch processing.
    """
    # ... function body remains the same

# ...

# In main(), update the function call at the end of the file
        else:
            await run_scrape_and_submit(competitor, days_to_scrape, scrape_all, batch_threshold)
```

-----

### 3\. Refine Content Exclusion Logic

To handle the "Skip ahead:" list for the Squiz competitor, we first need to pass the competitor's name down to the `_get_post_details` function.

**A. Update Function Calls**

In each of your competitor-specific scraping files (e.g., `moderncampus.py`, `squiz.py`, `terminalfour.py`), update the call to `_get_post_details` to pass the competitor's name.

```python
# Example for src/extract/competitors/squiz.py

# ... inside extract_from_squiz function
# ... inside the loop

                    # Pass the competitor name from the config
                    tasks.append(_get_post_details(client, "", full_post_url, config['name']))
```

You will need to do this for the `_get_post_details` call in all three competitor files: `moderncampus.py`, `squiz.py`, and `terminalfour.py`.

**B. Update `_get_post_details` in `src/extract/_common.py`**

Now, modify the function to accept the `competitor_name` and implement the conditional logic.

```python
# src/extract/_common.py

# Add competitor_name to the function signature
async def _get_post_details(client, base_url, post_url_path, competitor_name):
    """
    Scrapes an individual blog post page to find the title, URL, publication date,
    content, summary, and SEO keywords.
    """
    # ... (rest of the function up to content extraction)

        # --- Extract Content ---
        content_container = soup.find('div', class_=['article-content__main', 'post-content', 'blog-post-body', 'item-content', 'no-wysiwyg'])
        content_text = ""
        if content_container:
            # Specific logic for 'squiz' competitor
            if competitor_name == 'squiz':
                skip_ahead_header = content_container.find('h3', string=lambda text: text and 'Skip ahead:' in text)
                if skip_ahead_header:
                    # Find the <ul> that immediately follows the "Skip ahead:" header
                    next_ul = skip_ahead_header.find_next_sibling('ul')
                    if next_ul:
                        next_ul.decompose() # Remove the "Skip ahead" list from the parse tree

            # General content extraction (now without the squiz ToC)
            for element in content_container.children:
                if hasattr(element, 'get_text'):
                    content_text += element.get_text(separator=' ', strip=True) + " "

            content_text = re.sub(r'\s+', ' ', content_text)

        return {
            'title': title,
            'url': full_url,
            'publication_date': pub_date.strftime('%Y-%m-%d') if pub_date else 'N/A',
            'content': content_text.strip() if content_text else 'N/A',
            'summary': 'N/A',
            'seo_keywords': 'N/A',
            'seo_meta_keywords': seo_meta_keywords
        }

    except httpx.RequestError as e:
        logger.error(f"Error fetching post details from {full_url}: {e}")
        return None
```

-----

### 4\. Improve `src/transform/batch.py`

You are right to be cautious about parsing the batch results. Using a regex is okay as a fallback, but we can make it more robust by prioritizing a direct JSON load, since you've correctly set `response_mime_type` to `application/json` in the request.

Here is a revised `download_gemini_batch_results` function for `src/transform/batch.py`. It attempts to parse the entire response as JSON first, which is cleaner and safer. It only falls back to a regex search if the direct parsing fails.

````python
# src/transform/batch.py

def download_gemini_batch_results(job_id, original_posts):
    """
    Downloads the results of a completed Gemini batch job using the SDK
    and combines them with the original posts.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.error("GEMINI_API_KEY not set. Cannot download batch results.")
        return []

    genai.configure(api_key=gemini_api_key)
    client = genai.Client()

    try:
        batch_job = client.batches.get(job_id)
        
        # Check if the job has output files. Inlined responses are for small jobs.
        # For larger jobs, results are in a file. Let's get the results URI.
        # This part of the SDK interaction might need adjustment based on real-world output.
        # For this example, we'll assume we can get the result file content.
        # Let's assume we get a list of response objects.
        results = batch_job.inlined_responses
        if not results:
             # This is a placeholder for logic to fetch results from a file if not inlined
            logger.warning(f"Job {job_id} did not have inlined responses. You may need to add logic to download the result file.")
            # For example: result_file_uri = batch_job.result_file_uri
            # And then download and parse that file.
            return [] # Returning empty for now if no inlined_responses

        results_map = {}
        for result_item in results:
            key = result_item.metadata.get('key')
            
            # Ensure there are candidates and content parts
            if not result_item.candidates or not result_item.candidates[0].content.parts:
                results_map[key] = {'summary': 'N/A', 'seo_keywords': 'N/A'}
                continue

            text_part = result_item.candidates[0].content.parts[0].text
            parsed_json = None
            
            try:
                # First, try to load the whole text as JSON
                parsed_json = json.loads(text_part)
            except json.JSONDecodeError:
                # If that fails, fall back to searching for a JSON object with regex
                logger.warning(f"Could not parse the full response for key {key}. Falling back to regex search.")
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', text_part, re.DOTALL)
                if not json_match:
                    json_match = re.search(r'(\{.*?\})', text_part, re.DOTALL)

                if json_match:
                    try:
                        parsed_json = json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse extracted JSON for key {key}.")
                        parsed_json = None
            
            if parsed_json:
                results_map[key] = {
                    'summary': parsed_json.get('summary', 'N/A'),
                    'seo_keywords': ', '.join(parsed_json.get('seo_keywords', []))
                }
            else:
                results_map[key] = {'summary': 'N/A', 'seo_keywords': 'N/A'}
        
        transformed_posts = []
        for i, post in enumerate(original_posts):
            key = f"post-{i}"
            gemini_data = results_map.get(key, {})
            
            # Only update if the batch job provided a real summary
            if gemini_data.get('summary') != 'N/A':
                post['summary'] = gemini_data.get('summary')
                post['seo_keywords'] = gemini_data.get('seo_keywords')
            
            transformed_posts.append(post)

        # Sort posts by date
        posts_with_dates = [p for p in transformed_posts if p.get('publication_date') and p['publication_date'] != 'N/A']
        posts_without_dates = [p for p in transformed_posts if not p.get('publication_date') or p['publication_date'] == 'N/A']

        posts_with_dates.sort(key=lambda x: datetime.strptime(x['publication_date'], '%Y-%m-%d'), reverse=True)
        
        return posts_with_dates + posts_without_dates

    except Exception as e: # Catch a broader range of exceptions from the SDK
        logger.error(f"An error occurred downloading results for job {job_id}: {e}")
        return []

````
