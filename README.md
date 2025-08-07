````markdown
# Python Blog Scraper and Analyzer

### Project Description

This Python project is an ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, meta keywords, and content. It then uses the Gemini API to generate a concise summary and a list of SEO keywords for each article. The final, enriched data is saved to a clean `.txt` file and a `.csv` file.

The project is structured to be modular and easily expandable, allowing you to add new competitors by simply updating a single JSON configuration file.

## Project Setup

**Clone or download** the project files to your local machine.

### Prerequisites

To run this project, you will need:

* Python 3.6 or newer
* The `requests`, `beautifulsoup4`, `python-dotenv`, and `termcolor` libraries.

You can install these libraries by running the following command in your terminal:

```bash
pip install -r requirements.txt
````

You will also need a Gemini API key. You can get one for free at the Google AI Studio website:
[https://aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)



## How to Run the Scraper

The scraper can now be run with command-line arguments to specify the scraping duration and select specific competitors.

  * **Default (last 30 days for all competitors):**
    `python main.py`
  * **Specify a number of days for all competitors:**
    `python main.py 7`
  * **Scrape all posts (no date filter) for all competitors:**
    `python main.py --all`
  * **Scrape a specific competitor (default 30 days):**
    `python main.py --competitor "terminalfour"`
    or `python main.py -c "modern campus"`
  * **Scrape a specific competitor for a certain number of days:**
    `python main.py 90 --competitor "squiz"`
  * **Scrape all posts for a specific competitor:**
    `python main.py --all --competitor "terminalfour"`
  * **Show the help message:**
    `python main.py --all --competitor "terminalfour"`

The script will print its progress to the console. When it's finished, the generated files will be saved in the `scraped` directory.

-----

### Project Workflow: A Step-by-Step Breakdown

The scraper operates in a clear, modular ETL (Extract, Transform, Load) workflow, with each file handling a specific part of the process.

#### 1\. The Extraction Phase (`src/extract/`)

  * **Configuration Reading:** The `main.py` file reads the `config/competitor_seed_data.json` configuration file, which contains all the necessary URLs and selectors for each competitor.
  * **Existing Data Check (`src/extract/_common.py`):** The `_get_existing_urls` function checks if a previous scraper run exists by looking for a CSV file in the `scraped/` directory. This prevents the scraper from processing the same articles multiple times.
  * **Routing (`src/extract/__init__.py`):** The `extract_posts_in_batches` function acts as a router, dispatching the scraping task to the appropriate competitor-specific extraction function (e.g., `extract_from_terminalfour`, `extract_from_modern_campus`, `extract_from_squiz`) based on the competitor's name.
  * **Competitor-Specific Scraping (`src/extract/competitors/*.py`):**
      * Each file (e.g., `terminalfour.py`) contains the unique logic for navigating that specific blog's structure (e.g., iterating through categories, handling pagination, or performing a single-page scrape).
      * It identifies each article's unique URL on the page. The `--all` flag bypasses the date check, ensuring every page is scraped.
  * **Deep Scraping (`src/extract/_common.py`):** For each new, unprocessed article URL, the `_get_post_details` function performs a separate asynchronous request to that specific article page. It then extracts:
      * **Title:** The main heading of the article.
      * **Publication Date:** It checks for date information using multiple formats to be more resilient to a blog's HTML structure.
      * **Content:** It finds the main content area and uses a regular expression to clean up all the extra whitespace and line breaks.
      * **Meta Keywords:** It reads the `<meta name="keywords">` tag for high-quality SEO data.
  * **Batching:** As posts are extracted, they are collected into batches (defaulting to 10 posts). Once a batch is full, or at the end of a category/page, it's yielded to the transformation phase.

#### 2\. The Transformation Phase (`src/transform.py`)

  * **Data Enrichment:** This file takes each batch of raw data from `extract.py`. For each post with content, it calls the `_get_gemini_details` function to enrich the data.
  * **Gemini API Call:** Using your API key from the `.env` file, the script sends the article's content to the Gemini API. It handles potential API errors with an exponential backoff retry mechanism, specifically designed to respect quota limits by parsing `retryDelay` from `429` responses.
  * **Summary & Keywords Generation:** The API returns a generated summary and a list of 5 keywords, ordered by importance, which are then added to the post's dictionary.
  * **Sorting:** Finally, the function sorts the entire list of articles by publication date in descending order (newest first).

#### 3\. The Loading Phase (`src/load.py`)

  * **File Creation:** This file receives the fully enriched and sorted data. It creates a new folder for each competitor and generates a unique filename that includes the date of the scrape.
  * **Output:** The data is then written to a formatted `.txt` file for easy reading and a `.csv` file for data analysis. The `.csv` file fields are explicitly defined to include all the enriched data points.

### Project Files

  * **`main.py`**: The central orchestrator of the project.
  * **`config/competitor_seed_data.json`**: The configuration file for all competitors.
  * **`src/`**: Contains all core Python source code.
      * **`src/__init__.py`**: Makes `src` a Python package.
      * **`src/extract/`**: Extraction module.
          * **`src/extract/__init__.py`**: The main extraction router (`extract_posts_in_batches`).
          * **`src/extract/_common.py`**: Shared helper functions for extraction.
          * **`src/extract/competitors/`**: Directory for competitor-specific scrapers.
              * **`src/extract/competitors/__init__.py`**: Makes `competitors` a package.
              * **`src/extract/competitors/terminalfour.py`**: TerminalFour's scraping logic.
              * **`src/extract/competitors/moderncampus.py`**: Modern Campus's scraping logic.
              * **`src/extract/competitors/squiz.py`**: Squiz's scraping logic.
      * **`src/transform.py`**: Manages data enrichment and processing.
      * **`src/load.py`**: Handles the final output and file saving.
  * **`.env`**: Securely stores your Gemini API key.
  * **`.gitignore`**: Specifies which files and folders (like `scraped/` and `.env`) should be ignored by Git.

### Extending the Project

To add a new competitor, simply open the `config/competitor_seed_data.json` file and add a new object to the `competitors` array. You will need to provide the `name`, `urls` (a list of starting URLs or category paths), `base_url`, and the correct CSS selectors for the `post_list_selector` and `date_selector`.

**Example:**

```json
{
  "competitors": [
    {
      "name": "new_competitor",
      "urls": [
        "blog/posts/",
        "blog/cats/new-category/"
      ],
      "base_url": "[https://newcompetitor.com](https://newcompetitor.com)",
      "post_list_selector": "div.blog-post-card a.post-link",
      "date_selector": "span.publish-date"
    }
  ]
}
```

If the new blog has a different HTML structure (e.g., different pagination, or content location), you will need to create a new Python file in `src/extract/competitors/` for its specific scraping logic and update `src/extract/__init__.py` to import and route to it.