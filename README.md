
```markdown
# Python Blog Scraper and Analyzer

### Project Description

This Python project is an ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, meta keywords, and content. It then uses the Gemini API to generate a concise summary and a list of SEO keywords for each article. The final, enriched data is saved to a clean `.txt` file and a `.csv` file.

The project is structured to be modular and easily expandable, allowing you to add new competitors by simply updating a single JSON configuration file.

### Prerequisites

To run this project, you will need:

* Python 3.6 or newer
* The `requests`, `beautifulsoup4`, and `python-dotenv` libraries.

You can install these libraries by running the following command in your terminal:

```

pip install -r requirements.txt

````

You will also need a Gemini API key. You can get one for free at the Google AI Studio website:
<https://aistudio.google.com/app/apikey>

### Project Setup

1.  **Clone or download** the project files to your local machine.
2.  **Create a `.env` file** in the project's root directory. This file will store your API key securely.
3.  **Add your API key** to the `.env` file with the following format, replacing the placeholder with your actual key:
    ```
    GEMINI_API_KEY="your_api_key_here"
    ```
4.  **Review the `competitor_seed_data.json` file** to see the current list of competitors. You can add new competitors to this file by following the existing format.

### How to Run the Scraper

The scraper can now be run with command-line arguments to specify the scraping duration.

Default (last 30 days):
````

python main.py

````

Specify a number of days (example 7 days):
````

python main.py 7

````

Scrape all posts (no date filter):
````

python main.py -all

````

The script will print its progress to the console. When it's finished, the generated files will be saved in the scraped directory.

````

python main.py

````

The script will print its progress to the console. When it's finished, the generated files will be saved in the `scraped` directory.

---

### Project Workflow: A Step-by-Step Breakdown

The scraper operates in a clear, modular ETL (Extract, Transform, Load) workflow, with each file handling a specific part of the process.

#### 1. The Extraction Phase (`extract.py`)

* **Configuration Reading:** The `main.py` file reads the `competitor_seed_data.json` configuration file, which contains all the necessary URLs and selectors for each competitor.
* **Existing Data Check:** The `_get_existing_urls` function checks if a previous scraper run exists by looking for a CSV file. This prevents the scraper from processing the same articles multiple times.
* **Pagination & URL Discovery:** The scraper starts at the blog's main URL and follows the pagination links. It identifies each article's unique URL on the page.
* **Deep Scraping:** For each new, unprocessed article URL, the `_get_post_details` function performs a separate request to that specific article page. It then extracts:
    * **Title:** The main heading of the article.
    * **Publication Date:** It checks for date information using multiple formats to be more resilient to a blog's HTML structure.
    * **Content:** It finds the main content area and uses a regular expression to clean up all the extra whitespace and line breaks.
    * **Meta Keywords:** It reads the `<meta name="keywords">` tag for high-quality SEO data.
* The function returns a dictionary containing all this raw, extracted data. At this stage, the `summary` and `seo_keywords` fields are placeholders.

#### 2. The Transformation Phase (`transform.py`)

* **Data Enrichment:** This file takes the raw data from `extract.py`. For each post with content, it calls the `_get_gemini_details` function to enrich the data.
* **Gemini API Call:** Using your API key from the `.env` file, the script sends the article's content to the Gemini API. It handles potential API errors with an exponential backoff retry mechanism.
* **Summary & Keywords Generation:** The API returns a generated summary and a list of keywords, which are then added to the post's dictionary.
* **Sorting:** Finally, the function sorts the entire list of articles by publication date in descending order (newest first).

#### 3. The Loading Phase (`load.py`)

* **File Creation:** This file receives the fully enriched and sorted data. It creates a new folder for each competitor and generates a unique filename that includes the date of the scrape.
* **Output:** The data is then written to a formatted `.txt` file for easy reading and a `.csv` file for data analysis. The `.csv` file fields are explicitly defined to include all the enriched data points.

### Project Files

* **`main.py`**: The central orchestrator of the project.
* **`extract.py`**: Handles the data extraction phase.
* **`transform.py`**: Manages data enrichment and processing.
* **`load.py`**: Handles the final output and file saving.
* **`competitor_seed_data.json`**: The configuration file for all competitors.
* **`.env`**: Securely stores your Gemini API key.
* **`.gitignore`**: Specifies which files and folders (like `scraped/` and `.env`) should be ignored by Git.

---

### Extending the Project

To add a new competitor, simply open the `competitor_seed_data.json` file and add a new object to the `competitors` array. You will need to provide the `name`, `url`, `base_url`, and the correct CSS selectors for the `post_list_selector` and `date_selector`.

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
      "base_url": "https://newcompetitor.com",
      "post_list_selector": "div.blog-post-card a.post-link",
      "date_selector": "span.publish-date"
    }
  ]
}
````

If the new blog has a different HTML structure, you may need to add a dedicated scraping function in `extract.py` and update the `extract_posts` router function to call it.

```
```