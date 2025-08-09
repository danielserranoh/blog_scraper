
# Python Blog Scraper and Analyzer

## Project Description

This Python project is an advanced ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, and content, then enriches this data using the Gemini API to generate concise summaries and SEO keywords.

The scraper features a sophisticated, dual-path system for API interaction:
* **Live Processing**: For small jobs, it uses a high-performance, asynchronous workflow to get results in real-time.
* **Batch Processing**: For large jobs, it uses the cost-effective Gemini Batch API.

A key feature of this tool is its intelligent, interactive command-line interface. When a batch job is submitted, the scraper provides a **self-improving time estimate** based on the performance of previous jobs. It then gives the user the choice to wait for the results or check on the job later, creating a seamless and user-friendly experience.

## Project Setup

**Clone or download** the project files to your local machine.

### Prerequisites

1.  **Python and Libraries**: You will need Python 3.6 or newer. You can install all the required libraries by running:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Gemini API Key**: You will need a Gemini API key from the Google AI Studio. Create a `.env` file in the project's root directory and add your key:
    ```
    GEMINI_API_KEY=your_api_key_here
    ```

3.  **Configuration Files**: The project uses two main configuration files in the `config/` directory:
    * `config.json`: For application settings like the batch threshold and model names.
    * `competitor_seed_data.json`: A list of the competitor websites to be scraped.

    A `performance_log.json` file will be created automatically in the `config/` directory to store data for the time estimation feature.

## How to Run the Scraper

The script is run from the command line and offers several flags to control its behavior.

* **Default Scrape (last 30 days):**
    `python main.py`

* **Scrape All Posts for a Competitor:**
    `python main.py --all --competitor "squiz"`

* **Enrich Existing Data:**
    Finds the latest CSV for a competitor and enriches any posts with missing summaries or keywords.
    `python main.py --enrich --competitor "modern campus"`

* **Check a Batch Job:**
    If you previously submitted a batch job and chose not to wait, you can check its status with this command.
    `python main.py --check-job --competitor "modern campus"`

### Interactive Polling

When you submit a batch job (either through scraping or enrichment), the script will provide an estimated completion time and ask if you want to wait.

```

INFO: Based on previous jobs, the estimated completion time is \~5.2 minutes.
? Do you want to start polling for the results now? (y/n):

```
* Answering **`y`** will start an efficient polling process that waits for the job to complete.
* Answering **`n`** will exit the script, allowing you to run the `--check-job` command later.

## Project Workflow Breakdown

The scraper operates in a clear, modular ETL workflow, orchestrated by `main.py`.

#### 1. Configuration & Setup (`main.py`)

* The script loads settings from `config.json`, competitor data from `competitor_seed_data.json`, and performance metrics from `performance_log.json`.
* It parses command-line arguments to determine the desired action (scrape, enrich, or check job).

#### 2. Extraction Phase (`src/extract/`)

* Uses `asyncio` and `httpx` for high-performance, asynchronous scraping of blog pages.
* It checks against previously scraped URLs (from the latest CSV) to avoid redundant work.
* Competitor-specific logic is modularized in the `src/extract/competitors/` directory.

#### 3. Transformation Phase (`src/transform/`)

The script intelligently chooses one of two paths based on the `batch_threshold` setting:

* **Live Processing (`src/transform/live.py`)**: For small jobs, it uses the `google.genai.GenerativeModel` to make concurrent, asynchronous API calls for fast results.
* **Batch Processing (`src/transform/batch.py`)**: For large jobs, it uses the `google.genai.Client` to create, submit, and manage a batch job. This is slower but more cost-effective.

#### 4. Loading Phase (`src/load.py`)

* The final, enriched data is sorted by publication date.
* The results are saved to both a formatted `.txt` file for easy reading and a `.csv` file for data analysis in the `scraped/<competitor_name>/` directory.

#### 5. Performance Logging (`main.py`)

* When a batch job is completed via the interactive polling, the script calculates the actual time taken.
* It then calls `_update_performance_log` to save this new data, making future time estimates more accurate.

## Project Files

* **`main.py`**: The central orchestrator of the project.
* **`config/`**:
    * `config.json`: Core application settings (batch threshold, models).
    * `competitor_seed_data.json`: Data for competitor websites.
    * `performance_log.json`: (auto-generated) Stores data for time estimates.
* **`src/`**: Contains all core Python source code.
    * **`extract/`**: Module for the asynchronous scraping phase.
    * **`transform/`**: Module for the data enrichment phase, split into `live.py` and `batch.py`.
    * **`load.py`**: Handles the final output and file saving.
* **`.env`**: Securely stores your `GEMINI_API_KEY`.
* **`requirements.txt`**: A list of all Python dependencies.
* **`tests/`**: Contains unit tests for the project.

