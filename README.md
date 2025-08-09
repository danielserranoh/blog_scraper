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

````

INFO: Based on previous jobs, the estimated completion time is \~5.2 minutes.
? Do you want to start polling for the results now? (y/n):

```
* Answering **`y`** will start an efficient polling process that waits for the job to complete.
* Answering **`n`** will exit the script, allowing you to run the `--check-job` command later.

## Project Workflow Breakdown

The application is architected with a clean separation between the command-line interface and the core business logic.

#### 1. Entrypoint (`main.py`)

* This script's sole responsibility is to handle the command-line interface.
* It uses `argparse` to define and parse arguments.
* It performs initial argument validation.
* It then calls the main `run_pipeline` function from the orchestrator, passing the parsed arguments.

#### 2. Orchestrator (`src/orchestrator.py`)

* This is the "brains" of the application and contains all the core ETL logic.
* It loads all necessary configurations.
* It determines which workflow to run (scrape, enrich, or check job) based on the arguments.
* It executes the workflow, delegating to the `extract`, `transform`, and `load` modules.
* It manages the interactive polling and performance logging features.

#### 3. ETL Modules (`src/`)

* **Extraction (`src/extract/`)**: Uses `asyncio` and `httpx` for high-performance, asynchronous scraping of blog pages.
* **Transformation (`src/transform/`)**: Intelligently chooses between live processing (`live.py`) and batch processing (`batch.py`) based on the job size.
* **Loading (`src/load.py`)**: Saves the final, enriched data to `.txt` and `.csv` files.

## Project Files

* **`main.py`**: The command-line entrypoint.
* **`src/orchestrator.py`**: The central orchestrator containing the core application logic.
* **`config/`**:
    * `config.json`: Core application settings.
    * `competitor_seed_data.json`: Data for competitor websites.
    * `performance_log.json`: (auto-generated) Stores data for time estimates.
* **`src/`**: Contains all core Python source code.
    * **`extract/`**: Module for the asynchronous scraping phase.
    * **`transform/`**: Module for the data enrichment phase.
    * **`load.py`**: Handles the final output and file saving.
* **`.env`**: Securely stores your `GEMINI_API_KEY`.
* **`requirements.txt`**: A list of all Python dependencies.
* **`tests/`**: Contains unit tests for the project.
