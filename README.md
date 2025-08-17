# Python Blog Scraper and Analyzer

## Project Description
This Python project is an advanced ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, and content, then enriches this data using the Gemini API to generate concise summaries and SEO keywords.

The project is built on a modular, **manager-based architecture** that centralizes key functionality into dedicated components. This approach ensures the codebase is easy to maintain, scale, and extend. It also features a robust **dual API strategy** that intelligently switches between a high-performance "live" mode and a cost-effective "batch" mode for data enrichment.

Key architectural features include:
* **Centralized Configuration**: All site-specific logic and application settings are defined in a single location, making the core code generic and data-driven.
* **Single API Gateway**: All interactions with the Gemini API are funneled through a single, dedicated class, simplifying API management and updates.
* **Resilient Workflow**: The pipeline is designed to be safely resumed after a failure, ensuring no work is ever lost.

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
    * `competitor_seed_data.json`: A list of the competitor websites and their assigned scraping patterns.

    A `performance_log.json` file will be created automatically in the `config/` directory to store data for the time estimation feature.

## How to Run the Scraper

The script is run from the command line and offers several flags to control its behavior.

* **Default Scrape (last 30 days):**
    `python main.py`

* **Scrape All Posts for a Competitor:**
    `python main.py --all --competitor "squiz"`

* **Enrich Existing Data:**
    Finds the latest state file for a competitor and enriches any posts with missing summaries or keywords.
    `python main.py --enrich --competitor "modern campus"`

* **Check a Batch Job:**
    Checks the status of any pending batch jobs for one or all competitors.
    `python main.py --check-job --competitor "modern campus"`

* **Export Data:**
    Exports the latest scraped data for one or all competitors to a specified format. This command first checks for and processes any completed batch jobs to ensure the data is up-to-date.
    `python main.py --export txt --competitor "squiz"`
    `python main.py -e md`

## Project Workflow Breakdown

The application is architected with a clean separation between the command-line interface and the core ETL pipeline, which is now managed by specialized classes.

#### 1. Entrypoint (`main.py`)
This script's sole responsibility is to handle the command-line interface. It uses `argparse` to define and parse arguments and then calls the `run_pipeline` function in the orchestrator.

#### 2. The Orchestrator (`src/orchestrator.py`)
This is the "brains" of the application. It acts as a high-level **conductor**, loading all necessary configurations and delegating control to the appropriate manager class for each workflow (scrape, enrich, check job, or export). It contains no low-level implementation logic.

#### 3. Manager Modules
* **`src/extract/scraper_manager.py`**: This manager handles the **extraction** phase. It orchestrates the scraping process, saves the raw data, and passes the posts to the enrichment manager for processing.
* **`src/transform/enrichment_manager.py`**: This manager handles the **transformation** phase. It discovers which posts need enrichment and decides whether to use live or batch mode. It then calls the `BatchJobManager` for large jobs or a dedicated live enrichment function for small ones.
* **`src/transform/batch_manager.py`**: This manager encapsulates the entire batch job lifecycle. It handles chunking, submitting jobs, polling for status, and downloading and consolidating results.
* **`src/load/export_manager.py`**: This manager handles the **loading** phase. It reads the final, processed data from the project's data directory and formats it into user-facing output files.

#### 4. Service and Utility Layer
* **`src/api_connector.py`**: This is the single, centralized gateway to the Gemini API. All interactions, including live and batch calls, file uploads, and job management, are handled here.
* **`src/state_management/`**: Uses a flexible Adapter Pattern to save the primary data state to a canonical CSV file.
* **`src/config_loader.py`**: The single source for all application and competitor configurations.

## Extending the Project
Adding a new competitor is now incredibly simple due to the decoupled, pattern-based architecture.

1.  Open `config/competitor_seed_data.json`.
2.  Add a new JSON object for the new competitor.
3.  Fill in their `name`, `base_url`, `category_paths`, and the necessary CSS selectors.
4.  For `structure_pattern`, choose one of the existing structures (e.g., `"multi_category"`, `"single_list"`).
5.  Define the `pagination_pattern` object, choosing a `type` (e.g., `"linked_path"`, `"numeric_query"`) and providing its required parameters (e.g., `"selector"`).

In many cases, you will not need to touch any Python code.

# References:

https://ai.google.dev/gemini-api/docs/batch-mode
https://ai.google.dev/gemini-api/docs/files
https://github.com/googleapis/python-genai/blob/main/codegen_instructions.md