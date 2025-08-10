# Python Blog Scraper and Analyzer

## Project Description

This Python project is an advanced ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, and content, then enriches this data using the Gemini API to generate concise summaries and SEO keywords.

The architecture is built around a powerful **pattern-based system**, allowing it to be easily extended to scrape new blogs that follow common structural patterns (e.g., multi-page categories, single-page lists).

Other key features include:
* **Dual API Strategy**: Intelligently switches between a high-performance "live" mode for small jobs and a cost-effective "batch" mode for large jobs.
* **Self-Improving Time Estimation**: The scraper learns from the performance of previous batch jobs to provide increasingly accurate time estimates.
* **Interactive CLI**: When a batch job is submitted, the user is given a time estimate and the choice to either wait for the results or check on the job later.

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
    * `competitor_seed_data.json`: A list of the competitor websites and their assigned `scraping_pattern`.

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

* **Export Data:**
    Exports the latest scraped data for one or all competitors to a specified format.
    `python main.py --export txt --competitor "squiz"`
    `python main.py --export md`

## Project Workflow Breakdown

The application is architected with a clean separation between the command-line interface, the core orchestrator, and the modular ETL components.

#### 1. Entrypoint (`main.py`)

* This script's sole responsibility is to handle the command-line interface. It uses `argparse` to define and parse arguments and then calls the main orchestrator.

#### 2. Orchestrator (`src/orchestrator.py`)

* This is the "brains" of the application. It loads all necessary configurations and determines which workflow to run (scrape, enrich, check job, or export).

#### 3. Extraction Phase (`src/extract/`)

* The main router in this module reads the `scraping_pattern` for a given competitor from the configuration.
* It then dynamically calls the correct scraper module from the `src/extract/blog_patterns/` directory to handle the specific HTML structure of that blog.

#### 4. Transformation & Loading

* **`src/transform/`**: Enriches the data using the Gemini API, intelligently switching between live and batch modes.
* **`src/load/`**: Uses a flexible **Adapter Pattern** to save the primary data state to a CSV file in the `state/` directory.
* **`src/exporters.py`**: Contains logic to format the primary data into user-facing outputs like `.txt`, `.md`, and `.json` files, which are saved in the `exports/` directory.

## Extending the Project

Adding a new competitor is now incredibly simple, especially if they use an existing blog structure.

#### To Add a New Competitor (Using an Existing Pattern):

1.  Open `config/competitor_seed_data.json`.
2.  Add a new JSON object for the new competitor.
3.  Fill in their `name`, `base_url`, `urls`, and `post_list_selector`.
4.  For the `scraping_pattern`, choose one of the existing patterns:
    * `"multi_category_pagination"`
    * `"single_list_pagination"`
    * `"single_page_filter"`

That's it! You do not need to touch any Python code.

## Project Files

* **`main.py`**: The command-line entrypoint.
* **`src/orchestrator.py`**: The central orchestrator containing the core application logic.
* **`config/`**:
    * `config.json`: Core application settings.
    * `competitor_seed_data.json`: Data and pattern assignments for competitors.
    * `performance_log.json`: (auto-generated) Stores data for time estimates.
* **`src/`**: Contains all core Python source code.
    * **`extract/`**: Module for the asynchronous scraping phase.
        * `blog_patterns/`: Contains the different scraping pattern modules (e.g., `multi_category_pagination.py`).
    * **`transform/`**: Module for the data enrichment phase.
    * **`load/`**: Contains the Storage Adapter for saving state.
    * **`exporters.py`**: Contains logic for creating user-facing output files.
* **`.env`**: Securely stores your `GEMINI_API_KEY`.
* **`requirements.txt`**: A list of all Python dependencies.
* **`tests/`**: Contains unit tests for the project.
* **`state/`**: (auto-generated) Stores the primary data state in CSV files.
* **`exports/`**: (auto-generated) Stores the final, user-facing output files.
* **`workspace/`**: (auto-generated) Stores temporary files during batch processing.