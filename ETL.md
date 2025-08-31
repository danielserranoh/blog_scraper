# Data Pipeline Mental Model

The idea of a data pipeline is how data engineers design and build these systems. It's a way of tracking the state of each piece of work as it moves through a series of steps and being able to safely resume the process if it fails.

Think of our data pipeline like a chef preparing a meal.

* **Extraction (The Start of the Pipeline):** This is like gathering the raw ingredients from the grocery store. Our scraper's job is to find and collect the raw blog posts.
* **Transformation (The Middle of the Pipeline):** This is the chopping and seasoning of the ingredients. Our pipeline takes the raw posts and enriches them with new information, like summaries and SEO keywords from the Gemini API.
* **Loading (The End of the Pipeline):** This is plating the final dish to be served. The pipeline takes the finished posts and formats them into a final file, like a Markdown or CSV document, ready for use.

***

## A Manager-Based Architecture

To keep our pipeline organized and robust, we've designed it like a team of specialized managers. Each manager has one job and one job only, which makes the entire process predictable and easy to manage.

* **Scraper Manager:** This manager's job is to get the raw data from the websites. It hands the posts to the next manager when it's done, without worrying about what happens to them next.
* **Enrichment Manager:** This manager takes the raw posts and makes them better. Its only job is to get a summary and keywords for each post, deciding whether to do it live or in a batch.
* **Export Manager:** This manager takes the final, processed data and prepares it to be shared with the world. Its only job is to format the data into a file, like a CSV or a Markdown document.
* **State Manager:** This model centralizes all logic for saving and retrieving data across the entire pipeline. It acts as a single source of truth for all persistent data, regardless of which stage of the ETL process it's in.

***

***
Data Management

The State Manager sould be capable of string the transactional data in different formats and supports.
Hence, the State Manager is designed to use different adaptors to deal with this.

Additionally, it uses a Multi-File (Structured by Job) storyng system.
    **Pros:**

    - Highly Scalable: Each scrape or enrichment job creates a new, small file, which is much faster to read and write. This makes the system scalable to a large number of competitors and posts.

    - Resilience: A write failure only affects a single file, not the entire dataset. This aligns with your Golden Rules by making the pipeline more resilient to errors.

    - Clear State Management: Each file represents a single, complete job, which makes it easier to debug and track the state of your pipeline.the model to store the data isa 


## Blog Structural Patterns

This project uses a powerful, pattern-based system to scrape different blogs. This means we've separated the "structure" of a blog from the code itself. Instead of writing custom code for every new blog, we just need to identify which of the following patterns it follows and add a few details to our configuration file.

We have currently developed scrapers for three of these patterns.

* **Multi-Category Pagination**: This pattern is common on content-rich blogs organized into distinct sections, like `terminalfour`. You'll see a main blog page that links to several different category pages, and each category page has its own pagination.
* **Single-List Pagination**: A simpler structure, used by blogs like `modern campus`, that presents all of its content in a single chronological stream with a single set of pagination controls at the bottom.
* **Single-Page Filter**: This is a modern approach used by blogs like `squiz`. All or a very large number of posts are loaded at once, and when you click a filter, the list is instantly filtered using JavaScript. The scraper only needs to make a single request to get all the posts.

### Other Potential Patterns

For future development, we have identified other common patterns that could be implemented to make the scraper even more powerful:

* **Infinite Scroll**: New posts are loaded automatically as the user scrolls down the page.
* **Date-Based Archives**: Content is organized chronologically, with URLs often following a pattern like `example.com/blog/2025/08/`.
* **Static Site with a Search Index File**: The entire site's content is indexed in a single `.json` file that is loaded when the page starts.

***

## Process (New CLI)
The command `python main.py scrape --competitor "oho"` triggers a chain of functions that orchestrate the entire scrape and enrichment pipeline. The `main.py` file now uses the `click` library to define a clear, command-based workflow.

Here is a step-by-step list of the primary functions that are called in order:

1.  **`main.py -> cli()`**: The application starts here. The `click` library parses the command-line arguments and passes control to the specified command function, such as `scrape`.
2.  **`main.py -> scrape()`**: This function prepares the arguments into a format that the `run_pipeline` function can use and then starts the asynchronous event loop by calling `asyncio.run(run_pipeline(args))`.
3.  **`orchestrator.py -> run_pipeline(args)`**: This is the central conductor. It loads the configuration and, seeing the `scrape` flag, calls the `ScraperManager` to begin the scraping workflow.
4.  **`scraper_manager.py -> run_scrape_and_submit()`**: This method orchestrates the extraction phase. It calls the `extract_posts_in_batches` function to start the scraping process, and then passes the results to the `EnrichmentManager` and the `StateManager` that stores the results in the `data/raw/{competitor}/` folder.
5.  **`extract/__init__.py -> extract_posts_in_batches()`**: This is a router function that, based on the competitor's configuration, imports and calls the correct scraping pattern module. For "oho," it calls the `multi_category` scraper.
6.  **`multi_category.py -> scrape()`**: This function is the core of the scraper for "oho." It uses `httpx` to make asynchronous network requests to the blog's category pages. It then extracts the post links from the HTML and, for each link, creates an asynchronous task to get the post's details.
7.  **`_common.py -> _get_post_details()`**: This is a low-level function that performs the actual scraping of an individual post. It uses `httpx` to get the post's content and `BeautifulSoup` to parse the HTML and extract the title, date, content, and headings.
8.  **`scraper_manager.py -> EnrichmentManager.enrich_posts()`**: After a batch of posts has been scraped, the `ScraperManager` saves the raw data and then calls the `enrich_posts` method to begin the transformation phase.
9.  **`enrichment_manager.py -> enrich_posts()`**: This method determines whether to use live or batch mode. Since the `--scrape-all` flag is used, it will likely choose batch mode and call the `BatchJobManager`.
10. **`batch_manager.py -> submit_new_jobs()`**: This method prepares the posts for the Gemini API. It chunks the posts, creates a JSONL file, and calls the `GeminiAPIConnector` to submit the batch job.
11. **`api_connector.py -> create_batch_job()`**: This function is the final step. It uploads the JSONL file to the Gemini API and submits the batch job for processing, which is when the enrichment actually happens on the server side.

The command `python main.py check-job` triggers a chain of functions that orchestrate the batch job-checking workflow. Here is a step-by-step list of the primary functions that are called in order:

1.  **`main.py -> cli()`**: The application starts here. The `click` library parses the command-line arguments and passes control to the `check_job` function.
2.  **`main.py -> check_job()`**: This function prepares the arguments into a `SimpleNamespace` object and then starts the asynchronous event loop by calling `asyncio.run(run_pipeline(args))`.
3.  **`orchestrator.py -> run_pipeline(args)`**: This is the central conductor. It loads the configuration and, seeing the `--check-job` flag, calls the `check_and_load_results` method on the `BatchJobManager` for each competitor.
4.  **`batch_manager.py -> check_and_load_results()`**: This method orchestrates the job-checking process. It first reads the `pending_jobs.json` file to get a list of pending jobs, and then it calls the `_poll_job_statuses` method to get the status of each job.
5.  **`batch_manager.py -> _poll_job_statuses()`**: This function loops through the list of pending jobs and, for each job, calls the `check_batch_job` method on the `GeminiAPIConnector` to get the job's current status.
6.  **`api_connector.py -> check_batch_job()`**: This is the final step. It makes a direct call to the Gemini API to get the status of a specific batch job.
7.  **`batch_manager.py -> _consolidate_and_save_results()`**: If all the jobs have succeeded, this method is called. It downloads the results from the API, merges them with the original data, and saves the final processed data.
8.  **`batch_manager.py -> _cleanup_workspace()`**: Finally, after the results have been successfully saved, this method deletes all the temporary files from the workspace, completing the process.

***

## Our "Golden Rules"

We follow a set of core principles that guide our architecture. They are designed to ensure our project remains robust, reliable, and easy to understand.

* **Rule #1: The Configuration is the Source of Truth.** This rule means all the tricky, website-specific details live in one place (a JSON file). So, if you need to scrape a new website, you don't have to touch the Python code—you just update the instructions in the configuration file. This keeps our code simple and clean.
* **Rule #2: The Orchestrator Manages the "What", Managers Handle the "How".** The main `orchestrator.py` file is a simple director. It tells the managers what to do (`scrape this website`, `check for pending jobs`) but doesn't get involved in the details of how to do it. The managers handle all the complex, low-level tasks, leaving the orchestrator clean and readable.
* **Rule #3: The API Connector is the Single Gateway to Gemini.** Our entire project talks to the Gemini API through one single door. If anything changes with the API, we only have to change the code behind that one door, and everything else in the project will continue to work. This makes our code much more reliable and easier to maintain.
* **Rule #4: We care about In-Progress Work.** Our pipeline is designed to be resilient. It carefully tracks every piece of work in a special folder. If the script fails, the next time it runs, it can safely pick up exactly where it left off, so no work is ever lost.



1. `Orchestrator` tells `ScraperManager` to scrape a website.

2. ScraperManager scrapes the data and returns the raw posts and their file path to the Orchestrator.

3. Orchestrator then tells the EnrichmentManager to enrich the raw data it just received.

4.EnrichmentManager decides whether to use live or batch mode.

5. EnrichmentManager calls the Live or calls BatchJobManager to handle the batch job lifecycle.

6. Live or BatchJobManager reports to the EnrichmentManager that the job is complete.

7. EnrichmentManager reports the processed data back to the Orchestrator.

8. Orchestrator then tells the StateManager to save the final processed data.