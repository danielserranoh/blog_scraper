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

## Our "Golden Rules"

We follow a set of core principles that guide our architecture. They are designed to ensure our project remains robust, reliable, and easy to understand.

* **Rule #1: The Configuration is the Source of Truth.** This rule means all the tricky, website-specific details live in one place (a JSON file). So, if you need to scrape a new website, you don't have to touch the Python code—you just update the instructions in the configuration file. This keeps our code simple and clean.
* **Rule #2: The Orchestrator Manages the "What", Managers Handle the "How".** The main `orchestrator.py` file is a simple director. It tells the managers what to do (`scrape this website`, `check for pending jobs`) but doesn't get involved in the details of how to do it. The managers handle all the complex, low-level tasks, leaving the orchestrator clean and readable.
* **Rule #3: The API Connector is the Single Gateway to Gemini.** Our entire project talks to the Gemini API through one single door. If anything changes with the API, we only have to change the code behind that one door, and everything else in the project will continue to work. This makes our code much more reliable and easier to maintain.
* **Rule #4: We care about In-Progress Work.** Our pipeline is designed to be resilient. It carefully tracks every piece of work in a special folder. If the script fails, the next time it runs, it can safely pick up exactly where it left off, so no work is ever lost.