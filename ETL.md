# Data Pipeline Mental Model

The idea of a data pipeline is how data engineers design and build these systems. It's a way of tracking the state of each piece of work as it moves through a series of steps and being able to safely resume the process if it fails.

Think of our data pipeline like a chef preparing a meal.

* **Extraction (The Start of the Pipeline):** This is like gathering the raw ingredients from the grocery store. Our scraper's job is to find and collect the raw blog posts.
* **Transformation (The Middle of the Pipeline):** This is the chopping and seasoning of the ingredients. Our pipeline takes the raw posts and enriches them with new information, like summaries and SEO keywords from the Gemini API.
* **Loading (The End of the Pipeline):** This is plating the final dish to be served. The pipeline takes the finished posts and formats them into a final file, like a Markdown or CSV document, ready for use.

[Diagram](./BlogScraper_Architcture.pdf)
***

## Dependency Injection Architecture

To keep our pipeline organized, testable, and maintainable, we've implemented a **Dependency Injection (DI) Container** that manages all system dependencies. This modern architectural pattern ensures consistent initialization and makes the entire system more robust.

### **DIContainer (`src/di_container.py`)**
The central hub that manages the lifecycle of all system components:

* **Configuration Management**: Loads and validates all application and competitor configurations
* **Lazy Initialization**: Creates manager instances only when needed, reducing startup time
* **Dependency Resolution**: Ensures proper order of component initialization
* **Error Handling**: Provides structured exception management for the entire system

### **Manager-Based Architecture**

Each manager has one specialized responsibility, following the **Single Responsibility Principle**:

* **Scraper Manager**: Orchestrates the extraction phase and handles all scraping workflows
* **Enrichment Manager**: Decides between live/batch processing and manages content transformation
* **Batch Job Manager**: Handles the complete lifecycle of Gemini batch jobs
* **Export Manager**: Manages data export to various formats
* **State Manager**: Centralizes all data persistence operations with pluggable storage adapters

***

## Content Processing Pipeline

### **Content Preprocessing (`src/content_preprocessor.py`)**
A sophisticated system that prepares raw scraped content for API consumption:

#### **Content Cleaning**
- **Unicode Normalization**: Converts smart quotes (`'` `'` `"` `"`) to standard quotes
- **Special Character Handling**: Transforms em dashes (—), en dashes (–), ellipsis (…)
- **HTML Entity Removal**: Cleans residual HTML entities and non-printable characters
- **Whitespace Optimization**: Normalizes excessive whitespace and formatting

#### **Intelligent Content Chunking**
For posts exceeding API limits (>6,000 characters):
- **Sentence Boundary Detection**: Splits content at natural sentence endings
- **Context Preservation**: Adds continuation markers ("Continued from previous section")
- **Overlap Management**: Maintains 200-character overlap between chunks for context
- **Smart Reconstruction**: Merges chunked results back into coherent posts

#### **Result Merging**
- **Summary Integration**: Combines summaries from multiple chunks
- **Keyword Deduplication**: Merges and deduplicates SEO keywords intelligently
- **Funnel Stage Analysis**: Uses most frequent stage across chunks
- **Metadata Preservation**: Maintains all original post metadata

***

## Enhanced Error Handling System

### **Structured Exception Hierarchy**
Designed for both human operators and future LLM orchestrators:

```python
ETLError (Base)
├── ConfigurationError    # Configuration loading/validation failures
├── ScrapingError        # Website extraction issues  
├── EnrichmentError      # API and content processing failures
├── StateError           # Data persistence problems
├── BatchJobError        # Batch processing lifecycle issues
└── ExportError          # Data export formatting problems
```

### **LLM-Ready Error Responses**
All exceptions include structured data for machine consumption:
```python
{
    "error": True,
    "error_code": "ENRICHMENT_ERROR",
    "message": "Failed to enrich posts: API timeout",
    "details": {
        "competitor": "terminalfour",
        "posts_count": 15,
        "model": "gemini-2.0-flash"
    },
    "error_type": "EnrichmentError"
}
```

### **Failure Recovery System**
- **Enrichment Status Tracking**: Posts marked as 'completed', 'failed', or 'pending'
- **Smart Retry Logic**: `enrich` command automatically identifies and retries failures
- **User Guidance**: Clear recommendations provided for recovery actions
- **State Preservation**: No data loss even during catastrophic failures

***

## Intelligent Processing Modes

### **Live Mode (< 10 posts by default)**
- **Real-time Processing**: Immediate API calls with async concurrency
- **Faster Results**: No waiting for batch job processing
- **Immediate Feedback**: Instant success/failure notifications
- **Content Preprocessing**: Automatic cleaning and chunking applied

### **Batch Mode (≥ 10 posts by default)**  
- **Cost Optimization**: Leverages Gemini's batch API pricing
- **Large Scale Processing**: Handles hundreds of posts efficiently
- **Job Lifecycle Management**: Complete submission → polling → result processing
- **Chunked File Handling**: Automatically splits large jobs across multiple batch requests

### **Differential Processing**
The system intelligently processes only what needs attention:
- **New Content Detection**: Compares raw vs processed data to find unprocessed posts
- **Failed Enrichment Recovery**: Identifies and retries previously failed API calls  
- **Resource Optimization**: Avoids redundant processing of already-enriched content

***

## Data Management Evolution

### **State Manager with Adapter Pattern**
The State Manager now uses pluggable storage adapters, making it easy to switch between storage formats:

**Current Adapters:**
- **JSON Adapter**: Human-readable format with rich metadata preservation
- **CSV Adapter**: Tabular format for analysis tools

**Future Extensibility:**
- Database adapters (PostgreSQL, MongoDB)
- Cloud storage adapters (S3, Google Cloud Storage)  
- Real-time streaming adapters

### **Multi-File Storage Strategy**
- **Highly Scalable**: Each scrape creates a timestamped file for easy management
- **Failure Resilient**: Isolated file operations prevent data corruption
- **Clear State Tracking**: Easy to identify and debug individual scraping sessions
- **Performance Optimized**: Small, focused files load much faster than monolithic datasets

***

## Blog Structural Patterns (Enhanced)

The pattern-based scraping system has been refined for better maintainability:

### **Current Patterns**
* **Multi-Category Pagination**: Content organized into distinct sections with independent pagination
* **Single-List Pagination**: Chronological content stream with unified pagination
* **Single-Page Filter**: JavaScript-based content filtering (all posts loaded at once)

### **Pattern Configuration Schema**
```json
{
    "name": "competitor_name",
    "structure_pattern": "multi_category",
    "pagination_pattern": {
        "type": "linked_path",
        "selector": "a.next-page"
    },
    "content_selectors": {
        "post_list": "article.post a",
        "title": "h1.post-title", 
        "date": "time[datetime]",
        "content": "div.post-content"
    }
}
```

### **Future Patterns** 
* **Infinite Scroll**: Dynamic loading as user scrolls
* **Date-Based Archives**: Content organized by publication date
* **API-Based Sites**: Direct API integration where available
* **SPA (Single Page Applications)**: JavaScript-heavy sites requiring browser automation

***

## Enhanced CLI Process Flow

The command-line interface now provides rich feedback and intelligent guidance:

### **`python main.py get-posts --competitor "terminalfour"`**

1. **DIContainer Initialization**: Loads configurations and creates manager instances
2. **Content Extraction**: ScraperManager orchestrates pattern-based scraping
3. **Content Preprocessing**: Automatic cleaning, chunking, and optimization  
4. **Intelligent Routing**: EnrichmentManager decides live vs batch mode
5. **API Processing**: Content sent to Gemini API for enrichment
6. **Result Integration**: Chunked results merged back into complete posts
7. **State Persistence**: Final enriched data saved with comprehensive metadata
8. **User Feedback**: Clear success metrics and any failure guidance

### **`python main.py enrich --competitor "contentsis"`**

1. **Differential Analysis**: Identifies posts needing enrichment (missing data OR previous failures)
2. **Content Preprocessing**: Prepares content for API consumption
3. **Smart Processing**: Routes to appropriate enrichment mode
4. **Failure Recovery**: Automatically retries previously failed enrichments
5. **Result Merging**: Combines new results with existing processed data

### **`python main.py check-job --competitor "modern-campus"`**

1. **Job Discovery**: Finds pending batch jobs in workspace
2. **Status Polling**: Checks job completion status with Gemini API
3. **Result Processing**: Downloads and processes completed jobs
4. **Content Reconstruction**: Merges chunked results back into complete posts  
5. **State Updates**: Saves final results and cleans up temporary files

***

## Our "Golden Rules"


We follow a set of core principles that guide our architecture. They are designed to ensure our project remains robust, reliable, and easy to understand.

* **Rule #1: The Configuration is the Source of Truth.** This rule means all the tricky, website-specific details live in one place (a JSON file). So, if you need to scrape a new website, you don't have to touch the Python code—you just update the instructions in the configuration file. This keeps our code simple and clean.

### **Rule #2: Dependency Injection Over Direct Instantiation**
All system components are managed through the DI container, ensuring consistent initialization, easier testing, and better maintainability.

### **Rule #3: Content Preprocessing is Centralized** 
The Orchestrator Manages the "What", Managers Handle the "How". All content preparation for API consumption happens in one place, ensuring consistent behavior across live and batch processing modes. Our entire project talks to the Gemini API through one single door. If anything changes with the API, we only have to change the code behind that one door, and everything else in the project will continue to work.

### **Rule #4: Errors are Structured for Machine Consumption**
All exceptions include structured, machine-readable data designed for future LLM orchestrator integration.

### **Rule #5: Processing is Differential and Intelligent**
The system only processes what needs attention, avoiding redundant API calls and optimizing resource usage.

### **Rule #6: State is Preserved Through All Failures**
Comprehensive state tracking ensures no work is ever lost, and clear recovery paths are always available.

***

## Architecture Flow Diagram

```
CLI Command
    ↓
DIContainer (Dependency Resolution)
    ↓
Orchestrator (Workflow Coordination)
    ↓
┌─────────────────┬─────────────────┬─────────────────┐
│   Extract       │   Transform     │     Load        │
│                 │                 │                 │
│ ScraperManager  │ EnrichmentMgr   │ ExportManager   │
│       ↓         │       ↓         │       ↓         │
│ Pattern Scrapers│ ContentPreproc  │ Format Exports  │
│       ↓         │       ↓         │       ↓         │
│ StateManager    │ Live/BatchMgr   │ File/GSheets    │
│   (Raw Data)    │       ↓         │                 │
│                 │ APIConnector    │                 │
│                 │       ↓         │                 │
│                 │ StateManager    │                 │
│                 │ (Processed)     │                 │
└─────────────────┴─────────────────┴─────────────────┘
```

This architecture is designed to be **LLM-orchestrator ready**, with structured interfaces, comprehensive error handling, and machine-readable responses throughout the entire pipeline.






