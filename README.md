# Python Blog Scraper and Analyzer

## Project Description
This Python project is an advanced ETL (Extract, Transform, Load) pipeline designed to scrape blog posts from multiple competitor websites. It gathers key information such as the title, URL, publication date, and content, then enriches this data using the Gemini API to generate comprehensive strategic analysis including summaries, SEO keywords, funnel stage classification, and **advanced content intelligence** featuring content angles, competitive differentiation analysis, freshness scoring, and detailed content structure metrics.

The project is built on a **mature manager-based architecture** with **dependency injection**, **intelligent content preprocessing**, and **comprehensive error handling**. It features a robust **dual API strategy** that intelligently switches between high-performance "live" mode and cost-effective "batch" mode for data enrichment.

**Key architectural features include:**
* **Dependency Injection Container**: Centralized management of all system dependencies
* **Advanced Content Analysis**: Automatic content metrics calculation including word count, reading time, structure analysis, and complexity scoring
* **Intelligent Content Preprocessing**: Automatic content cleaning, chunking, and optimization for API consumption with configurable parameters
* **Strategic Intelligence Generation**: Comprehensive competitive analysis with content angle classification, freshness scoring, and differentiation analysis
* **Centralized Configuration**: All site-specific logic and application settings are defined in a single location
* **Single API Gateway**: All interactions with the Gemini API are funneled through a dedicated, robust connector
* **LLM-Ready Error Handling**: Structured exception system designed for future LLM orchestrator integration
* **Resilient Workflow**: The pipeline can safely resume after failures with comprehensive state tracking

## Project Setup

**Clone or download** the project files to your local machine.

### Prerequisites

1.  **Python and Libraries**: You will need Python 3.8 or newer. You can install all the required libraries by running:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Gemini API Key**: You will need a Gemini API key from the Google AI Studio. Create a `.env` file in the project's root directory and add your key:
    ```
    GEMINI_API_KEY=your_api_key_here
    ```

3.  **Configuration Files**: The project uses two main configuration files in the `config/` directory:
    * `config.json`: For application settings like the batch threshold, model names, and a list of general DXP competitors to aid in funnel stage analysis.
    * `competitor_data.json`: A list of the competitor websites and their assigned scraping patterns.

    A `performance_log.json` file will be created automatically in the `config/` directory to store data for the time estimation feature.

## How to Run the Scraper

The script uses the `click` library for a streamlined command-line interface. All commands are subcommands of the main `main.py` entry point.

### Core Commands

* **Full Pipeline (Scrape + Enrich + Save):**
    ```bash
    python main.py get-posts --competitor "terminalfour"
    python main.py get-posts --all --competitor "squiz"  # All historical posts
    ```

* **Scrape Only (No Enrichment):**
    ```bash
    python main.py scrape --competitor "terminalfour"
    python main.py scrape --all --competitor "squiz"
    ```

* **Enrich Existing Data:**
    ```bash
    python main.py enrich --competitor "modern campus"           # Enrich processed data
    python main.py enrich --raw --competitor "contentsis"       # Enrich raw data
    ```

* **Check Batch Job Status:**
    ```bash
    python main.py check-job --competitor "modern campus"
    ```

* **Export Data:**
    ```bash
    python main.py export --format md --competitor "squiz"
    python main.py export --format csv    # All competitors
    python main.py export --format strategy-brief    # Strategic intelligence report
    python main.py export --format content-gaps      # Content gap analysis
    ```

* **Analyze Data:** ✨ *NEW*
    ```bash
    python main.py analyze --gaps             # Interactive content gap analysis
    python main.py analyze --strategy         # Interactive strategic intelligence
    python main.py analyze --gaps --competitor "terminalfour"  # Competitor-focused analysis
    ```

### Command Options

* `--competitor`, `-c`: Specify a single competitor to process
* `--all`, `-a`: Process all available posts (overrides default 30-day limit)
* `--days`, `-d`: Specify number of days to scrape (default: 30)
* `--wait`: Wait for batch jobs to complete before exiting
* `--raw`: For enrich command, process raw data instead of processed data
* `--gaps`: For analyze command, perform content gap analysis
* `--strategy`: For analyze command, generate strategic intelligence brief

## 📖 User Guide

For detailed CLI usage examples, workflows, and best practices, see **[USER_GUIDE.md](USER_GUIDE.md)**.

## Architecture Overview

The project follows a clean **manager-based architecture** with clear separation of concerns:

### Dependency Injection Container (`src/di_container.py`)
Centrally manages all system dependencies with lazy initialization:
- **Configuration Loading**: Validates and loads all application settings
- **Manager Lifecycle**: Creates and manages all manager instances
- **Error Handling**: Provides structured exceptions for LLM consumption

### Core Managers

#### Extract Layer (`src/extract/`)
* **`ScraperManager`**: Orchestrates the extraction workflow, manages scraping processes
* **`extract/__init__.py`**: Pattern-based router that dynamically loads appropriate scrapers
* **Blog Patterns**: Modular scrapers for different website structures (multi-category, single-list, single-page)

#### Transform Layer (`src/transform/`)
* **`EnrichmentManager`**: Orchestrates content enrichment, decides between live/batch modes
* **`ContentPreprocessor`**: Intelligently prepares content for API consumption with cleaning and chunking
* **`BatchJobManager`**: Manages complete batch job lifecycle from submission to result processing  
* **`live.py`**: Handles real-time API enrichment for small datasets

#### Load Layer (`src/load/`)
* **`ExportManager`**: Handles data export to multiple formats
* **`exporters.py`**: Format-specific export logic (Markdown, CSV, JSON, Google Sheets)

### State Management (`src/state_management/`)
* **`StateManager`**: Centralized data persistence with adapter pattern
* **Storage Adapters**: Pluggable storage backends (JSON, CSV) with consistent interface

### Content Processing Pipeline

1. **Raw Content Extraction**: Scrapers extract content using pattern-based selectors
2. **Advanced Content Preprocessing**: 
   - Unicode character normalization (smart quotes, em dashes, etc.)
   - Content metrics calculation (word count, reading time, sentence complexity)
   - Content structure analysis (headings, paragraphs, lists, media detection)
   - Content length optimization with intelligent chunking at sentence boundaries (configurable limits up to 50,000 characters)
   - Problematic pattern detection and cleaning
3. **Strategic API Enrichment**: Content sent to Gemini API for comprehensive analysis including:
   - Content summaries and SEO keyword extraction
   - Funnel stage classification (ToFu/MoFu/BoFu)
   - Content angle classification (How-to Guide, Case Study, Thought Leadership, etc.)
   - Competitive differentiation analysis
   - Content freshness scoring (1-10 scale for industry relevance)
   - Target persona indicators (Technical/Marketing/Business/Mixed)
   - Content depth assessment (Surface/Intermediate/Deep)
4. **Result Merging**: Chunked content results intelligently merged back into complete posts with deduplicated analysis
5. **Enhanced State Persistence**: Final enriched data saved with comprehensive metadata structure including content processing metrics and strategic analysis

## Advanced Content Intelligence & Analysis

The system includes sophisticated content analysis and preprocessing capabilities:

### Content Analysis Features ✨ *NEW*
* **Comprehensive Content Metrics**: Automatic calculation of word count, reading time, sentence complexity, and structure analysis
* **Content Structure Detection**: Identifies headings, paragraphs, lists, images, code blocks, and links for richness assessment
* **Strategic Content Classification**: AI-powered analysis of content angles, depth levels, target personas, and competitive differentiation
* **Content Quality Scoring**: Freshness scoring (1-10) for industry relevance and timeliness assessment
* **Readability Analysis**: Sentence complexity scoring for audience accessibility evaluation

### Enhanced Content Preprocessing Features
* **Smart Character Cleaning**: Normalizes smart quotes, em dashes, and other Unicode characters
* **Optimized Intelligent Chunking**: Automatically splits oversized content (configurable up to 50,000 characters) at sentence boundaries for optimal AI analysis
* **Context Preservation**: Adds continuation markers to maintain narrative flow across chunks
* **Advanced Merge-Back Logic**: Reconstructs chunked results into complete posts with deduplicated keywords and merged strategic analysis

### API Optimization
* **Length Validation**: Ensures content fits within API token limits
* **Retry Logic**: Robust error handling with exponential backoff
* **Failure Detection**: Distinguishes between API failures and missing content
* **Recovery Guidance**: Provides clear next steps for failed enrichments

## Error Handling & Recovery

The system features comprehensive error handling designed for reliability and future LLM integration:

### Structured Exceptions
* **ETLError**: Base exception with machine-readable error codes
* **ScrapingError**: Website-specific scraping failures  
* **EnrichmentError**: API and content processing failures
* **StateError**: Data persistence issues
* **BatchJobError**: Batch processing failures

### Failure Recovery
* **Failed Enrichment Tracking**: System tracks which posts failed enrichment vs. missing data
* **Intelligent Retry**: `enrich` command automatically identifies and retries failed posts
* **User Guidance**: Clear recommendations provided when failures occur
* **State Preservation**: No data loss even during failures

## Blog Structural Patterns

The scraper uses a powerful, pattern-based system that separates website structure from code implementation:

### Supported Patterns
* **Multi-Category Pagination**: Content organized into distinct sections (e.g., terminalfour)
* **Single-List Pagination**: Chronological content stream (e.g., modern campus) 
* **Single-Page Filter**: JavaScript-based filtering (e.g., squiz)

### Future Patterns
* **Infinite Scroll**: Dynamic content loading
* **Date-Based Archives**: Chronological organization
* **Static Site with Search Index**: JSON-based content indexing

## Extending the Project

Adding a new competitor requires only configuration changes:

1. Open `config/competitor_data.json`
2. Add new competitor configuration with:
   - `name`, `base_url`, `category_paths`  
   - CSS selectors for content extraction
   - `structure_pattern` (choose existing pattern)
   - `pagination_pattern` configuration

**No Python code changes required** in most cases due to the pattern-based architecture.

## Performance Features

* **Differential Processing**: Only processes new/failed content, avoiding redundant API calls
* **Batch Optimization**: Automatically switches to batch mode for large datasets
* **Performance Tracking**: Maintains metrics for time estimation
* **Content Caching**: Intelligent detection of previously processed content

## Data Export Options

The system supports multiple export formats designed for both human analysis and agent consumption:

### **Standard Formats**
* **Markdown (md)**: Enhanced competitive intelligence with strategic indicators
* **CSV**: Structured data for analysis and spreadsheet import
* **JSON**: Machine-readable format for programmatic consumption
* **Google Sheets**: Direct integration with Google Workspace
* **Plain Text**: Simple text format for basic consumption

### **Strategic Intelligence Formats** ✨ *ENHANCED*
* **Strategy Brief (strategy-brief)**: Executive-level competitive intelligence report featuring:
  - **Content Quality & Structure Analysis**: Word count benchmarking, reading time distribution, content complexity analysis
  - **Enhanced Competitor Profiling**: Content types, quality metrics, freshness ratings, and strategic positioning
  - **Market Intelligence**: Publishing velocity benchmarks, content length strategies, and competitive differentiation insights
  - **Strategic Opportunities**: Data-driven recommendations based on comprehensive content analysis
  
* **Content Gap Analysis (content-gaps)**: Advanced data-driven opportunity identification including:
  - **Strategic Content Gaps**: Missing content angles, depth levels, and complexity analysis by competitor
  - **Content Quality Benchmarking**: Comprehensive comparison tables with word count ranges, freshness scores, and readability metrics
  - **Topic Coverage Analysis**: Underserved content areas with specific strategic recommendations
  - **Competitive Positioning**: Content strategy insights based on structure, complexity, and quality metrics

### **Enhanced Competitive Intelligence Report** ✨ *NEW*
The markdown export now includes comprehensive content analysis:
- **Content Length Distribution**: Detailed breakdown of content categories (Micro/Short/Medium/Long/Deep Dive) with visual distribution charts
- **Length Strategy by Competitor**: Individual competitor content strategies and distribution patterns
- **Content Complexity & Structure Overview**: Reading time analysis, structure scoring, and readability benchmarking
- **Funnel Stage Content Preferences**: Optimal content lengths and complexity for each funnel stage
- **Strategic Market Intelligence**: Actionable insights for content strategy optimization

## Content Strategy Workflow Integration

The enhanced export system is specifically designed to support agent-driven content strategy workflows:

### **For Content Strategy Agents**
- **Structured Intelligence**: All exports include machine-readable strategic insights
- **Gap Identification**: Automated detection of content opportunities and competitive weaknesses
- **Trend Analysis**: Publishing velocity and topic evolution tracking
- **Strategic Context**: Each content piece includes competitive positioning data

### **Workflow Examples**
```bash
# Generate weekly competitive intelligence briefing
python main.py export --format strategy-brief

# Identify content opportunities for strategic planning
python main.py export --format content-gaps

# Create competitor-specific analysis for campaign planning
python main.py export --format strategy-brief --competitor "terminalfour"

# Export enhanced markdown for agent consumption and refinement
python main.py export --format md
```

### **Agent-Ready Data Structure**
All strategic exports include enhanced intelligence:
- **Quantified Content Insights**: Word count benchmarks, reading time analysis, complexity scores, content structure metrics
- **Strategic Content Analysis**: Content angles, freshness scores, competitive differentiation insights, depth assessments
- **Quality Benchmarking**: Comparative analysis of content strategies, readability levels, and structural approaches
- **Actionable Recommendations**: Data-driven strategic suggestions based on comprehensive content intelligence
- **Competitive Context**: Relative positioning with detailed content quality and strategy comparisons
- **Market Intelligence**: Content length distributions, complexity patterns, and strategic opportunity identification

# Future Architecture Notes

The current architecture is designed to be **LLM-orchestrator ready**:
- **Structured Error Responses**: All exceptions include machine-readable details
- **Manager Interface Standardization**: Consistent method signatures across managers
- **Dependency Injection**: Easy to swap implementations for testing or enhancement
- **Result Schema Definition**: Standardized return formats for LLM consumption

# References

- [Gemini Batch API Documentation](https://ai.google.dev/gemini-api/docs/batch-mode)
- [Gemini File API Documentation](https://ai.google.dev/gemini-api/docs/files)
- [Google GenAI Python SDK](https://github.com/googleapis/python-genai/blob/main/codegen_instructions.md)