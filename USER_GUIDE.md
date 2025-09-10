# 📖 Blog Scraper User Guide

Welcome to the Blog Scraper ETL Pipeline! This guide will help you get started with all the features and commands available for **advanced competitive content intelligence** including comprehensive content analysis, strategic insights, and quality benchmarking.

## 🚀 Quick Start

### Prerequisites
1. **Python 3.8+** installed
2. **Virtual environment** activated: `source .venv/bin/activate`
3. **Environment setup** complete (see [README.md](README.md))

### Basic Workflow
```bash
# 1. Scrape and enrich competitor content
python main.py get-posts --competitor "terminalfour"

# 2. Analyze for strategic insights
python main.py analyze --gaps

# 3. Export for sharing
python main.py export --format strategy-brief
```

---

## 📋 Complete Command Reference

### 🔍 **Data Collection Commands**

#### `get-posts` - Full Pipeline (Scrape + Enrich + Save)
```bash
# Scrape last 30 days from single competitor
python main.py get-posts --competitor "terminalfour"

# Scrape all historical posts
python main.py get-posts --all --competitor "squiz"

# Scrape specific time period
python main.py get-posts --days 7 --competitor "contentsis"

# Process all competitors
python main.py get-posts

# Wait for batch jobs to complete
python main.py get-posts --competitor "modern campus" --wait
```

#### `scrape` - Data Collection Only
```bash
# Scrape without enrichment (faster)
python main.py scrape --competitor "terminalfour"
python main.py scrape --all --competitor "squiz"
```

#### `enrich` - Content Enhancement
```bash
# Enrich existing processed data
python main.py enrich --competitor "terminalfour"

# Enrich raw scraped data  
python main.py enrich --raw --competitor "contentsis"

# Wait for batch processing
python main.py enrich --competitor "squiz" --wait
```

#### `check-job` - Batch Job Management
```bash
# Check pending batch jobs
python main.py check-job --competitor "modern campus"
python main.py check-job  # All competitors
```

---

### 📊 **Analysis Commands** ✨

#### `analyze` - Interactive Strategic Intelligence
```bash
# Content gap analysis (terminal display)
python main.py analyze --gaps

# Strategic intelligence brief (terminal display)
python main.py analyze --strategy

# Competitor-focused analysis
python main.py analyze --gaps --competitor "terminalfour"
python main.py analyze --strategy --competitor "squiz"
```

**When to use `analyze`:**
- Quick strategic insights during planning meetings
- Interactive terminal-based analysis
- Real-time competitive intelligence
- Command-line workflows

---

### 💾 **Export Commands**

#### `export` - Generate Shareable Reports
```bash
# Standard formats
python main.py export --format md --competitor "squiz"
python main.py export --format csv    # All competitors
python main.py export --format json
python main.py export --format txt

# Strategic intelligence formats
python main.py export --format strategy-brief
python main.py export --format content-gaps

# Google Sheets integration
python main.py export --format gsheets
```

**When to use `export`:**
- Creating reports for distribution
- Agent consumption workflows  
- Archival and documentation
- Sharing with stakeholders

---

## 🎯 Strategic Workflows

### **Weekly Competitive Intelligence Briefing**
```bash
# 1. Update all competitor data
python main.py get-posts

# 2. Generate executive summary
python main.py export --format strategy-brief

# 3. Quick terminal analysis
python main.py analyze --gaps
```

### **Content Strategy Planning Session**
```bash
# 1. Interactive gap analysis
python main.py analyze --gaps

# 2. Export detailed report for team
python main.py export --format content-gaps

# 3. Focus on specific competitor
python main.py analyze --strategy --competitor "terminalfour"
```

### **Campaign-Specific Research**
```bash
# 1. Target competitor analysis
python main.py get-posts --competitor "modern campus"

# 2. Strategic positioning insights
python main.py analyze --strategy --competitor "modern campus"

# 3. Export for campaign brief
python main.py export --format strategy-brief --competitor "modern campus"
```

### **Monthly Competitive Review**
```bash
# 1. Collect recent activity (last 7 days)
python main.py get-posts --days 7

# 2. Comprehensive gap analysis
python main.py export --format content-gaps

# 3. Strategic recommendations
python main.py export --format strategy-brief
```

### **Content Quality Benchmarking** ✨ *NEW*
```bash
# 1. Update all competitor content with enhanced analysis
python main.py get-posts

# 2. Generate enhanced competitive intelligence report
python main.py export --format md

# 3. Analyze content quality gaps
python main.py export --format content-gaps

# 4. Quick terminal analysis of content strategies
python main.py analyze --strategy
```

### **Content Strategy Optimization** ✨ *NEW*  
```bash
# 1. Focus on specific competitor for deep analysis
python main.py get-posts --competitor "terminalfour"

# 2. Analyze their content length and complexity strategies
python main.py export --format md --competitor "terminalfour"

# 3. Identify content angle and depth gaps
python main.py export --format content-gaps --competitor "terminalfour"

# 4. Generate strategic brief with quality benchmarks
python main.py export --format strategy-brief --competitor "terminalfour"
```

---

## 📋 Command Options Reference

| Option | Short | Commands | Description |
|--------|-------|----------|-------------|
| `--competitor` | `-c` | All | Target specific competitor |
| `--all` | `-a` | get-posts, scrape | Process all historical posts |
| `--days` | `-d` | get-posts, scrape | Specify time period (default: 30) |
| `--wait` | | get-posts, enrich | Wait for batch jobs to complete |
| `--raw` | | enrich | Process raw data instead of processed |
| `--gaps` | | analyze | Perform content gap analysis |
| `--strategy` | | analyze | Generate strategic intelligence |
| `--format` | `-f` | export | Specify export format |

---

## 🎯 Export Format Guide

### **Standard Formats**

#### **Markdown (`md`)**
- **Enhanced**: Competitive intelligence with strategic indicators
- **Features**: Market metrics, activity trends, content organization
- **Best for**: Agent consumption, documentation, sharing
- **Output**: `competitor-YYMMDD.md`

#### **CSV (`csv`)**
- **Purpose**: Structured data analysis
- **Features**: All post fields in tabular format
- **Best for**: Spreadsheet analysis, data processing
- **Output**: `competitor-YYMMDD.csv`

#### **JSON (`json`)**
- **Purpose**: Programmatic consumption
- **Features**: Complete structured data
- **Best for**: API integration, custom processing
- **Output**: `competitor-YYMMDD.json`

### **Strategic Intelligence Formats** ✨ *ENHANCED*

#### **Strategy Brief (`strategy-brief`)**
- **Purpose**: Executive-level competitive intelligence with advanced content analysis
- **Enhanced Features**: 
  - **Content Quality & Structure Analysis**: Word count benchmarking, reading time distribution, complexity scoring
  - **Enhanced Competitor Profiling**: Content types, quality metrics, freshness ratings, strategic positioning
  - **Market Intelligence**: Publishing velocity, content length strategies, competitive differentiation insights
  - **Strategic Opportunities**: Data-driven recommendations based on comprehensive content analysis
- **Best for**: Leadership briefings, strategic planning, content strategy development
- **Output**: `competitor-strategy-brief-YYMMDD.md`

#### **Content Gaps (`content-gaps`)**
- **Purpose**: Advanced data-driven opportunity identification with quality benchmarking
- **Enhanced Features**:
  - **Strategic Content Gaps**: Missing content angles, depth levels, complexity analysis by competitor
  - **Content Quality Benchmarking**: Comprehensive comparison tables with word count ranges, freshness scores, readability metrics  
  - **Topic Coverage Analysis**: Underserved content areas with specific strategic recommendations
  - **Competitive Positioning**: Content strategy insights based on structure, complexity, and quality metrics
- **Best for**: Content strategy planning, editorial calendars, competitive positioning
- **Output**: `competitor-content-gaps-YYMMDD.md`

#### **Enhanced Competitive Intelligence Report (`md`)** ✨ *NEW*
- **Purpose**: Comprehensive competitive analysis with detailed content intelligence
- **Advanced Features**:
  - **Content Length Distribution**: Detailed breakdown with visual charts (Micro/Short/Medium/Long/Deep Dive content)
  - **Content Complexity & Structure Overview**: Reading time analysis, structure scoring, readability benchmarking
  - **Length Strategy by Competitor**: Individual competitor strategies and distribution patterns
  - **Funnel Stage Content Preferences**: Optimal content lengths and complexity for ToFu/MoFu/BoFu
- **Best for**: In-depth competitive analysis, content strategy optimization, market intelligence
- **Output**: `competitor-YYMMDD.md`

---

## 🎯 Advanced Content Analysis Features ✨ *NEW*

### **Comprehensive Content Metrics**
Every processed post automatically includes detailed content analysis:

#### **Content Quality Metrics**
- **Word Count**: Exact word count for benchmarking and strategy analysis
- **Reading Time**: Estimated reading time (225 words/minute average)
- **Sentence Complexity**: Average words per sentence for readability assessment
- **Content Structure**: Headings, paragraphs, lists, and media element detection

#### **Strategic Content Classification**
- **Content Angle**: How-to Guide, Case Study, Thought Leadership, Product Overview, Industry Analysis, Best Practices
- **Content Depth**: Surface (basic overview), Intermediate (detailed), Deep (comprehensive analysis)
- **Target Persona**: Technical (developers/architects), Marketing (marketers/creators), Business (executives), Mixed
- **Content Freshness**: 1-10 score for industry relevance and timeliness
- **Competitive Differentiation**: Unique angles and insights that differentiate from typical competitor content

#### **Content Structure Analysis**
- **Media Richness**: Image count, code blocks, links for engagement assessment
- **Organization Quality**: Heading structure, list usage, content scanability
- **Accessibility Indicators**: Sentence length, complexity scoring for audience reach

### **Quality Benchmarking**
The system automatically compares content across competitors:

- **Length Strategies**: Micro (≤300), Short (301-800), Medium (801-1500), Long (1501-3000), Deep Dive (3000+)
- **Complexity Levels**: Simple, Moderate, Complex based on sentence structure
- **Structure Scoring**: Low, Moderate, High based on headings and organizational elements
- **Reading Time Distribution**: Quick (≤3min), Medium (4-8min), Long (8+ min) reads

---

## 🔍 Understanding Content Gap Analysis

### **Gap Opportunity Scoring**
The system identifies topics covered by **20-80% of competitors** as prime opportunities:

- **< 20% coverage**: Too niche, limited value
- **20-80% coverage**: 🎯 **Sweet spot** - proven valuable but not saturated  
- **> 80% coverage**: Oversaturated, hard to differentiate

### **Opportunity Ranking**
Topics are ranked by proximity to **50% coverage** - the maximum differentiation potential.

### **Funnel Stage Gap Detection**
- **Gap** (⚠️): < 20% of competitor's content in stage
- **Moderate** (➡️): 20-40% coverage  
- **Good** (✅): > 40% coverage

### **Enhanced Content Format Analysis** ✨ *UPDATED*
Now based on comprehensive word count analysis rather than character estimates:
- **Deep Dive Content**: 3000+ words (comprehensive guides, whitepapers)
- **Long-form Content**: 1501-3000 words (detailed articles, tutorials)  
- **Medium Content**: 801-1500 words (standard blog posts, explanations)
- **Short Content**: 301-800 words (quick tips, announcements)
- **Micro Content**: ≤300 words (brief updates, social posts)

### **Strategic Content Gap Types** ✨ *NEW*
The enhanced system identifies multiple gap categories:

#### **Content Angle Gaps**
- Missing content types (How-to vs Case Study vs Thought Leadership)
- Strategic opportunities where 20-80% of competitors have coverage
- Identification of underserved content approaches

#### **Content Depth Gaps** 
- Surface vs Intermediate vs Deep content analysis
- Opportunities for more comprehensive or accessible content
- Competitive positioning through content complexity

#### **Quality & Structure Gaps**
- Reading time distribution imbalances
- Content structure optimization opportunities  
- Competitive differentiation through content quality

---

## 🚨 Troubleshooting

### **Common Issues**

#### **"No processed data found"**
```bash
# Solution: Run enrichment first
python main.py get-posts --competitor "competitor-name"
# OR
python main.py enrich --raw --competitor "competitor-name"
```

#### **"No pending jobs found"**
This is normal - indicates no batch jobs are currently running.

#### **Batch jobs not completing**
```bash
# Check job status
python main.py check-job --competitor "competitor-name"

# Force wait for completion
python main.py enrich --competitor "competitor-name" --wait
```

#### **Export files not found**
Files are saved to `exports/` directory with timestamped names:
```bash
ls exports/  # Check generated files
```

### **Best Practices**

#### **General Usage**
1. **Always activate virtual environment**: `source .venv/bin/activate`
2. **Start with small datasets**: Use `--competitor` flag for testing
3. **Use `--wait` for batch jobs**: Ensures completion before proceeding
4. **Check job status regularly**: `python main.py check-job`
5. **Use analyze for quick insights**: `python main.py analyze --gaps`
6. **Export for persistence**: `python main.py export --format strategy-brief`

#### **Content Analysis Optimization** ✨ *NEW*
7. **Leverage enhanced content intelligence**: Always export with `--format md` for comprehensive analysis
8. **Compare content quality metrics**: Use content-gaps format to benchmark word counts, complexity, and freshness
9. **Monitor content length strategies**: Track competitor preferences for Micro/Short/Medium/Long/Deep Dive content
10. **Analyze content structure patterns**: Use structure scoring to identify engagement optimization opportunities
11. **Track content freshness**: Monitor competitor content relevance scores to identify outdated content opportunities
12. **Benchmark reading time**: Use reading time analysis to optimize content accessibility for your audience

---

## 📈 Advanced Usage

### **Automated Workflows**
```bash
#!/bin/bash
# Daily competitive intelligence script

# Update all competitors
python main.py get-posts --days 1

# Generate strategic brief
python main.py export --format strategy-brief

# Quick gap analysis
python main.py analyze --gaps > daily-analysis.txt
```

### **Integration with Content Strategy** ✨ *ENHANCED*
1. **Content Calendar Planning**: Use enhanced gap analysis for topic identification with content angle and depth insights
2. **Content Quality Benchmarking**: Compare word counts, reading times, complexity scores, and content structure across competitors
3. **Strategic Content Positioning**: Use detailed competitor profiling with content types, freshness ratings, and quality metrics
4. **Performance Optimization**: Benchmark content length strategies, reading time distribution, and structural approaches
5. **Audience Targeting**: Leverage persona indicators and complexity analysis for content accessibility optimization
6. **Content Differentiation**: Use competitive differentiation analysis to identify unique positioning opportunities
7. **Agent Integration**: Enhanced export formats with comprehensive content intelligence for LLM consumption

### **Data Pipeline Optimization**
- **Incremental Updates**: Use `--days` for recent activity only
- **Batch Processing**: System automatically optimizes for dataset size
- **Resource Management**: Use `--competitor` to limit scope
- **State Recovery**: Pipeline can resume after failures

---

## 🔗 Additional Resources

- **Architecture Details**: [ETL.md](ETL.md)
- **Technical Documentation**: [README.md](README.md)  
- **Configuration**: `config/` directory
- **Export Files**: `exports/` directory
- **Logs**: Check console output for detailed information

---

*Need help? The system includes comprehensive error messages and recovery guidance for most scenarios.*