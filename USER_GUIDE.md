# 📖 Blog Scraper User Guide

Welcome to the Blog Scraper ETL Pipeline! This guide will help you get started with all the features and commands available for competitive content intelligence.

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

### **Strategic Intelligence Formats** ✨

#### **Strategy Brief (`strategy-brief`)**
- **Purpose**: Executive-level competitive intelligence
- **Features**: 
  - Market share analysis
  - Publishing velocity benchmarks
  - Content focus identification
  - Strategic opportunities with recommendations
- **Best for**: Leadership briefings, strategic planning
- **Output**: `competitor-strategy-brief-YYMMDD.md`

#### **Content Gaps (`content-gaps`)**
- **Purpose**: Data-driven opportunity identification
- **Features**:
  - Topic coverage gaps (20-80% market coverage)
  - Funnel stage distribution analysis
  - Content format opportunities
  - Competitive positioning insights
- **Best for**: Content strategy planning, editorial calendars
- **Output**: `competitor-content-gaps-YYMMDD.md`

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

### **Content Format Analysis**
Based on content length as format proxy:
- **Long-form Guide**: > 3000 characters
- **Medium Article**: 1500-3000 characters  
- **Short Post**: 500-1500 characters
- **Brief Update**: < 500 characters

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

1. **Always activate virtual environment**: `source .venv/bin/activate`
2. **Start with small datasets**: Use `--competitor` flag for testing
3. **Use `--wait` for batch jobs**: Ensures completion before proceeding
4. **Check job status regularly**: `python main.py check-job`
5. **Use analyze for quick insights**: `python main.py analyze --gaps`
6. **Export for persistence**: `python main.py export --format strategy-brief`

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

### **Integration with Content Strategy**
1. **Content Calendar Planning**: Use gap analysis for topic identification
2. **Competitive Positioning**: Use strategy briefs for market analysis  
3. **Performance Benchmarking**: Compare publishing velocity and formats
4. **Agent Integration**: Export formats designed for LLM consumption

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