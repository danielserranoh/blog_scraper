# src/load/exporters.py
# This module contains functions to format post data into different output types.

import json
import csv
import io
import os.path
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# --- Helper Functions ---


def _format_as_txt(posts):
    """Formats a list of posts into a plain text string."""
    output = []
    for post in posts:
        # Add the competitor name to the output for clarity in combined files
        if 'competitor' in post:
            output.append(f"Competitor: {post['competitor']}")
        output.append(f"Title: {post.get('title', 'N/A')}")
        output.append(f"Publication Date: {post.get('publication_date', 'N/A')}")
        output.append(f"URL: {post.get('url', 'N/A')}")
        output.append(f"Summary: {post.get('summary', 'N/A')}")
        output.append(f"SEO Keywords (LLM): {post.get('seo_keywords', 'N/A')}")
        output.append(f"Meta Keywords: {post.get('seo_meta_keywords', 'N/A') + ' '}")

        # --- UPDATED: Format headings for text output ---
        headings_list = post.get('headings', [])
        if headings_list:
            output.append("Headings:")
            for heading in headings_list:
                output.append(f"  - {heading['text']} ({heading['tag']})")
        else:
            output.append("Headings: N/A")
            
        # --- NEW: Add funnel stage to text output ---
        output.append(f"Funnel Stage: {post.get('funnel_stage', 'N/A')}")
        output.append(f"Target Audience: {post.get('target_audience', 'N/A')}")
        
        # --- NEW: Add strategic analysis to text output ---
        strategic_analysis = post.get('strategic_analysis', {})
        if strategic_analysis:
            output.append("Strategic Analysis:")
            output.append(f"  Content Angle: {strategic_analysis.get('content_angle', 'N/A')}")
            output.append(f"  Content Depth: {strategic_analysis.get('content_depth', 'N/A')}")
            output.append(f"  Freshness Score: {strategic_analysis.get('content_freshness_score', 'N/A')}")
            output.append(f"  Persona Focus: {strategic_analysis.get('target_persona_indicators', 'N/A')}")
            competitive_diff = strategic_analysis.get('competitive_differentiation', 'N/A')
            if competitive_diff != 'N/A':
                output.append(f"  Competitive Differentiation: {competitive_diff}")
        
        # --- NEW: Add content metrics to text output ---
        content_processing = post.get('metadata', {}).get('content_processing', {})
        if content_processing:
            output.append("Content Metrics:")
            if 'word_count' in content_processing:
                output.append(f"  Word Count: {content_processing['word_count']}")
            if 'reading_time_minutes' in content_processing:
                output.append(f"  Reading Time: {content_processing['reading_time_minutes']} minutes")
            if 'avg_sentence_length' in content_processing:
                output.append(f"  Avg Sentence Length: {content_processing['avg_sentence_length']} words")
            if 'heading_count' in content_processing:
                output.append(f"  Structure: {content_processing['heading_count']} headings, {content_processing.get('paragraph_count', 0)} paragraphs")
            if 'list_items_count' in content_processing and content_processing['list_items_count'] > 0:
                output.append(f"  Lists: {content_processing['list_items_count']} items ({content_processing.get('bullet_points', 0)} bullets, {content_processing.get('numbered_items', 0)} numbered)")
            if 'image_count' in content_processing or 'code_block_count' in content_processing:
                media_info = []
                if content_processing.get('image_count', 0) > 0:
                    media_info.append(f"{content_processing['image_count']} images")
                if content_processing.get('code_block_count', 0) > 0:
                    media_info.append(f"{content_processing['code_block_count']} code blocks")
                if content_processing.get('link_count', 0) > 0:
                    media_info.append(f"{content_processing['link_count']} links")
                if media_info:
                    output.append(f"  Media: {', '.join(media_info)}")
        
        # --- NEW: Add schemas to text output ---
        schemas_list = post.get('schemas', [])
        if schemas_list:
            output.append(f"Schemas Found: {len(schemas_list)}")
            for schema in schemas_list:
                output.append(f"  - @type: {schema.get('@type', 'N/A')}")

        output.append("-" * 40)
    return "\n".join(output)

def _format_as_json(posts):
    """Formats a list of posts into a JSON string."""
    return json.dumps(posts, indent=2)

def _format_as_md(posts):
    """Formats a list of posts into an enhanced Markdown string with competitive intelligence."""
    from datetime import datetime, timedelta
    from collections import Counter, defaultdict
    
    # Generate intelligence header
    output = []
    output.append("# Competitive Content Intelligence Report")
    output.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"**Total Posts**: {len(posts)}")
    
    # Competitive analysis
    competitor_stats = Counter(post.get('competitor', 'Unknown') for post in posts)
    funnel_stats = Counter(post.get('funnel_stage', 'N/A') for post in posts)
    
    output.append("\n## 📊 Competitive Intelligence Overview")
    output.append("### Posts by Competitor")
    for competitor, count in competitor_stats.most_common():
        percentage = (count / len(posts)) * 100
        output.append(f"- **{competitor}**: {count} posts ({percentage:.1f}%)")
    
    output.append("\n### Content by Funnel Stage")
    for stage, count in funnel_stats.most_common():
        percentage = (count / len(posts)) * 100
        stage_emoji = {'ToFu': '🔍', 'MoFu': '🎯', 'BoFu': '💼'}.get(stage, '📝')
        output.append(f"- {stage_emoji} **{stage}**: {count} posts ({percentage:.1f}%)")
    
    # Content length distribution analysis
    output.append("\n### 📏 Content Length Distribution")
    
    # Collect word counts and categorize
    word_counts = []
    length_categories = {'Micro (≤300)': 0, 'Short (301-800)': 0, 'Medium (801-1500)': 0, 'Long (1501-3000)': 0, 'Deep Dive (3000+)': 0}
    length_by_competitor = defaultdict(lambda: {'Micro (≤300)': 0, 'Short (301-800)': 0, 'Medium (801-1500)': 0, 'Long (1501-3000)': 0, 'Deep Dive (3000+)': 0})
    length_by_funnel = defaultdict(lambda: defaultdict(int))
    
    for post in posts:
        processing_info = post.get('metadata', {}).get('content_processing', {})
        word_count = processing_info.get('word_count', 0)
        
        if word_count > 0:
            word_counts.append(word_count)
            competitor = post.get('competitor', 'Unknown')
            funnel_stage = post.get('funnel_stage', 'N/A')
            
            # Categorize by length
            if word_count <= 300:
                category = 'Micro (≤300)'
            elif word_count <= 800:
                category = 'Short (301-800)'
            elif word_count <= 1500:
                category = 'Medium (801-1500)'
            elif word_count <= 3000:
                category = 'Long (1501-3000)'
            else:
                category = 'Deep Dive (3000+)'
            
            length_categories[category] += 1
            length_by_competitor[competitor][category] += 1
            length_by_funnel[funnel_stage][category] += 1
    
    if word_counts:
        avg_length = sum(word_counts) / len(word_counts)
        median_length = sorted(word_counts)[len(word_counts)//2]
        output.append(f"**Market Average**: {avg_length:,.0f} words | **Median**: {median_length:,} words")
        
        # Overall distribution
        total_with_metrics = len(word_counts)
        output.append("\n**Overall Market Distribution:**")
        for category, count in length_categories.items():
            percentage = (count / total_with_metrics) * 100
            bar = "█" * int(percentage / 5) + "▌" if (percentage % 5) >= 2.5 else "█" * int(percentage / 5)
            output.append(f"- {category}: {count} posts ({percentage:.1f}%) {bar}")
    
    # Content length strategy by competitor
    if length_by_competitor:
        output.append("\n**Length Strategy by Competitor:**")
        for competitor in sorted(length_by_competitor.keys()):
            competitor_total = sum(length_by_competitor[competitor].values())
            if competitor_total > 0:
                # Find dominant strategy
                dominant_category = max(length_by_competitor[competitor].items(), key=lambda x: x[1])
                dominant_pct = (dominant_category[1] / competitor_total) * 100
                
                # Create mini distribution
                mini_dist = []
                for category, count in length_categories.items():
                    comp_count = length_by_competitor[competitor][category]
                    if comp_count > 0:
                        pct = (comp_count / competitor_total) * 100
                        mini_dist.append(f"{category.split()[0]}: {comp_count} ({pct:.0f}%)")
                
                output.append(f"  - **{competitor}**: {dominant_category[0]} focused ({dominant_pct:.0f}%)")
                output.append(f"    - Distribution: {' | '.join(mini_dist[:3])}")
    
    # Length preference by funnel stage
    if length_by_funnel:
        output.append("\n**Content Length by Funnel Stage:**")
        for stage in ['ToFu', 'MoFu', 'BoFu']:
            if stage in length_by_funnel:
                stage_total = sum(length_by_funnel[stage].values())
                if stage_total > 0:
                    dominant_length = max(length_by_funnel[stage].items(), key=lambda x: x[1])
                    avg_for_stage = []
                    
                    # Calculate average length for this stage
                    stage_posts = [p for p in posts if p.get('funnel_stage') == stage]
                    stage_word_counts = []
                    for post in stage_posts:
                        wc = post.get('metadata', {}).get('content_processing', {}).get('word_count', 0)
                        if wc > 0:
                            stage_word_counts.append(wc)
                    
                    if stage_word_counts:
                        stage_avg = sum(stage_word_counts) / len(stage_word_counts)
                        output.append(f"  - **{stage}**: Avg {stage_avg:,.0f} words, prefers {dominant_length[0]} ({dominant_length[1]} posts)")
    
    # Content complexity and structural analysis
    output.append("\n### 🎯 Content Complexity & Structure Overview")
    
    # Collect complexity metrics
    complexity_by_competitor = defaultdict(list)
    reading_times = []
    structure_metrics = {'Low Structure': 0, 'Moderate Structure': 0, 'High Structure': 0}
    
    for post in posts:
        processing_info = post.get('metadata', {}).get('content_processing', {})
        competitor = post.get('competitor', 'Unknown')
        
        # Sentence complexity
        avg_sentence_length = processing_info.get('avg_sentence_length', 0)
        if avg_sentence_length > 0:
            complexity_by_competitor[competitor].append(avg_sentence_length)
        
        # Reading time
        reading_time = processing_info.get('reading_time_minutes', 0)
        if reading_time > 0:
            reading_times.append(reading_time)
        
        # Structure analysis (headings + lists as structure indicators)
        heading_count = processing_info.get('heading_count', 0)
        list_items = processing_info.get('list_items_count', 0)
        structure_score = heading_count + (list_items / 5)  # Weighted structure score
        
        if structure_score <= 2:
            structure_metrics['Low Structure'] += 1
        elif structure_score <= 6:
            structure_metrics['Moderate Structure'] += 1
        else:
            structure_metrics['High Structure'] += 1
    
    if reading_times:
        avg_reading_time = sum(reading_times) / len(reading_times)
        quick_reads = len([rt for rt in reading_times if rt <= 3])
        medium_reads = len([rt for rt in reading_times if 3 < rt <= 8])
        long_reads = len([rt for rt in reading_times if rt > 8])
        
        output.append(f"**Reading Time Distribution**:")
        total_timed = len(reading_times)
        output.append(f"- Quick reads (≤3 min): {quick_reads} posts ({(quick_reads/total_timed)*100:.1f}%)")
        output.append(f"- Medium reads (4-8 min): {medium_reads} posts ({(medium_reads/total_timed)*100:.1f}%)")  
        output.append(f"- Long reads (8+ min): {long_reads} posts ({(long_reads/total_timed)*100:.1f}%)")
        output.append(f"- **Market Average**: {avg_reading_time:.1f} minutes per post")
    
    # Content structure distribution
    total_structured = sum(structure_metrics.values())
    if total_structured > 0:
        output.append(f"\n**Content Structure Analysis**:")
        for structure_type, count in structure_metrics.items():
            percentage = (count / total_structured) * 100
            output.append(f"- {structure_type}: {count} posts ({percentage:.1f}%)")
    
    # Complexity by competitor
    if complexity_by_competitor:
        output.append(f"\n**Readability by Competitor** (avg words per sentence):")
        for competitor in sorted(complexity_by_competitor.keys()):
            if complexity_by_competitor[competitor]:
                avg_complexity = sum(complexity_by_competitor[competitor]) / len(complexity_by_competitor[competitor])
                complexity_level = "Complex" if avg_complexity > 18 else "Moderate" if avg_complexity > 12 else "Simple"
                output.append(f"  - **{competitor}**: {avg_complexity:.1f} words/sentence ({complexity_level})")
    
    # Recent content trends (last 30 days)
    recent_cutoff = datetime.now() - timedelta(days=30)
    recent_posts = []
    for post in posts:
        try:
            pub_date = datetime.strptime(post.get('publication_date', ''), '%Y-%m-%d')
            if pub_date >= recent_cutoff:
                recent_posts.append(post)
        except (ValueError, TypeError):
            continue
    
    if recent_posts:
        output.append(f"\n### 📈 Recent Activity (Last 30 Days): {len(recent_posts)} posts")
        recent_competitors = Counter(post.get('competitor', 'Unknown') for post in recent_posts)
        for competitor, count in recent_competitors.most_common(3):
            output.append(f"- **{competitor}**: {count} recent posts")
    
    output.append("\n---\n")
    
    # Group posts by competitor for better organization
    posts_by_competitor = defaultdict(list)
    for post in posts:
        posts_by_competitor[post.get('competitor', 'Unknown')].append(post)
    
    # Sort posts within each competitor by date (newest first)
    for competitor in posts_by_competitor:
        posts_by_competitor[competitor].sort(
            key=lambda p: p.get('publication_date', '1900-01-01'), 
            reverse=True
        )
    
    # Generate content sections by competitor
    for competitor, competitor_posts in posts_by_competitor.items():
        output.append(f"# 🏢 {competitor.upper()} ({len(competitor_posts)} posts)")
        output.append("")
        
        for post in competitor_posts:
            # Enhanced post header with strategic indicators
            stage_emoji = {'ToFu': '🔍', 'MoFu': '🎯', 'BoFu': '💼'}.get(post.get('funnel_stage', ''), '📝')
            output.append(f"## {stage_emoji} {post.get('title', 'N/A')}")
            
            # Metadata with strategic context
            output.append(f"**📅 Date**: {post.get('publication_date', 'N/A')}")
            output.append(f"**🔗 URL**: <{post.get('url', 'N/A')}>")
            output.append(f"**🎯 Funnel Stage**: {post.get('funnel_stage', 'N/A')}")
            
            # Content length indicator
            content_length = len(post.get('content', ''))
            if content_length > 3000:
                length_indicator = "📖 Long-form"
            elif content_length > 1000:
                length_indicator = "📄 Medium"
            else:
                length_indicator = "📝 Short"
            output.append(f"**📏 Content Length**: {length_indicator} ({content_length:,} chars)")
            
            # Enhanced summary section
            if post.get('summary') and post.get('summary') != 'N/A':
                output.append("\n### 💡 Strategic Summary")
                output.append(f"> {post.get('summary')}")
            
            # SEO Intelligence
            if post.get('seo_keywords') and post.get('seo_keywords') != 'N/A':
                output.append("\n### 🎯 SEO Keywords")
                output.append(f"{post.get('seo_keywords')}")
            
            # Content structure analysis
            headings_list = post.get('headings', [])
            if headings_list:
                output.append("\n### 📋 Content Structure")
                h2_count = len([h for h in headings_list if h.get('tag') == 'h2'])
                h3_count = len([h for h in headings_list if h.get('tag') == 'h3'])
                output.append(f"**Structure Depth**: {h2_count} main sections, {h3_count} subsections")
                
                output.append("\n**Outline:**")
                for heading in headings_list[:10]:  # Limit to top 10 headings
                    try:
                        level = int(heading['tag'][1])
                        indent = "  " * (level - 2) if level >= 2 else ""
                        output.append(f"{indent}- {heading['text']}")
                    except (IndexError, ValueError):
                        output.append(f"- {heading.get('text', 'N/A')}")
                
                if len(headings_list) > 10:
                    output.append(f"  ... and {len(headings_list) - 10} more sections")
            
            # Technical implementation details
            schemas_list = post.get('schemas', [])
            if schemas_list:
                output.append("\n### ⚙️ Technical Implementation")
                schema_types = [s.get('@type', 'Unknown') for s in schemas_list]
                output.append(f"**Schema.org Types**: {', '.join(set(schema_types))}")
            
            output.append("\n---\n")
    
    return "\n".join(output)

def _format_as_csv(posts):
    """Formats a list of posts into a CSV string."""
    if not posts:
        return ""
    
    # Use io.StringIO to build the CSV in memory
    output = io.StringIO()
    
    # A set to dynamically gather all possible fieldnames from the posts
    all_fieldnames = set()
    for post in posts:
        all_fieldnames.update(post.keys())
        
    fieldnames = list(all_fieldnames)
    
    # Sort for consistent column order
    fieldnames.sort()

    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    
    writer.writeheader()
    writer.writerows(posts)
    
    return output.getvalue()


# --- NEW: Google Sheets Exporter ---

# If modifying these scopes, delete token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _authenticate_google_sheets():
    """Handles the OAuth2 authentication flow for Google Sheets."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

def _export_to_gsheets(posts, config):
    """Exports a list of posts to a Google Sheet."""
    spreadsheet_name = config.get('google_sheets', {}).get('spreadsheet_name', 'Blog Scraper Export')
    
    try:
        creds = _authenticate_google_sheets()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Check if the spreadsheet already exists (not implemented, create new for simplicity)
        # For this version, we will just create a new spreadsheet on each export.
        # A more advanced version could find and update an existing sheet.
        
        spreadsheet = sheet.create(body={'properties': {'title': spreadsheet_name}}, fields='spreadsheetId').execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        logger.info(f"Created new spreadsheet with ID: {spreadsheet_id}")

        # Prepare the data for the sheet (header + rows)
        header = list(posts[0].keys()) if posts else []
        values = [header] + [list(post.values()) for post in posts]

        body = {"values": values}
        result = sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range="A1",
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()
        
        logger.info(f"{result.get('updatedCells')} cells updated in Google Sheet.")
        return f"Successfully exported data to Google Sheet: '{spreadsheet_name}'"

    except HttpError as err:
        logger.error(f"An error occurred with the Google Sheets API: {err}")
        return f"Failed to export to Google Sheets: {err}"
    except FileNotFoundError:
        logger.error("credentials.json not found. Please follow the authentication setup steps.")
        return "Failed to export: credentials.json not found."

def _format_as_strategy_brief(posts):
    """Formats posts into a strategic content intelligence brief."""
    from datetime import datetime, timedelta
    from collections import Counter, defaultdict
    
    output = []
    output.append("# 🎯 Content Strategy Intelligence Brief")
    output.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append("\n---\n")
    
    # Executive Summary
    competitor_count = len(set(post.get('competitor', 'Unknown') for post in posts))
    total_posts = len(posts)
    
    output.append("## 📋 Executive Summary")
    output.append(f"- **Competitors Analyzed**: {competitor_count}")
    output.append(f"- **Total Content Pieces**: {total_posts}")
    
    # Recent activity analysis
    recent_cutoff = datetime.now() - timedelta(days=30)
    recent_posts = []
    for post in posts:
        try:
            pub_date = datetime.strptime(post.get('publication_date', ''), '%Y-%m-%d')
            if pub_date >= recent_cutoff:
                recent_posts.append(post)
        except (ValueError, TypeError):
            continue
    
    output.append(f"- **Recent Activity (30 days)**: {len(recent_posts)} posts")
    
    # Content strategy insights
    funnel_distribution = Counter(post.get('funnel_stage', 'N/A') for post in posts)
    dominant_stage = funnel_distribution.most_common(1)[0] if funnel_distribution else ('N/A', 0)
    output.append(f"- **Dominant Content Type**: {dominant_stage[0]} ({dominant_stage[1]} posts)")
    
    # Competitive positioning analysis
    output.append("\n## 🏆 Competitive Landscape")
    competitor_stats = Counter(post.get('competitor', 'Unknown') for post in posts)
    
    for competitor, count in competitor_stats.most_common():
        percentage = (count / total_posts) * 100
        
        # Calculate recent activity
        recent_count = len([p for p in recent_posts if p.get('competitor') == competitor])
        activity_trend = "📈 Active" if recent_count >= 3 else "📉 Low" if recent_count <= 1 else "➡️ Moderate"
        
        output.append(f"### {competitor}")
        output.append(f"- **Total Content**: {count} posts ({percentage:.1f}% of market)")
        output.append(f"- **Recent Activity**: {activity_trend} ({recent_count} posts/month)")
        
        # Enhanced competitor analysis with new metrics
        competitor_posts = [p for p in posts if p.get('competitor') == competitor]
        
        # Funnel stage focus
        comp_funnel = Counter(p.get('funnel_stage', 'N/A') for p in competitor_posts)
        top_stage = comp_funnel.most_common(1)[0] if comp_funnel else ('N/A', 0)
        output.append(f"- **Content Focus**: {top_stage[0]} ({(top_stage[1]/count*100):.0f}% of their content)")
        
        # Content strategy analysis
        comp_angles = [p.get('strategic_analysis', {}).get('content_angle', '') for p in competitor_posts]
        comp_angles = [a for a in comp_angles if a and a != 'N/A']
        if comp_angles:
            angle_dist = Counter(comp_angles)
            top_angle = angle_dist.most_common(1)[0]
            output.append(f"- **Primary Content Type**: {top_angle[0]} ({(top_angle[1]/len(comp_angles)*100):.0f}% of analyzed content)")
        
        # Content quality metrics
        comp_word_counts = [p.get('metadata', {}).get('content_processing', {}).get('word_count', 0) for p in competitor_posts]
        comp_word_counts = [w for w in comp_word_counts if w > 0]
        if comp_word_counts:
            avg_words = sum(comp_word_counts) / len(comp_word_counts)
            output.append(f"- **Average Content Length**: {avg_words:,.0f} words")
        
        comp_freshness = [p.get('strategic_analysis', {}).get('content_freshness_score', '') for p in competitor_posts]
        try:
            comp_freshness_nums = [int(f) for f in comp_freshness if f and f != 'N/A']
            if comp_freshness_nums:
                avg_freshness = sum(comp_freshness_nums) / len(comp_freshness_nums)
                output.append(f"- **Content Freshness**: {avg_freshness:.1f}/10 (industry relevance)")
        except (ValueError, TypeError):
            pass
        
        # Content themes (from headings - kept for backward compatibility)
        all_headings = []
        for post in competitor_posts:
            headings = post.get('headings', [])
            for heading in headings:
                if heading.get('tag') == 'h2':  # Focus on main sections
                    all_headings.append(heading.get('text', '').lower())
        
        if all_headings:
            common_themes = Counter(all_headings).most_common(3)
            themes_text = ", ".join([theme.title() for theme, _ in common_themes])
            output.append(f"- **Common Themes**: {themes_text}")
        output.append("")
    
    # Content gaps and opportunities
    output.append("## 🎯 Strategic Opportunities")
    
    # Funnel stage gaps
    total_by_stage = {stage: count for stage, count in funnel_distribution.items()}
    min_stage = min(total_by_stage.items(), key=lambda x: x[1]) if total_by_stage else ('N/A', 0)
    
    if min_stage[0] != 'N/A':
        output.append(f"### Underserved Funnel Stage: {min_stage[0]}")
        output.append(f"Only {min_stage[1]} posts found in {min_stage[0]} stage across all competitors.")
        output.append("**Recommendation**: Consider focusing content strategy on this underserved stage.\n")
    
    # Publishing frequency insights
    if recent_posts:
        avg_posts_per_competitor = len(recent_posts) / competitor_count
        output.append(f"### Publishing Velocity")
        output.append(f"**Industry Average**: {avg_posts_per_competitor:.1f} posts per competitor per month")
        output.append("**Recommendation**: Match or exceed top performers in publishing frequency.\n")
    
    # Enhanced content analysis using new metrics
    output.append(f"### 📊 Content Quality & Structure Analysis")
    
    # Word count and reading time analysis
    word_counts = []
    reading_times = []
    avg_sentence_lengths = []
    content_angles = []
    content_depths = []
    freshness_scores = []
    
    for post in posts:
        processing_info = post.get('metadata', {}).get('content_processing', {})
        strategic_info = post.get('strategic_analysis', {})
        
        if 'word_count' in processing_info:
            word_counts.append(processing_info['word_count'])
        if 'reading_time_minutes' in processing_info:
            reading_times.append(processing_info['reading_time_minutes'])
        if 'avg_sentence_length' in processing_info:
            avg_sentence_lengths.append(processing_info['avg_sentence_length'])
        if 'content_angle' in strategic_info:
            content_angles.append(strategic_info['content_angle'])
        if 'content_depth' in strategic_info:
            content_depths.append(strategic_info['content_depth'])
        if 'content_freshness_score' in strategic_info:
            try:
                freshness_scores.append(int(strategic_info['content_freshness_score']))
            except (ValueError, TypeError):
                pass
    
    if word_counts:
        avg_words = sum(word_counts) / len(word_counts)
        avg_reading_time = sum(reading_times) / len(reading_times) if reading_times else 0
        output.append(f"**Average Word Count**: {avg_words:,.0f} words")
        output.append(f"**Average Reading Time**: {avg_reading_time:.1f} minutes")
        
        # Content length distribution
        short_content = len([w for w in word_counts if w < 500])
        medium_content = len([w for w in word_counts if 500 <= w <= 1500])
        long_content = len([w for w in word_counts if w > 1500])
        
        output.append(f"**Content Length Distribution**:")
        output.append(f"  - Short (<500 words): {short_content} posts ({(short_content/len(word_counts)*100):.1f}%)")
        output.append(f"  - Medium (500-1500 words): {medium_content} posts ({(medium_content/len(word_counts)*100):.1f}%)")
        output.append(f"  - Long (>1500 words): {long_content} posts ({(long_content/len(word_counts)*100):.1f}%)")
    
    if avg_sentence_lengths:
        avg_sentence = sum(avg_sentence_lengths) / len(avg_sentence_lengths)
        readability = "Complex" if avg_sentence > 20 else "Moderate" if avg_sentence > 15 else "Simple"
        output.append(f"**Content Complexity**: {avg_sentence:.1f} words/sentence ({readability})")
    
    # Strategic content analysis
    if content_angles:
        angle_distribution = Counter(content_angles)
        output.append(f"**Content Types**: {', '.join([f'{angle} ({count})' for angle, count in angle_distribution.most_common(3)])}")
    
    if content_depths:
        depth_distribution = Counter(content_depths)
        output.append(f"**Content Depth**: {', '.join([f'{depth} ({count})' for depth, count in depth_distribution.most_common()])}")
    
    if freshness_scores:
        avg_freshness = sum(freshness_scores) / len(freshness_scores)
        output.append(f"**Average Content Freshness**: {avg_freshness:.1f}/10")
        
    output.append("**Recommendation**: Analyze top-performing content lengths and complexity levels for your target audience.\n")
    
    output.append("---\n")
    output.append("*This intelligence brief was auto-generated from competitor content analysis.*")
    
    return "\n".join(output)

def _format_as_content_gaps(posts):
    """Identifies and formats content gaps for strategic planning."""
    from collections import Counter, defaultdict
    
    output = []
    output.append("# 🔍 Content Gap Analysis")
    output.append("\n## Methodology")
    output.append("This analysis identifies underserved topics and content opportunities based on competitor content patterns.\n")
    
    # Topic extraction from headings
    topics_by_competitor = defaultdict(set)
    all_topics = set()
    
    for post in posts:
        competitor = post.get('competitor', 'Unknown')
        headings = post.get('headings', [])
        
        for heading in headings:
            if heading.get('tag') in ['h2', 'h3']:  # Focus on major sections
                topic = heading.get('text', '').lower().strip()
                if len(topic) > 5:  # Filter out very short headings
                    topics_by_competitor[competitor].add(topic)
                    all_topics.add(topic)
    
    # Find topics covered by some but not all competitors
    competitor_list = list(topics_by_competitor.keys())
    gap_opportunities = []
    
    for topic in all_topics:
        competitors_covering = [comp for comp in competitor_list if topic in topics_by_competitor[comp]]
        coverage_ratio = len(competitors_covering) / len(competitor_list)
        
        # Topics covered by 20-80% of competitors represent opportunities
        if 0.2 <= coverage_ratio <= 0.8:
            gap_opportunities.append({
                'topic': topic,
                'coverage_ratio': coverage_ratio,
                'competitors_covering': competitors_covering,
                'competitors_missing': [c for c in competitor_list if c not in competitors_covering]
            })
    
    # Sort by opportunity (topics with moderate coverage are best opportunities)
    gap_opportunities.sort(key=lambda x: abs(x['coverage_ratio'] - 0.5))
    
    output.append("## 🎯 High-Opportunity Content Gaps")
    output.append("*Topics covered by some competitors but not others - prime opportunities for differentiation*\n")
    
    for i, gap in enumerate(gap_opportunities[:10], 1):  # Top 10 opportunities
        coverage_pct = gap['coverage_ratio'] * 100
        output.append(f"### {i}. {gap['topic'].title()}")
        output.append(f"**Market Coverage**: {coverage_pct:.0f}% ({len(gap['competitors_covering'])}/{len(competitor_list)} competitors)")
        output.append(f"**Covered by**: {', '.join(gap['competitors_covering'])}")
        output.append(f"**Gap opportunity for**: {', '.join(gap['competitors_missing'])}\n")
    
    # Enhanced strategic content gaps analysis
    output.append("## 🎯 Strategic Content Gaps Analysis")
    
    # Content angle gaps
    content_angles_by_competitor = defaultdict(lambda: defaultdict(int))
    content_depths_by_competitor = defaultdict(lambda: defaultdict(int))
    funnel_coverage = defaultdict(lambda: defaultdict(int))
    
    for post in posts:
        competitor = post.get('competitor', 'Unknown')
        strategic_info = post.get('strategic_analysis', {})
        
        # Analyze content angles
        content_angle = strategic_info.get('content_angle', '')
        if content_angle and content_angle != 'N/A':
            content_angles_by_competitor[competitor][content_angle] += 1
        
        # Analyze content depths
        content_depth = strategic_info.get('content_depth', '')
        if content_depth and content_depth != 'N/A':
            content_depths_by_competitor[competitor][content_depth] += 1
        
        # Funnel stage coverage
        funnel_stage = post.get('funnel_stage', '')
        if funnel_stage and funnel_stage != 'N/A':
            funnel_coverage[competitor][funnel_stage] += 1
    
    # Content angle gap analysis
    if content_angles_by_competitor:
        output.append("### Content Type Gaps")
        all_angles = set()
        for competitor_angles in content_angles_by_competitor.values():
            all_angles.update(competitor_angles.keys())
        
        competitor_list = list(content_angles_by_competitor.keys())
        for angle in sorted(all_angles):
            competitors_with_angle = [comp for comp in competitor_list if angle in content_angles_by_competitor[comp]]
            coverage_ratio = len(competitors_with_angle) / len(competitor_list)
            
            if 0.2 <= coverage_ratio <= 0.8:  # Gap opportunity
                missing_competitors = [comp for comp in competitor_list if comp not in competitors_with_angle]
                output.append(f"**{angle}**: {len(competitors_with_angle)}/{len(competitor_list)} competitors ({coverage_ratio*100:.0f}%)")
                output.append(f"  - Gap opportunity for: {', '.join(missing_competitors)}")
        output.append("")
    
    # Content depth gap analysis  
    if content_depths_by_competitor:
        output.append("### Content Depth Gaps")
        all_depths = set()
        for competitor_depths in content_depths_by_competitor.values():
            all_depths.update(competitor_depths.keys())
        
        for depth in ['Surface', 'Intermediate', 'Deep']:  # Ordered by complexity
            if depth in all_depths:
                competitors_with_depth = [comp for comp in competitor_list if depth in content_depths_by_competitor[comp]]
                coverage_ratio = len(competitors_with_depth) / len(competitor_list)
                
                if coverage_ratio < 0.8:  # Under-served depth level
                    missing_competitors = [comp for comp in competitor_list if comp not in competitors_with_depth]
                    output.append(f"**{depth} Content**: {len(competitors_with_depth)}/{len(competitor_list)} competitors ({coverage_ratio*100:.0f}%)")
                    if missing_competitors:
                        output.append(f"  - Opportunity for: {', '.join(missing_competitors)}")
        output.append("")
    
    # Funnel stage gaps by competitor
    output.append("### Funnel Stage Coverage Analysis")
    
    funnel_stages = ['ToFu', 'MoFu', 'BoFu']
    
    for post in posts:
        competitor = post.get('competitor', 'Unknown')
        stage = post.get('funnel_stage', 'N/A')
        stage_coverage[competitor][stage] += 1
    
    for competitor in stage_coverage:
        output.append(f"### {competitor}")
        competitor_total = sum(stage_coverage[competitor].values())
        
        for stage in funnel_stages:
            count = stage_coverage[competitor].get(stage, 0)
            percentage = (count / competitor_total * 100) if competitor_total > 0 else 0
            
            # Identify gaps (less than 20% in any stage)
            status = "⚠️ Gap" if percentage < 20 else "✅ Good" if percentage > 40 else "➡️ Moderate"
            output.append(f"  - **{stage}**: {count} posts ({percentage:.0f}%) {status}")
        output.append("")
    
    # Enhanced content quality benchmarking
    output.append("## 📊 Content Quality Benchmarking")
    
    # Word count benchmarking
    competitor_metrics = defaultdict(lambda: {'word_counts': [], 'freshness_scores': [], 'complexity_scores': []})
    
    for post in posts:
        competitor = post.get('competitor', 'Unknown')
        processing_info = post.get('metadata', {}).get('content_processing', {})
        strategic_info = post.get('strategic_analysis', {})
        
        # Collect word counts
        if 'word_count' in processing_info:
            competitor_metrics[competitor]['word_counts'].append(processing_info['word_count'])
        
        # Collect freshness scores
        if 'content_freshness_score' in strategic_info:
            try:
                score = int(strategic_info['content_freshness_score'])
                competitor_metrics[competitor]['freshness_scores'].append(score)
            except (ValueError, TypeError):
                pass
        
        # Collect complexity scores (sentence length)
        if 'avg_sentence_length' in processing_info:
            competitor_metrics[competitor]['complexity_scores'].append(processing_info['avg_sentence_length'])
    
    output.append("### Content Length Benchmarking")
    output.append("| Competitor | Avg Words | Content Range | Length Strategy |")
    output.append("|------------|-----------|---------------|-----------------|")
    
    for competitor in sorted(competitor_metrics.keys()):
        word_counts = competitor_metrics[competitor]['word_counts']
        if word_counts:
            avg_words = sum(word_counts) / len(word_counts)
            min_words = min(word_counts)
            max_words = max(word_counts)
            
            # Determine strategy
            if avg_words > 1500:
                strategy = "Long-form focused"
            elif avg_words > 800:
                strategy = "Balanced approach"
            else:
                strategy = "Concise content"
            
            output.append(f"| {competitor} | {avg_words:,.0f} | {min_words:,}-{max_words:,} | {strategy} |")
    
    output.append("")
    
    # Content freshness benchmarking
    output.append("### Content Freshness Benchmarking")
    for competitor in sorted(competitor_metrics.keys()):
        freshness_scores = competitor_metrics[competitor]['freshness_scores']
        if freshness_scores:
            avg_freshness = sum(freshness_scores) / len(freshness_scores)
            freshness_level = "High" if avg_freshness >= 7 else "Moderate" if avg_freshness >= 5 else "Low"
            output.append(f"**{competitor}**: {avg_freshness:.1f}/10 ({freshness_level} industry relevance)")
    
    output.append("")
    
    # Content complexity benchmarking
    complexity_data = []
    for competitor in sorted(competitor_metrics.keys()):
        complexity_scores = competitor_metrics[competitor]['complexity_scores']
        if complexity_scores:
            avg_complexity = sum(complexity_scores) / len(complexity_scores)
            complexity_level = "Complex" if avg_complexity > 18 else "Moderate" if avg_complexity > 12 else "Simple"
            complexity_data.append((competitor, avg_complexity, complexity_level))
    
    if complexity_data:
        output.append("### Content Complexity Analysis")
        for competitor, score, level in complexity_data:
            output.append(f"**{competitor}**: {score:.1f} words/sentence ({level} readability)")
    
    output.append("")
    output.append("---")
    output.append("*This content gap analysis leverages strategic content intelligence and structural analysis to identify specific opportunities for competitive differentiation.*")
    
    return "\n".join(output)

def export_data(posts, export_format, config):
    """
    Router function to format data based on the specified format.
    """
    if export_format == 'txt':
        return _format_as_txt(posts)
    elif export_format == 'md':
        return _format_as_md(posts)
    elif export_format == 'strategy-brief':
        return _format_as_strategy_brief(posts)
    elif export_format == 'content-gaps':
        return _format_as_content_gaps(posts)
    elif export_format == 'json':
        return _format_as_json(posts)
    elif export_format == 'csv':
        return _format_as_csv(posts)
    elif export_format == 'gsheets':
        return _export_to_gsheets(posts, config)
    
    else:
        raise ValueError(f"Unknown export format: {export_format}")