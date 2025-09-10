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
        
        # Funnel stage focus
        competitor_posts = [p for p in posts if p.get('competitor') == competitor]
        comp_funnel = Counter(p.get('funnel_stage', 'N/A') for p in competitor_posts)
        top_stage = comp_funnel.most_common(1)[0] if comp_funnel else ('N/A', 0)
        output.append(f"- **Content Focus**: {top_stage[0]} ({(top_stage[1]/count*100):.0f}% of their content)")
        
        # Content themes (from headings)
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
    
    # Content length analysis
    content_lengths = [len(post.get('content', '')) for post in posts if post.get('content')]
    if content_lengths:
        avg_length = sum(content_lengths) / len(content_lengths)
        output.append(f"### Content Depth Analysis")
        output.append(f"**Average Content Length**: {avg_length:,.0f} characters")
        long_form_count = len([l for l in content_lengths if l > 3000])
        output.append(f"**Long-form Content**: {long_form_count} posts ({(long_form_count/len(posts)*100):.1f}%)")
        output.append("**Recommendation**: Balance long-form thought leadership with accessible shorter content.\n")
    
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
    
    # Funnel stage gaps by competitor
    output.append("## 🎯 Funnel Stage Gaps by Competitor")
    
    funnel_stages = ['ToFu', 'MoFu', 'BoFu']
    stage_coverage = defaultdict(lambda: defaultdict(int))
    
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
    
    # Content format gaps
    output.append("## 📊 Content Format Analysis")
    
    format_analysis = defaultdict(lambda: defaultdict(int))
    
    for post in posts:
        competitor = post.get('competitor', 'Unknown')
        content_length = len(post.get('content', ''))
        
        # Categorize by content length as a proxy for format
        if content_length > 3000:
            format_type = "Long-form Guide"
        elif content_length > 1500:
            format_type = "Medium Article"
        elif content_length > 500:
            format_type = "Short Post"
        else:
            format_type = "Brief Update"
        
        format_analysis[competitor][format_type] += 1
    
    all_formats = set()
    for competitor_formats in format_analysis.values():
        all_formats.update(competitor_formats.keys())
    
    for format_type in sorted(all_formats):
        output.append(f"### {format_type}")
        for competitor in sorted(format_analysis.keys()):
            count = format_analysis[competitor].get(format_type, 0)
            total = sum(format_analysis[competitor].values())
            percentage = (count / total * 100) if total > 0 else 0
            
            status = "📈 Strong" if percentage > 30 else "📉 Weak" if percentage < 10 else "➡️ Moderate"
            output.append(f"  - **{competitor}**: {count} posts ({percentage:.0f}%) {status}")
        output.append("")
    
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