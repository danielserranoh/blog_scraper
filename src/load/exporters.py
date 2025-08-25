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
    """Formats a list of posts into a Markdown string."""
    output = []
    for post in posts:
        if 'competitor' in post:
            output.append(f"### Competitor: {post['competitor']}")
        output.append(f"## {post.get('title', 'N/A')}")
        output.append(f"**Date**: {post.get('publication_date', 'N/A')}  ")
        output.append(f"**URL**: <{post.get('url', 'N/A')}>  ")
        output.append(f"**Funnel Stage**: {post.get('funnel_stage', 'N/A')}")
        output.append("\n>**Summary**")
        output.append(f"> {post.get('summary', 'N/A')}")
        output.append("\n**Keywords**: " + post.get('seo_keywords', 'N/A'))
        output.append("**Meta Keywords**: " + post.get('seo_meta_keywords', 'N/A') + "  ")

        # --- UPDATED: Simplified headings logic. The ExportManager now handles parsing. ---
        headings_list = post.get('headings', [])
        if headings_list:
            output.append("\n**Headings**")
            for heading in headings_list:
                try:
                    #md_tag = '#' * int(heading['tag'].replace("h",""))
                    md_tag = '#' * int(heading.get('tag').replace("h",""))
                    output.append(f"{md_tag} {heading['text']}")
                except (IndexError, ValueError):
                    output.append(f"- {heading.get('text', 'N/A')}")

        
        # --- NEW: Add schemas to Markdown output ---
        schemas_list = post.get('schemas', [])
        if schemas_list:
            output.append("\n**JSON-LD Schemas**")
            output.append("```json")
            output.append(json.dumps(schemas_list, indent=2))
            output.append("```")

        output.append("\n---")
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

def export_data(posts, export_format, config):
    """
    Router function to format data based on the specified format.
    """
    if export_format == 'txt':
        return _format_as_txt(posts)
    elif export_format == 'md':
        return _format_as_md(posts)
    elif export_format == 'json':
        return _format_as_json(posts)
    elif export_format == 'csv':
        return _format_as_csv(posts)
    elif export_format == 'gsheets':
        return _export_to_gsheets(posts, config)
    
    else:
        raise ValueError(f"Unknown export format: {export_format}")