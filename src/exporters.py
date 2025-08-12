# src/exporters.py
# This module contains functions to format post data into different output types.

import json

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
        output.append("-" * 40)
    return "\n".join(output)

def _format_as_json(posts):
    """Formats a list of posts into a JSON string."""
    # The CSV reader will read all values as strings, so we convert where appropriate
    for post in posts:
        # Example of converting a field if needed, though for JSON it's often fine
        pass
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
        output.append("\n> ### Summary")
        output.append(f"> {post.get('summary', 'N/A')}")
        output.append("\n**Keywords**: " + post.get('seo_keywords', 'N/A'))
        output.append("\n---")
    return "\n".join(output)

def export_data(posts, export_format):
    """
    Router function to format data based on the specified format.
    """
    if export_format == 'txt':
        return _format_as_txt(posts)
    elif export_format == 'json':
        return _format_as_json(posts)
    elif export_format == 'md':
        return _format_as_md(posts)
    else:
        raise ValueError(f"Unknown export format: {export_format}")