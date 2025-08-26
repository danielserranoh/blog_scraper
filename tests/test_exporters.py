# tests/test_exporters.py
# This file contains unit tests for the data export functions.

import pytest
import json
from types import SimpleNamespace
from src.load import exporters

def test_format_as_md_with_headings():
    """
    Tests that the markdown formatter correctly handles headings
    when the tag format is correct.
    """
    posts = [
        {
            'title': 'Test Post',
            'url': 'https://example.com/test-post',
            'publication_date': '2025-08-26',
            'summary': 'A test summary.',
            'seo_keywords': 'keyword1, keyword2',
            'seo_meta_keywords': 'meta1, meta2',
            'funnel_stage': 'ToFu',
            'headings': [
                {'tag': 'h1', 'text': 'Main Heading'},
                {'tag': 'h2', 'text': 'Sub Heading'},
                {'tag': 'h3', 'text': 'Sub-sub Heading'}
            ],
            'schemas': []
        }
    ]
    
    expected_output_part = "# Main Heading\n## Sub Heading\n### Sub-sub Heading"
    
    formatted_data = exporters._format_as_md(posts)
    assert expected_output_part in formatted_data

def test_format_as_md_with_invalid_headings(caplog):
    """
    Tests that the markdown formatter gracefully handles
    invalid headings without crashing.
    """
    posts = [
        {
            'title': 'Bad Post',
            'url': 'https://example.com/bad-post',
            'publication_date': '2025-08-26',
            'summary': 'A bad test summary.',
            'seo_keywords': 'keyword1, keyword2',
            'seo_meta_keywords': 'meta1, meta2',
            'funnel_stage': 'ToFu',
            'headings': [
                {'tag': 'h1', 'text': 'Main Heading'},
                {'tag': 'h', 'text': 'Invalid Heading'}, # <-- This is the invalid tag
                {'tag': 'h2', 'text': 'Sub Heading'}
            ],
            'schemas': []
        }
    ]
    
    expected_output_part = "# Main Heading\n- Invalid Heading\n## Sub Heading"
    
    formatted_data = exporters._format_as_md(posts)
    assert expected_output_part in formatted_data