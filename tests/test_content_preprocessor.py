# tests/test_content_preprocessor.py
# This file contains unit tests for the content preprocessing logic.

import pytest
from src.content_preprocessor import ContentPreprocessor

class TestContentPreprocessor:
    """Test suite for ContentPreprocessor functionality."""

    def test_clean_content_removes_smart_quotes(self):
        """Test that smart quotes are properly converted to regular quotes."""
        content = "This is 'quoted text' with "smart quotes" and—em dashes."
        
        cleaned = ContentPreprocessor._clean_content(content)
        
        assert "'" not in cleaned
        assert "'" not in cleaned
        assert """ not in cleaned
        assert """ not in cleaned
        assert "—" not in cleaned
        assert cleaned == "This is 'quoted text' with \"smart quotes\" and - em dashes."

    def test_clean_content_handles_unicode_characters(self):
        """Test that various Unicode characters are properly handled."""
        content = "Text with…ellipsis and\u00a0non-breaking\u200bspaces."
        
        cleaned = ContentPreprocessor._clean_content(content)
        
        assert "…" not in cleaned
        assert "\u00a0" not in cleaned
        assert "\u200b" not in cleaned
        assert "..." in cleaned

    def test_clean_content_removes_excessive_whitespace(self):
        """Test that excessive whitespace is normalized."""
        content = "Text    with     excessive\n\n\n\nwhitespace."
        
        cleaned = ContentPreprocessor._clean_content(content)
        
        assert "    " not in cleaned
        assert "\n\n" not in cleaned
        assert cleaned == "Text with excessive whitespace."

    def test_prepare_posts_short_content(self, sample_posts):
        """Test preprocessing posts with content that doesn't need chunking."""
        processed = ContentPreprocessor.prepare_posts_for_enrichment(sample_posts)
        
        assert len(processed) == len(sample_posts)
        assert not any(post.get('content_processing', {}).get('chunked', False) for post in processed)
        
        # Verify content processing metadata
        for post in processed:
            processing_info = post['content_processing']
            assert 'original_length' in processing_info
            assert 'processed_length' in processing_info
            assert processing_info['chunked'] is False

    def test_prepare_posts_long_content_chunking(self, sample_long_content_post):
        """Test preprocessing posts with content that needs chunking."""
        posts = [sample_long_content_post]
        
        processed = ContentPreprocessor.prepare_posts_for_enrichment(posts)
        
        # Should create multiple chunks
        assert len(processed) > 1
        
        # All chunks should be marked as chunked
        for post in processed:
            processing_info = post['content_processing']
            assert processing_info['chunked'] is True
            assert 'chunk_number' in processing_info
            assert 'total_chunks' in processing_info

        # Check chunk titles
        assert processed[0]['title'].endswith("(Part 1/2)")
        assert processed[1]['title'].endswith("(Part 2/2)")
        
        # Verify original title is preserved
        for post in processed:
            assert post['original_title'] == sample_long_content_post['title']

    def test_create_content_chunks_sentence_boundaries(self):
        """Test that chunks are created at sentence boundaries when possible."""
        # Create content with clear sentence boundaries
        content = "First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence."
        
        chunks = ContentPreprocessor._create_content_chunks(content, "Test")
        
        # All chunks should end with sentence endings (except continuation markers)
        for chunk in chunks[:-1]:  # All but last chunk
            # Remove continuation markers for testing
            clean_chunk = chunk.replace(" [Continued in next section]", "")
            assert clean_chunk.endswith(('.', '!', '?'))

    def test_create_content_chunks_with_continuation_markers(self):
        """Test that continuation markers are properly added."""
        content = "A" * 10000  # Long content that will definitely be chunked
        
        chunks = ContentPreprocessor._create_content_chunks(content, "Test")
        
        # Should have multiple chunks
        assert len(chunks) > 1
        
        # First chunk should not have "Continued from" marker
        assert not chunks[0].startswith("[Continued from previous section]")
        
        # Middle chunks should have both markers
        if len(chunks) > 2:
            middle_chunk = chunks[1]
            assert middle_chunk.startswith("[Continued from previous section]")
            assert middle_chunk.endswith("[Continued in next section]")
        
        # Last chunk should have "Continued from" but not "Continued in"
        last_chunk = chunks[-1]
        assert last_chunk.startswith("[Continued from previous section]")
        assert not last_chunk.endswith("[Continued in next section]")

    def test_merge_chunked_results_combines_summaries(self):
        """Test that chunked results are properly merged back together."""
        chunked_posts = [
            {
                'title': 'Long Post (Part 1/2)',
                'original_title': 'Long Post',
                'url': 'https://test.com/long',
                'chunk_index': 0,
                'total_chunks': 2,
                'summary': 'First part summary.',
                'seo_keywords': 'keyword1, keyword2',
                'funnel_stage': 'ToFu',
                'content_processing': {'chunked': True}
            },
            {
                'title': 'Long Post (Part 2/2)',
                'original_title': 'Long Post',
                'url': 'https://test.com/long',
                'chunk_index': 1,
                'total_chunks': 2,
                'summary': 'Second part summary.',
                'seo_keywords': 'keyword2, keyword3',
                'funnel_stage': 'ToFu',
                'content_processing': {'chunked': True}
            }
        ]
        
        merged = ContentPreprocessor.merge_chunked_results(chunked_posts)
        
        # Should result in single post
        assert len(merged) == 1
        
        merged_post = merged[0]
        assert merged_post['title'] == 'Long Post'
        assert merged_post['summary'] == 'First part summary. Second part summary.'
        assert merged_post['seo_keywords'] == 'keyword1, keyword2, keyword3'
        assert merged_post['funnel_stage'] == 'ToFu'
        
        # Chunk-specific fields should be removed
        assert 'original_title' not in merged_post
        assert 'chunk_index' not in merged_post
        assert 'total_chunks' not in merged_post

    def test_merge_chunked_results_deduplicates_keywords(self):
        """Test that duplicate keywords are removed during merging."""
        chunked_posts = [
            {
                'title': 'Test (Part 1/2)',
                'original_title': 'Test',
                'url': 'https://test.com/test',
                'chunk_index': 0,
                'total_chunks': 2,
                'seo_keywords': 'python, testing, automation',
                'content_processing': {'chunked': True}
            },
            {
                'title': 'Test (Part 2/2)',
                'original_title': 'Test',
                'url': 'https://test.com/test',
                'chunk_index': 1,
                'total_chunks': 2,
                'seo_keywords': 'testing, automation, quality',
                'content_processing': {'chunked': True}
            }
        ]
        
        merged = ContentPreprocessor.merge_chunked_results(chunked_posts)
        
        merged_post = merged[0]
        keywords = merged_post['seo_keywords'].split(', ')
        
        # Should have unique keywords in order of appearance
        assert 'python' in keywords
        assert 'testing' in keywords
        assert 'automation' in keywords
        assert 'quality' in keywords
        
        # Should not have duplicates
        assert len(keywords) == len(set(keywords))

    def test_merge_chunked_results_mixed_chunked_and_regular(self):
        """Test merging when some posts are chunked and others are not."""
        posts = [
            {
                'title': 'Regular Post',
                'url': 'https://test.com/regular',
                'summary': 'Regular summary',
                'content_processing': {'chunked': False}
            },
            {
                'title': 'Chunked Post (Part 1/2)',
                'original_title': 'Chunked Post',
                'url': 'https://test.com/chunked',
                'chunk_index': 0,
                'summary': 'Chunk 1 summary',
                'content_processing': {'chunked': True}
            },
            {
                'title': 'Chunked Post (Part 2/2)',
                'original_title': 'Chunked Post',
                'url': 'https://test.com/chunked',
                'chunk_index': 1,
                'summary': 'Chunk 2 summary',
                'content_processing': {'chunked': True}
            }
        ]
        
        merged = ContentPreprocessor.merge_chunked_results(posts)
        
        # Should result in 2 posts (regular + merged chunked)
        assert len(merged) == 2
        
        # Find the regular post
        regular_posts = [p for p in merged if p['title'] == 'Regular Post']
        assert len(regular_posts) == 1
        assert regular_posts[0]['summary'] == 'Regular summary'
        
        # Find the merged chunked post
        chunked_posts = [p for p in merged if p['title'] == 'Chunked Post']
        assert len(chunked_posts) == 1
        assert chunked_posts[0]['summary'] == 'Chunk 1 summary Chunk 2 summary'

    def test_handles_empty_content(self):
        """Test handling of posts with no content."""
        posts = [
            {
                'title': 'Empty Post',
                'url': 'https://test.com/empty',
                'content': '',
                'publication_date': '2025-01-01'
            }
        ]
        
        processed = ContentPreprocessor.prepare_posts_for_enrichment(posts)
        
        assert len(processed) == 1
        assert processed[0]['content'] == ''

    def test_handles_na_content(self):
        """Test handling of posts with N/A content."""
        posts = [
            {
                'title': 'NA Post',
                'url': 'https://test.com/na',
                'content': 'N/A',
                'publication_date': '2025-01-01'
            }
        ]
        
        processed = ContentPreprocessor.prepare_posts_for_enrichment(posts)
        
        assert len(processed) == 1
        assert processed[0]['content'] == 'N/A'