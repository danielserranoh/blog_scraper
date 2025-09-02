# src/transform/content_preprocessor.py
# Content preprocessing utilities for API consumption

import logging
import re
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

class ContentPreprocessor:
    """
    Handles content preprocessing for API consumption, including cleaning,
    chunking, and validation. This ensures consistent content preparation
    across both live and batch enrichment workflows.
    """
    
    # Conservative limits that work reliably with API + prompt overhead
    MAX_CONTENT_LENGTH = 6000
    CHUNK_SIZE = 5000
    CHUNK_OVERLAP = 200
    
    @classmethod
    def prepare_posts_for_enrichment(cls, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepares a list of posts for enrichment by cleaning and processing content.
        
        Args:
            posts: List of post dictionaries
            
        Returns:
            List of processed post dictionaries, potentially with chunked posts
        """
        processed_posts = []
        
        for post in posts:
            content = post.get('content', '')
            title = post.get('title', 'Unknown Post')
            
            if not content or content == 'N/A':
                processed_posts.append(post)
                continue
            
            # Clean the content
            cleaned_content = cls._clean_content(content)
            
            # Check if content needs chunking
            if len(cleaned_content) <= cls.MAX_CONTENT_LENGTH:
                # Content fits in single request
                processed_post = post.copy()
                processed_post['content'] = cleaned_content
                processed_post['content_processing'] = {
                    'original_length': len(content),
                    'processed_length': len(cleaned_content),
                    'chunked': False,
                    'cleaning_applied': cleaned_content != content
                }
                processed_posts.append(processed_post)
            else:
                # Content needs chunking
                logger.info(f"Content for '{title}' ({len(cleaned_content)} chars) needs chunking")
                chunks = cls._create_content_chunks(cleaned_content, title)
                
                for i, chunk in enumerate(chunks):
                    chunk_post = post.copy()
                    chunk_post['content'] = chunk
                    chunk_post['title'] = f"{title} (Part {i+1}/{len(chunks)})"
                    chunk_post['original_title'] = title
                    chunk_post['chunk_index'] = i
                    chunk_post['total_chunks'] = len(chunks)
                    chunk_post['content_processing'] = {
                        'original_length': len(content),
                        'chunk_length': len(chunk),
                        'chunked': True,
                        'chunk_number': i + 1,
                        'total_chunks': len(chunks),
                        'cleaning_applied': True
                    }
                    processed_posts.append(chunk_post)
        
        original_count = len(posts)
        processed_count = len(processed_posts)
        
        if processed_count > original_count:
            logger.info(f"Content preprocessing: {original_count} posts became {processed_count} processable items")
        
        return processed_posts
    
    @classmethod
    def _clean_content(cls, content: str) -> str:
        """
        Cleans content by removing or replacing problematic characters.
        
        Args:
            content: Raw content string
            
        Returns:
            Cleaned content string
        """
        if not content:
            return content
            
        cleaned = content
        
        # Replace smart quotes and other problematic Unicode characters
        char_replacements = {
            ''': "'",    # Left single quotation mark
            ''': "'",    # Right single quotation mark
            '"': '"',    # Left double quotation mark  
            '"': '"',    # Right double quotation mark
            '—': ' - ',  # Em dash (with spaces for better readability)
            '–': '-',    # En dash
            '…': '...',  # Horizontal ellipsis
            '\u00a0': ' ',  # Non-breaking space
            '\u200b': '',   # Zero-width space
            '\u2019': "'",  # Right single quotation mark (alternative)
            '\u201c': '"',  # Left double quotation mark (alternative)
            '\u201d': '"',  # Right double quotation mark (alternative)
        }
        
        for old_char, new_char in char_replacements.items():
            cleaned = cleaned.replace(old_char, new_char)
        
        # Remove or replace other problematic patterns
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove HTML entities that might have been missed
        cleaned = re.sub(r'&[a-zA-Z0-9#]+;', ' ', cleaned)
        
        # Remove any remaining non-printable characters
        cleaned = ''.join(char for char in cleaned if char.isprintable() or char in '\n\t')
        
        return cleaned.strip()
    
    @classmethod
    def _create_content_chunks(cls, content: str, title: str) -> List[str]:
        """
        Creates intelligent chunks from long content.
        
        Args:
            content: Content to chunk
            title: Title for logging purposes
            
        Returns:
            List of content chunks
        """
        if len(content) <= cls.MAX_CONTENT_LENGTH:
            return [content]
        
        chunks = []
        current_pos = 0
        
        while current_pos < len(content):
            # Determine chunk end position
            chunk_end = min(current_pos + cls.CHUNK_SIZE, len(content))
            
            # Try to find a good break point (sentence ending)
            if chunk_end < len(content):
                # Look for sentence endings within the last 20% of the chunk
                search_start = max(current_pos + int(cls.CHUNK_SIZE * 0.8), current_pos + 500)
                search_area = content[search_start:chunk_end + 100]  # Look a bit ahead
                
                # Find sentence endings
                sentence_endings = []
                for match in re.finditer(r'[.!?]\s+[A-Z]', search_area):
                    sentence_endings.append(search_start + match.start() + 1)
                
                if sentence_endings:
                    chunk_end = sentence_endings[-1]  # Use the last sentence ending
            
            # Extract chunk
            chunk = content[current_pos:chunk_end].strip()
            
            # Add context markers for chunked content
            if current_pos > 0:
                chunk = "[Continued from previous section] " + chunk
            
            if chunk_end < len(content):
                chunk = chunk + " [Continued in next section]"
            
            chunks.append(chunk)
            
            # Move to next chunk with overlap
            if chunk_end < len(content):
                current_pos = chunk_end - cls.CHUNK_OVERLAP
                # Make sure we don't go backwards
                if current_pos <= 0:
                    current_pos = chunk_end
            else:
                break
        
        logger.info(f"Created {len(chunks)} chunks for '{title}' (avg {sum(len(c) for c in chunks) // len(chunks)} chars/chunk)")
        return chunks
    
    @classmethod
    def merge_chunked_results(cls, enriched_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merges results from chunked content back into single posts.
        
        Args:
            enriched_posts: List of enriched posts (may include chunks)
            
        Returns:
            List of posts with chunks merged back together
        """
        # Separate chunked and non-chunked posts
        chunked_groups = {}
        non_chunked = []
        
        for post in enriched_posts:
            processing_info = post.get('content_processing', {})
            
            if processing_info.get('chunked', False):
                original_title = post.get('original_title', post.get('title'))
                if original_title not in chunked_groups:
                    chunked_groups[original_title] = []
                chunked_groups[original_title].append(post)
            else:
                non_chunked.append(post)
        
        # Merge chunks back together
        merged_posts = non_chunked.copy()
        
        for original_title, chunks in chunked_groups.items():
            # Sort chunks by chunk_index
            chunks.sort(key=lambda x: x.get('chunk_index', 0))
            
            # Take the first chunk as the base and merge others into it
            if chunks:
                merged_post = chunks[0].copy()
                merged_post['title'] = original_title  # Restore original title
                
                # Combine summaries
                summaries = [chunk.get('summary', '') for chunk in chunks if chunk.get('summary') and chunk.get('summary') != 'N/A']
                if summaries:
                    merged_post['summary'] = ' '.join(summaries)
                
                # Combine and deduplicate keywords
                all_keywords = []
                for chunk in chunks:
                    keywords = chunk.get('seo_keywords', '')
                    if keywords and keywords != 'N/A':
                        all_keywords.extend([kw.strip() for kw in keywords.split(',')])
                
                # Deduplicate while preserving order
                unique_keywords = []
                seen = set()
                for kw in all_keywords:
                    kw_lower = kw.lower()
                    if kw_lower not in seen and kw.strip():
                        unique_keywords.append(kw)
                        seen.add(kw_lower)
                
                merged_post['seo_keywords'] = ', '.join(unique_keywords[:10])  # Top 10 keywords
                
                # Use the most common funnel_stage
                funnel_stages = [chunk.get('funnel_stage', '') for chunk in chunks if chunk.get('funnel_stage') and chunk.get('funnel_stage') != 'N/A']
                if funnel_stages:
                    # Use the most frequent stage, or first if tied
                    from collections import Counter
                    stage_counts = Counter(funnel_stages)
                    merged_post['funnel_stage'] = stage_counts.most_common(1)[0][0]
                
                # Update processing info
                merged_post['content_processing'] = {
                    'was_chunked': True,
                    'chunk_count': len(chunks),
                    'merged_back': True
                }
                
                # Remove chunk-specific fields
                for field in ['original_title', 'chunk_index', 'total_chunks']:
                    merged_post.pop(field, None)
                
                merged_posts.append(merged_post)
        
        if chunked_groups:
            logger.info(f"Merged {sum(len(chunks) for chunks in chunked_groups.values())} chunks back into {len(chunked_groups)} posts")
        
        return merged_posts