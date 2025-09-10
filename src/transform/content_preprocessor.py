# src/transform/content_preprocessor.py
# Content preprocessing utilities for API consumption

import logging
import re
from typing import List, Dict, Any, Tuple
from src import utils

logger = logging.getLogger(__name__)

class ContentPreprocessor:
    """
    Handles content preprocessing for API consumption, including cleaning,
    chunking, and validation. This ensures consistent content preparation
    across both live and batch enrichment workflows.
    """
    
    @classmethod
    def _get_config_values(cls):
        """Get content processing configuration values from config file."""
        config = utils.get_content_processing_config()
        return (
            config['max_content_length'],
            config['chunk_size'], 
            config['chunk_overlap']
        )
    
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
        max_content_length, chunk_size, chunk_overlap = cls._get_config_values()
        
        for post in posts:
            content = post.get('content', '')
            title = post.get('title', 'Unknown Post')
            
            if not content or content == 'N/A':
                processed_posts.append(post)
                continue
            
            # Clean the content
            cleaned_content = cls._clean_content(content)
            
            # Check if content needs chunking
            if len(cleaned_content) <= max_content_length:
                # Content fits in single request
                processed_post = post.copy()
                processed_post['content'] = cleaned_content
                
                # Initialize metadata structure if not present
                if 'metadata' not in processed_post:
                    processed_post['metadata'] = {}
                    
                # Calculate content metrics
                word_count = len(cleaned_content.split())
                reading_time_minutes = round(word_count / 225, 1)  # 225 words per minute average
                
                # Content structure analysis
                structure_metrics = cls._analyze_content_structure(cleaned_content)
                
                processed_post['metadata']['content_processing'] = {
                    'original_length': len(content),
                    'processed_length': len(cleaned_content),
                    'word_count': word_count,
                    'reading_time_minutes': reading_time_minutes,
                    'chunked': False,
                    'cleaning_applied': cleaned_content != content,
                    **structure_metrics
                }
                processed_posts.append(processed_post)
            else:
                # Content needs chunking
                logger.info(f"Content for '{title}' ({len(cleaned_content)} chars) needs chunking")
                chunks = cls._create_content_chunks(cleaned_content, title, chunk_size, chunk_overlap)
                
                for i, chunk in enumerate(chunks):
                    chunk_post = post.copy()
                    chunk_post['content'] = chunk
                    chunk_post['title'] = f"{title} (Part {i+1}/{len(chunks)})"
                    chunk_post['original_title'] = title
                    chunk_post['chunk_index'] = i
                    chunk_post['total_chunks'] = len(chunks)
                    
                    # Initialize metadata structure if not present
                    if 'metadata' not in chunk_post:
                        chunk_post['metadata'] = {}
                    
                    # Calculate metrics for this chunk
                    chunk_word_count = len(chunk.split())
                    chunk_reading_time = round(chunk_word_count / 225, 1)
                    
                    chunk_post['metadata']['content_processing'] = {
                        'original_length': len(content),
                        'chunk_length': len(chunk),
                        'chunk_word_count': chunk_word_count,
                        'chunk_reading_time_minutes': chunk_reading_time,
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
    def _analyze_content_structure(cls, content: str) -> Dict[str, Any]:
        """
        Analyzes content structure and complexity metrics.
        
        Args:
            content: Cleaned content string
            
        Returns:
            Dictionary of structure metrics
        """
        if not content:
            return {}
        
        # Count headings (markdown style)
        heading_count = len(re.findall(r'^#+\s+', content, re.MULTILINE))
        
        # Count paragraphs (double line breaks)
        paragraph_count = len([p for p in content.split('\n\n') if p.strip()])
        
        # Count lists (markdown bullets and numbers)
        bullet_list_items = len(re.findall(r'^\s*[-*+]\s+', content, re.MULTILINE))
        numbered_list_items = len(re.findall(r'^\s*\d+\.\s+', content, re.MULTILINE))
        total_list_items = bullet_list_items + numbered_list_items
        
        # Count sentences for complexity analysis
        sentences = re.split(r'[.!?]+', content)
        sentences = [s.strip() for s in sentences if s.strip()]
        sentence_count = len(sentences)
        
        # Calculate average sentence length
        avg_sentence_length = 0
        if sentences:
            total_sentence_words = sum(len(sentence.split()) for sentence in sentences)
            avg_sentence_length = round(total_sentence_words / len(sentences), 1)
        
        # Detect media elements (basic patterns)
        image_count = len(re.findall(r'!\[.*?\]\(.*?\)|<img\s+|image:', content, re.IGNORECASE))
        code_block_count = len(re.findall(r'```|<code>|<pre>', content, re.IGNORECASE))
        link_count = len(re.findall(r'\[.*?\]\(.*?\)|<a\s+href|http[s]?://', content))
        
        return {
            'heading_count': heading_count,
            'paragraph_count': paragraph_count,
            'sentence_count': sentence_count,
            'avg_sentence_length': avg_sentence_length,
            'list_items_count': total_list_items,
            'bullet_points': bullet_list_items,
            'numbered_items': numbered_list_items,
            'image_count': image_count,
            'code_block_count': code_block_count,
            'link_count': link_count
        }
    
    @classmethod
    def _create_content_chunks(cls, content: str, title: str, chunk_size: int = 30000, chunk_overlap: int = 500) -> List[str]:
        """
        Creates intelligent chunks from long content.
        
        Args:
            content: Content to chunk
            title: Title for logging purposes
            chunk_size: Size of each chunk
            chunk_overlap: Overlap between chunks
            
        Returns:
            List of content chunks
        """
        if len(content) <= chunk_size:
            return [content]
        
        chunks = []
        current_pos = 0
        
        while current_pos < len(content):
            # Determine chunk end position
            chunk_end = min(current_pos + chunk_size, len(content))
            
            # Try to find a good break point (sentence ending)
            if chunk_end < len(content):
                # Look for sentence endings within the last 20% of the chunk
                search_start = max(current_pos + int(chunk_size * 0.8), current_pos + 500)
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
                current_pos = chunk_end - chunk_overlap
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
            # Check both new metadata structure and old structure for backward compatibility
            processing_info = post.get('metadata', {}).get('content_processing', {})
            if not processing_info:
                processing_info = post.get('content_processing', {})  # Fallback to old structure
            
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
                
                # Update processing info in metadata structure
                if 'metadata' not in merged_post:
                    merged_post['metadata'] = {}
                    
                merged_post['metadata']['content_processing'] = {
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