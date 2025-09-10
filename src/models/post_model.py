# src/models/post_model.py
# Data model definitions for blog posts and enrichment data

from typing import Dict, List, Optional, Any

class PostModel:
    """
    Defines the expected structure for enriched blog posts.
    Used to validate completeness and determine if re-enrichment is needed.
    """
    
    # Required basic fields from scraping
    REQUIRED_BASIC_FIELDS = {
        'title': str,
        'url': str, 
        'publication_date': str,
        'content': str,
    }
    
    # Optional basic fields from scraping
    OPTIONAL_BASIC_FIELDS = {
        'seo_meta_keywords': str,
        'headings': list,
        'schemas': list,
    }
    
    # Required enrichment fields from API
    REQUIRED_ENRICHMENT_FIELDS = {
        'summary': str,
        'seo_keywords': str,
        'funnel_stage': str,
        'target_audience': str,
        'strategic_analysis': dict,
    }
    
    # Expected strategic analysis sub-fields
    REQUIRED_STRATEGIC_ANALYSIS_FIELDS = {
        'content_angle': str,
        'competitive_differentiation': str,
        'content_freshness_score': str,  # Can be string like "8/10 - Current trends"
        'target_persona_indicators': str,
        'content_depth': str,
    }
    
    # Organized metadata structure
    METADATA_FIELDS = {
        'metadata': {
            'content_processing': dict,  # Chunking and preprocessing info
            'enrichment_status': str,    # Status: completed, failed, no_content
            'competitor': str,           # Competitor name for filtering
        }
    }
    
    @classmethod
    def get_all_expected_fields(cls) -> Dict[str, type]:
        """Returns all expected fields and their types for a complete post."""
        all_fields = {}
        all_fields.update(cls.REQUIRED_BASIC_FIELDS)
        all_fields.update(cls.OPTIONAL_BASIC_FIELDS) 
        all_fields.update(cls.REQUIRED_ENRICHMENT_FIELDS)
        all_fields.update(cls.METADATA_FIELDS)
        return all_fields
    
    @classmethod
    def needs_enrichment(cls, post: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Determines if a post needs enrichment by checking for missing or invalid enrichment fields.
        
        Args:
            post: Post dictionary to check
            
        Returns:
            Tuple of (needs_enrichment: bool, missing_fields: List[str])
        """
        missing_fields = []
        
        # Check basic enrichment fields
        for field, expected_type in cls.REQUIRED_ENRICHMENT_FIELDS.items():
            value = post.get(field)
            
            if field == 'strategic_analysis':
                # Special handling for strategic analysis
                if not value or not isinstance(value, dict):
                    missing_fields.append(field)
                else:
                    # Check strategic analysis sub-fields
                    for sub_field in cls.REQUIRED_STRATEGIC_ANALYSIS_FIELDS:
                        sub_value = value.get(sub_field)
                        if not sub_value or sub_value in ['N/A', '', None]:
                            missing_fields.append(f"strategic_analysis.{sub_field}")
            else:
                # Check basic enrichment fields
                if not value or value in ['N/A', '', None]:
                    missing_fields.append(field)
        
        # Check enrichment status for explicit failures (in metadata structure)
        metadata = post.get('metadata', {})
        if metadata.get('enrichment_status') == 'failed':
            if 'metadata.enrichment_status' not in missing_fields:
                missing_fields.append('metadata.enrichment_status')
        
        # Check if post has content to enrich
        has_content = post.get('content') and post.get('content') != 'N/A' and len(post.get('content', '').strip()) > 10
        if not has_content:
            # Posts without content don't need enrichment
            return False, []
            
        needs_enrichment = len(missing_fields) > 0
        return needs_enrichment, missing_fields
    
    @classmethod 
    def validate_post_structure(cls, post: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validates if a post has the expected structure.
        
        Args:
            post: Post dictionary to validate
            
        Returns:
            Tuple of (is_valid: bool, issues: List[str])
        """
        issues = []
        
        # Check required basic fields
        for field, expected_type in cls.REQUIRED_BASIC_FIELDS.items():
            if field not in post:
                issues.append(f"Missing required field: {field}")
            elif not isinstance(post[field], expected_type):
                issues.append(f"Field '{field}' should be {expected_type.__name__}, got {type(post[field]).__name__}")
        
        is_valid = len(issues) == 0
        return is_valid, issues