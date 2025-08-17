# src/transform/live.py
# This file contains the high-level router for live API processing.

import logging
from src.api_connector import GeminiAPIConnector

logger = logging.getLogger(__name__)

async def transform_posts_live(posts, model_name):
    """
    Transforms a batch of extracted post data by enriching it with live,
    asynchronous calls to the Gemini API via the connector.
    """
    connector = GeminiAPIConnector() # <-- The connector is now instantiated here
    if not connector.client:
        return posts # Return original posts if connector failed
    
    return await connector.batch_enrich_posts_live(posts, model_name)