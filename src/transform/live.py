# src/transform/live.py
# This file contains the high-level router for live API processing.

import logging
from src.api_connector import GeminiAPIConnector

logger = logging.getLogger(__name__)

# Instantiate the connector here so it's ready for use
connector = GeminiAPIConnector()

async def transform_posts_live(posts, model_name):
    """
    Transforms a batch of extracted post data by enriching it with live,
    asynchronous calls to the Gemini API via the connector.
    """
    return await connector.batch_enrich_posts_live(posts, model_name)