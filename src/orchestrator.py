# src/orchestrator.py
# This file contains the core application logic and workflow orchestration.

import os
import json
import logging
import asyncio
import time
import csv
from datetime import datetime
from types import SimpleNamespace

# Import refactored modules
from .config_loader import load_configuration, get_competitors_to_process
from .extract.scraper_manager import ScraperManager
from .transform.enrichment_manager import EnrichmentManager
from .transform.batch_manager import BatchJobManager
from .load.export_manager import ExportManager
from src.state_management.state_manager import StateManager

logger = logging.getLogger(__name__)

async def run_pipeline(args):
    """The primary orchestration function that executes the ETL workflow."""
    app_config, competitor_config = load_configuration()
    if not app_config or not competitor_config:
        return
        
    batch_threshold = app_config.get('batch_threshold', 10)
    live_model = app_config.get('models', {}).get('live', 'gemini-2.0-flash')
    batch_model = app_config.get('models', {}).get('batch', 'gemini-2.0-flash-lite')

    competitors_to_process = get_competitors_to_process(competitor_config, args.get('competitor'))
    if not competitors_to_process:
        return

    # Instantiate manager classes
    state_manager = StateManager(app_config)
    batch_manager = BatchJobManager(app_config)
    scraper_manager = ScraperManager(app_config, state_manager)
    enrichment_manager = EnrichmentManager(app_config, state_manager, batch_manager)
    export_manager = ExportManager(app_config, state_manager)

    if args.get('check_job'):
        for competitor in competitors_to_process:
            await batch_manager.check_and_load_results(competitor, app_config)
            
    elif args.get('export'):
        for competitor in competitors_to_process:
            await batch_manager.check_and_load_results(competitor, app_config)
        export_manager.run_export_process(competitors_to_process, args.get('export_format'), app_config)

    elif args.get('enrich'):
        logger.info("--- Enrichment process ---")
        for competitor in competitors_to_process:
            all_posts_from_file, posts_to_enrich = enrichment_manager._find_posts_to_enrich(competitor['name'])
            
            if not posts_to_enrich:
                logger.info(f"No posts found that require enrichment for '{competitor['name']}'.")
                continue

            logger.info(f"Will enrich {len(posts_to_enrich)} posts for '{competitor['name']}'.")
            
            final_posts = await enrichment_manager.enrich_posts(
                competitor,
                posts_to_enrich,
                all_posts_from_file,
                batch_threshold,
                live_model,
                batch_model,
                args.get('wait'),
                source_raw_filepath=None
            )
            
            if final_posts:
                state_manager.save_processed_data(final_posts, competitor['name'], "placeholder.csv")

    elif args.get('enrich_raw'):
        logger.info("--- Raw data enrichment process ---")
        for competitor in competitors_to_process:
            raw_posts = state_manager.load_raw_data(competitor['name'])
            
            if not raw_posts:
                logger.info(f"No raw data found for '{competitor['name']}'.")
                continue
                
            logger.info(f"Found {len(raw_posts)} raw posts to enrich for '{competitor['name']}'.")
            
            latest_raw_filepath = state_manager.get_latest_raw_filepath(competitor['name'])
            
            final_posts = await enrichment_manager.enrich_posts(
                competitor,
                raw_posts,
                raw_posts,
                batch_threshold,
                live_model,
                batch_model,
                args.get('wait'),
                source_raw_filepath=latest_raw_filepath
            )
            
            if final_posts:
                state_manager.save_processed_data(final_posts, competitor['name'], "placeholder.csv")

    elif args.get('scrape') or args.get('get_posts'):
        for competitor in competitors_to_process:
            logger.info("--- Starting scrape process ---")
            
            # 1. Orchestrator tells ScraperManager to scrape and return posts.
            all_posts = await scraper_manager.scrape_and_return_posts(
                competitor, args.get('days'), args.get('all')
            )
            
            if not all_posts:
                continue
            
            # 2. Orchestrator tells StateManager to save the raw data.
            raw_filepath = state_manager.save_raw_data(all_posts, competitor['name'])

            if not raw_filepath:
                logger.error(f"Failed to save raw data for {competitor['name']}, aborting enrichment.")
                continue

            if args.get('get_posts'):
                # 3. The Orchestrator calls enrich_posts with the raw data and its file path.
                logger.info("--- Starting enrichment process for scraped posts ---")
                final_posts = await enrichment_manager.enrich_posts(
                    competitor,
                    all_posts,
                    all_posts, # The all_posts_for_merge argument is now the new posts.
                    batch_threshold,
                    live_model,
                    batch_model,
                    args.get('wait'),
                    source_raw_filepath=raw_filepath
                )
                
                if final_posts:
                    state_manager.save_processed_data(final_posts, competitor['name'], os.path.basename(raw_filepath))
            
    if args.get('enrich'):
        process_name = "Enrichment"
    elif args.get('check_job'):
        process_name = "Job Check"
    elif args.get('export'):
        process_name = f"Export to {args.get('export_format').upper()}"
    elif args.get('enrich_raw'):
        process_name = "Raw Enrichment"
    elif args.get('get_posts'):
        process_name = "Scrape & Enrich"
    else:
        process_name = "Scraping"

    logger.info(f"\n--- {process_name} process completed ---")