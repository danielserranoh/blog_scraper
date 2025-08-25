# src/orchestrator.py
# This file contains the core application logic and workflow orchestration.

import os
import json
import logging
import asyncio
import time
import csv
from datetime import datetime

# Import refactored modules
from .config_loader import load_configuration, get_competitors_to_process
from .extract.scraper_manager import ScraperManager
from .transform.enrichment_manager import EnrichmentManager
from .transform.batch_manager import BatchJobManager
from .load.export_manager import ExportManager

logger = logging.getLogger(__name__)

async def run_pipeline(args):
    """The primary orchestration function that executes the ETL workflow."""
    app_config, competitor_config = load_configuration()
    if not app_config or not competitor_config:
        return
        
    batch_threshold = app_config.get('batch_threshold', 10)
    live_model = app_config.get('models', {}).get('live', 'gemini-2.0-flash')
    batch_model = app_config.get('models', {}).get('batch', 'gemini-2.0-flash-lite')

    competitors_to_process = get_competitors_to_process(competitor_config, args.competitor)
    if not competitors_to_process:
        return

    # Instantiate manager classes
    batch_manager = BatchJobManager(app_config)
    scraper_manager = ScraperManager(app_config)
    enrichment_manager = EnrichmentManager(app_config)
    export_manager = ExportManager(app_config)

    if args.check_job:
        for competitor in competitors_to_process:
            await batch_manager.check_and_load_results(competitor, app_config)
            
    elif args.export:
        for competitor in competitors_to_process:
            await batch_manager.check_and_load_results(competitor, app_config)
        export_manager.run_export_process(competitors_to_process, args.export, app_config)

    elif args.enrich:
        logger.info("--- Enrichment process ---")
        for competitor in competitors_to_process:
            await batch_manager.check_and_load_results(competitor, app_config)
            await enrichment_manager.run_enrichment_process(
                competitor, batch_threshold, live_model, batch_model, app_config, args.wait
            )
    elif args.enrich_raw:
        logger.info("--- Raw data enrichment process ---")
        for competitor in competitors_to_process:
            await enrichment_manager.enrich_raw_data(
                competitor, batch_threshold, live_model, batch_model, app_config, args.wait
            )
    else:
        for competitor in competitors_to_process:
            await scraper_manager.run_scrape_and_submit(
                competitor, args.days, args.all, batch_threshold, live_model, batch_model, app_config, args.wait
            )
            
    if args.enrich:
        process_name = "Enrichment"
    elif args.check_job:
        process_name = "Job Check"
    elif args.export:
        process_name = f"Export to {args.export.upper()}"
    elif args.enrich_raw:
        process_name = "Raw Enrichment"
    else:
        process_name = "Scraping"

    logger.info(f"\n--- {process_name} process completed ---")