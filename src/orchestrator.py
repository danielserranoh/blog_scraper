# src/orchestrator.py
# This file contains the core application logic and workflow orchestration.

import logging
import asyncio
import os
from typing import Dict, Any, Optional

from .di_container import DIContainer
from .exceptions import (
    ETLError, 
    ScrapingError, 
    EnrichmentError, 
    StateError,
    ConfigurationError
)

logger = logging.getLogger(__name__)

async def run_pipeline(args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    The primary orchestration function that executes the ETL workflow.
    
    Args:
        args: Dictionary containing command-line arguments and flags
        
    Returns:
        Dictionary with execution results for LLM consumption, or None on success
    """
    try:
        # Initialize DI container
        container = DIContainer()
        
        # Get configuration values
        competitors_to_process = container.get_competitors_to_process(args.get('competitor'))
        if not competitors_to_process:
            raise ConfigurationError(
                f"No competitors found to process for: {args.get('competitor')}", 
                {"requested_competitor": args.get('competitor')}
            )
        
        live_model, batch_model = container.get_models()
        batch_threshold = container.get_batch_threshold()
        
        logger.info(f"Processing {len(competitors_to_process)} competitor(s) with pipeline")
        
        # Route to appropriate workflow
        if args.get('check_job'):
            return await _handle_check_job(container, competitors_to_process)
            
        elif args.get('export'):
            return await _handle_export(container, competitors_to_process, args.get('export_format'))
            
        elif args.get('enrich'):
            return await _handle_enrich_existing(container, competitors_to_process, live_model, batch_model, batch_threshold, args.get('wait', False))
            
        elif args.get('enrich_raw'):
            return await _handle_enrich_raw(container, competitors_to_process, live_model, batch_model, batch_threshold, args.get('wait', False))
            
        elif args.get('scrape'):
            return await _handle_scrape_only(container, competitors_to_process, args.get('days'), args.get('all', False))
            
        elif args.get('get_posts'):
            return await _handle_get_posts(container, competitors_to_process, args.get('days'), args.get('all', False), live_model, batch_model, batch_threshold, args.get('wait', False))
        
        else:
            raise ConfigurationError("No valid command specified", {"available_commands": ["check_job", "export", "enrich", "enrich_raw", "scrape", "get_posts"]})
    
    except ETLError as e:
        logger.error(f"ETL Error: {e.message}")
        return e.to_dict()
    except Exception as e:
        logger.error(f"Unexpected error in pipeline: {e}")
        error = ETLError(f"Unexpected pipeline error: {str(e)}", "PIPELINE_ERROR")
        return error.to_dict()


async def _handle_check_job(container: DIContainer, competitors: list) -> Optional[Dict[str, Any]]:
    """Handle checking batch job status."""
    try:
        results = []
        for competitor in competitors:
            result = await container.batch_manager.check_and_load_results(competitor, container.app_config)
            if result:
                results.extend(result)
        
        logger.info(f"Job check completed for {len(competitors)} competitor(s)")
        return {"success": True, "operation": "check_job", "results_count": len(results)}
        
    except Exception as e:
        raise EnrichmentError(f"Failed to check batch jobs: {str(e)}", details={"competitors": [c['name'] for c in competitors]})


async def _handle_export(container: DIContainer, competitors: list, export_format: str) -> Optional[Dict[str, Any]]:
    """Handle data export operations."""
    try:
        # Check jobs first to ensure latest data
        for competitor in competitors:
            await container.batch_manager.check_and_load_results(competitor, container.app_config)
        
        container.export_manager.run_export_process(competitors, export_format, container.app_config)
        
        logger.info(f"Export completed in {export_format} format for {len(competitors)} competitor(s)")
        return {"success": True, "operation": "export", "format": export_format, "competitors_count": len(competitors)}
        
    except Exception as e:
        raise ExportError(f"Export failed: {str(e)}", format_type=export_format, competitors=competitors)


async def _handle_enrich_existing(container: DIContainer, competitors: list, live_model: str, batch_model: str, batch_threshold: int, wait: bool) -> Optional[Dict[str, Any]]:
    """Handle enrichment of existing processed data."""
    try:
        total_enriched = 0
        for competitor in competitors:
            all_posts_from_file, posts_to_enrich = container.enrichment_manager._find_posts_to_enrich(competitor['name'])
            
            if not posts_to_enrich:
                logger.info(f"No posts found that require enrichment for '{competitor['name']}'.")
                continue

            logger.info(f"Will enrich {len(posts_to_enrich)} posts for '{competitor['name']}'.")
            
            final_posts = await container.enrichment_manager.enrich_posts(
                competitor,
                posts_to_enrich,
                all_posts_from_file,
                batch_threshold,
                live_model,
                batch_model,
                wait,
                source_raw_filepath=None
            )
            
            if final_posts:
                container.state_manager.save_processed_data(final_posts, competitor['name'], "enrichment_update.json")
                total_enriched += len(final_posts)

        logger.info(f"Enrichment process completed - {total_enriched} posts processed")
        return {"success": True, "operation": "enrich", "posts_enriched": total_enriched}
        
    except Exception as e:
        raise EnrichmentError(f"Enrichment failed: {str(e)}", details={"competitors": [c['name'] for c in competitors]})


async def _handle_enrich_raw(container: DIContainer, competitors: list, live_model: str, batch_model: str, batch_threshold: int, wait: bool) -> Optional[Dict[str, Any]]:
    """Handle enrichment of raw scraped data."""
    try:
        total_enriched = 0
        for competitor in competitors:
            raw_posts = container.state_manager.load_raw_data(competitor['name'])
            
            if not raw_posts:
                logger.info(f"No raw data found for '{competitor['name']}'.")
                continue
                
            logger.info(f"Found {len(raw_posts)} raw posts to enrich for '{competitor['name']}'.")
            
            latest_raw_filepath = container.state_manager.get_latest_raw_filepath(competitor['name'])
            
            final_posts = await container.enrichment_manager.enrich_posts(
                competitor,
                raw_posts,
                raw_posts,
                batch_threshold,
                live_model,
                batch_model,
                wait,
                source_raw_filepath=latest_raw_filepath
            )
            
            if final_posts:
                container.state_manager.save_processed_data(final_posts, competitor['name'], os.path.basename(latest_raw_filepath) if latest_raw_filepath else "raw_enrichment.json")
                total_enriched += len(final_posts)

        logger.info(f"Raw enrichment process completed - {total_enriched} posts processed")
        return {"success": True, "operation": "enrich_raw", "posts_enriched": total_enriched}
        
    except Exception as e:
        raise EnrichmentError(f"Raw enrichment failed: {str(e)}", details={"competitors": [c['name'] for c in competitors]})


async def _handle_scrape_only(container: DIContainer, competitors: list, days: Optional[int], scrape_all: bool) -> Optional[Dict[str, Any]]:
    """Handle scraping-only operations (no enrichment)."""
    try:
        total_scraped = 0
        for competitor in competitors:
            logger.info(f"--- Starting scrape-only process for '{competitor['name']}' ---")
            
            # Scrape and save raw data
            scraped_posts = await container.scraper_manager.scrape_and_return_posts(
                competitor, days, scrape_all
            )
            
            if not scraped_posts:
                logger.info(f"No new posts found for '{competitor['name']}'")
                continue
            
            # Save raw data only
            raw_filepath = container.state_manager.save_raw_data(scraped_posts, competitor['name'])
            
            if not raw_filepath:
                raise StateError(f"Failed to save raw data for {competitor['name']}", operation="save_raw_data")
            
            total_scraped += len(scraped_posts)
            logger.info(f"Scraped and saved {len(scraped_posts)} posts for '{competitor['name']}'")

        logger.info(f"Scrape-only process completed - {total_scraped} posts scraped")
        return {"success": True, "operation": "scrape", "posts_scraped": total_scraped}
        
    except Exception as e:
        raise ScrapingError(f"Scraping failed: {str(e)}", details={"competitors": [c['name'] for c in competitors]})


async def _handle_get_posts(container: DIContainer, competitors: list, days: Optional[int], scrape_all: bool, live_model: str, batch_model: str, batch_threshold: int, wait: bool) -> Optional[Dict[str, Any]]:
    """Handle full pipeline: scrape + enrich + save processed data."""
    try:
        total_processed = 0
        for competitor in competitors:
            logger.info(f"--- Starting full pipeline for '{competitor['name']}' ---")
            
            # 1. Scrape new posts
            scraped_posts = await container.scraper_manager.scrape_and_return_posts(
                competitor, days, scrape_all
            )
            
            if not scraped_posts:
                logger.info(f"No new posts found for '{competitor['name']}'")
                continue
            
            # 2. Save raw data
            raw_filepath = container.state_manager.save_raw_data(scraped_posts, competitor['name'])

            if not raw_filepath:
                raise StateError(f"Failed to save raw data for {competitor['name']}", operation="save_raw_data")

            # 3. Enrich the scraped posts
            logger.info(f"--- Starting enrichment for {len(scraped_posts)} scraped posts ---")
            final_posts = await container.enrichment_manager.enrich_posts(
                competitor,
                scraped_posts,
                scraped_posts, # all_posts_for_merge is the same as scraped posts for new data
                batch_threshold,
                live_model,
                batch_model,
                wait,
                source_raw_filepath=raw_filepath
            )
            
            # 4. Save processed data
            if final_posts:
                container.state_manager.save_processed_data(final_posts, competitor['name'], os.path.basename(raw_filepath))
                total_processed += len(final_posts)
                logger.info(f"Completed processing {len(final_posts)} posts for '{competitor['name']}'")

        logger.info(f"Full pipeline completed - {total_processed} posts processed")
        return {"success": True, "operation": "get_posts", "posts_processed": total_processed}
        
    except Exception as e:
        raise ScrapingError(f"Full pipeline failed: {str(e)}", details={"competitors": [c['name'] for c in competitors]})