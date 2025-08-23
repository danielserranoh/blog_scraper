# src/utils.py
# This file contains shared helper functions for the application.

import json
import logging

logger = logging.getLogger(__name__)

# --- Prompt Management ---
_PROMPTS = {}

def _load_prompts():
    """Loads the prompts from the config file into a global variable."""
    global _PROMPTS
    if not _PROMPTS:
        try:
            with open('config/config.json', 'r') as f:
                config = json.load(f)
                _PROMPTS = config.get('prompts', {})
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Could not load prompts from config.json: {e}")
            _PROMPTS = {}

def get_prompt(prompt_name, content, headings=None, primary_competitors=None, dxp_competitors=None):
    """
    Retrieves a prompt instruction from the config and combines it with the
    programmatic context.
    """
    if not _PROMPTS:
        _load_prompts()
    
    prompt_instruction = _PROMPTS.get(prompt_name)
    if not prompt_instruction:
        logger.error(f"Prompt instruction '{prompt_name}' not found in configuration.")
        return ""
    
    # --- UPDATED: Add tiered competitor lists to the prompt ---
    competitors_text = ""
    if primary_competitors:
        competitors_text += f"\n\nPrimary Competitors: {', '.join(primary_competitors)}"
    if dxp_competitors:
        competitors_text += f"\n\nOther DXP/CMS Competitors: {', '.join(dxp_competitors)}"

    # Combine the instruction with the context
    return f"{prompt_instruction}{competitors_text}\n\nContent: {content}"