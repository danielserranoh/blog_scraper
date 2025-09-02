# src/di_container.py
# Dependency Injection Container for managing all manager dependencies

import logging
from typing import Optional

from .config_loader import load_configuration, get_competitors_to_process
from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)

class DIContainer:
    """
    Dependency Injection Container that manages the lifecycle and dependencies
    of all manager classes. This ensures consistent initialization and makes
    the system more testable and maintainable.
    """
    
    def __init__(self):
        self._app_config: Optional[dict] = None
        self._competitor_config: Optional[dict] = None
        self._state_manager: Optional['StateManager'] = None
        self._api_connector: Optional['GeminiAPIConnector'] = None
        self._batch_manager: Optional['BatchJobManager'] = None
        self._scraper_manager: Optional['ScraperManager'] = None
        self._enrichment_manager: Optional['EnrichmentManager'] = None
        self._export_manager: Optional['ExportManager'] = None
        
        # Load configurations immediately
        self._load_configurations()
    
    def _load_configurations(self):
        """Load application and competitor configurations."""
        try:
            self._app_config, self._competitor_config = load_configuration()
            if not self._app_config or not self._competitor_config:
                raise ConfigurationError("Failed to load required configurations")
            logger.debug("Configurations loaded successfully")
        except Exception as e:
            raise ConfigurationError(f"Configuration loading failed: {e}")
    
    @property
    def app_config(self) -> dict:
        """Get application configuration."""
        if self._app_config is None:
            raise ConfigurationError("Application configuration not available")
        return self._app_config
    
    @property
    def competitor_config(self) -> dict:
        """Get competitor configuration."""
        if self._competitor_config is None:
            raise ConfigurationError("Competitor configuration not available")
        return self._competitor_config
    
    @property
    def state_manager(self) -> 'StateManager':
        """Get or create StateManager instance."""
        if self._state_manager is None:
            from .state_management.state_manager import StateManager
            self._state_manager = StateManager(self.app_config)
            logger.debug("StateManager initialized")
        return self._state_manager
    
    @property
    def api_connector(self) -> 'GeminiAPIConnector':
        """Get or create GeminiAPIConnector instance."""
        if self._api_connector is None:
            from .api_connector import GeminiAPIConnector
            self._api_connector = GeminiAPIConnector()
            logger.debug("GeminiAPIConnector initialized")
        return self._api_connector
    
    @property
    def batch_manager(self) -> 'BatchJobManager':
        """Get or create BatchJobManager instance."""
        if self._batch_manager is None:
            from .transform.batch_manager import BatchJobManager
            self._batch_manager = BatchJobManager(self.app_config)
            logger.debug("BatchJobManager initialized")
        return self._batch_manager
    
    @property
    def scraper_manager(self) -> 'ScraperManager':
        """Get or create ScraperManager instance."""
        if self._scraper_manager is None:
            from .extract.scraper_manager import ScraperManager
            self._scraper_manager = ScraperManager(self.app_config, self.state_manager)
            logger.debug("ScraperManager initialized")
        return self._scraper_manager
    
    @property
    def enrichment_manager(self) -> 'EnrichmentManager':
        """Get or create EnrichmentManager instance."""
        if self._enrichment_manager is None:
            from .transform.enrichment_manager import EnrichmentManager
            self._enrichment_manager = EnrichmentManager(
                self.app_config, 
                self.state_manager, 
                self.batch_manager
            )
            logger.debug("EnrichmentManager initialized")
        return self._enrichment_manager
    
    @property
    def export_manager(self) -> 'ExportManager':
        """Get or create ExportManager instance."""
        if self._export_manager is None:
            from .load.export_manager import ExportManager
            self._export_manager = ExportManager(self.app_config, self.state_manager)
            logger.debug("ExportManager initialized")
        return self._export_manager
    
    def get_competitors_to_process(self, selected_competitor_name: Optional[str] = None) -> list:
        """Get filtered list of competitors to process."""
        return get_competitors_to_process(self.competitor_config, selected_competitor_name)
    
    def get_models(self) -> tuple[str, str]:
        """Get live and batch model names from configuration."""
        models = self.app_config.get('models', {})
        live_model = models.get('live', 'gemini-2.0-flash')
        batch_model = models.get('batch', 'gemini-2.0-flash-lite')
        return live_model, batch_model
    
    def get_batch_threshold(self) -> int:
        """Get batch processing threshold from configuration."""
        return self.app_config.get('batch_threshold', 10)