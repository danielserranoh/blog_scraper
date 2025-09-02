# src/di_container.py
# Dependency Injection Container for managing all manager dependencies

import logging
from typing import Optional

from .config_loader import load_configuration, get_competitors_to_process
from .extract.scraper_manager import ScraperManager
from .transform.enrichment_manager import EnrichmentManager
from .transform.batch_manager import BatchJobManager
from .load.export_manager import ExportManager
from .state_management.state_manager import StateManager
from .api_connector import GeminiAPIConnector

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
        self._state_manager: Optional[StateManager] = None
        self._api_connector: Optional[GeminiAPIConnector] = None
        self._batch_manager: Optional[BatchJobManager] = None
        self._scraper_manager: Optional[ScraperManager] = None
        self._enrichment_manager: Optional[EnrichmentManager] = None
        self._export_manager: Optional[ExportManager] = None
        
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
    def state_manager(self) -> StateManager:
        """Get or create StateManager instance."""
        if self._state_manager is None:
            self._state_manager = StateManager(self.app_config)
            logger.debug("StateManager initialized")
        return self._state_manager
    
    @property
    def api_connector(self) -> GeminiAPIConnector:
        """Get or create GeminiAPIConnector instance."""
        if self._api_connector is None:
            self._api_connector = GeminiAPIConnector()
            logger.debug("GeminiAPIConnector initialized")
        return self._api_connector
    
    @property
    def batch_manager(self) -> BatchJobManager:
        """Get or create BatchJobManager instance."""
        if self._batch_manager is None:
            self._batch_manager = BatchJobManager(self.app_config)
            logger.debug("BatchJobManager initialized")
        return self._batch_manager
    
    @property
    def scraper_manager(self) -> ScraperManager:
        """Get or create ScraperManager instance."""
        if self._scraper_manager is None:
            self._scraper_manager = ScraperManager(self.app_config, self.state_manager)
            logger.debug("ScraperManager initialized")
        return self._scraper_manager
    
    @property
    def enrichment_manager(self) -> EnrichmentManager:
        """Get or create EnrichmentManager instance."""
        if self._enrichment_manager is None:
            self._enrichment_manager = EnrichmentManager(
                self.app_config, 
                self.state_manager, 
                self.batch_manager
            )
            logger.debug("EnrichmentManager initialized")
        return self._enrichment_manager
    
    @property
    def export_manager(self) -> ExportManager:
        """Get or create ExportManager instance."""
        if self._export_manager is None:
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


# Custom Exceptions for LLM-friendly error handling
class ETLError(Exception):
    """Base exception for ETL pipeline errors."""
    
    def __init__(self, message: str, error_code: str = "ETL_UNKNOWN", details: dict = None):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary for LLM consumption."""
        return {
            "error": True,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "error_type": self.__class__.__name__
        }


class ConfigurationError(ETLError):
    """Raised when configuration loading or validation fails."""
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, "CONFIG_ERROR", details)


class ScrapingError(ETLError):
    """Raised when scraping operations fail."""
    
    def __init__(self, message: str, competitor: str = None, urls: list = None, details: dict = None):
        error_details = details or {}
        if competitor:
            error_details["competitor"] = competitor
        if urls:
            error_details["failed_urls"] = urls
        super().__init__(message, "SCRAPING_ERROR", error_details)


class EnrichmentError(ETLError):
    """Raised when enrichment operations fail."""
    
    def __init__(self, message: str, posts_count: int = None, model: str = None, details: dict = None):
        error_details = details or {}
        if posts_count is not None:
            error_details["posts_count"] = posts_count
        if model:
            error_details["model"] = model
        super().__init__(message, "ENRICHMENT_ERROR", error_details)


class StateError(ETLError):
    """Raised when state management operations fail."""
    
    def __init__(self, message: str, operation: str = None, file_path: str = None, details: dict = None):
        error_details = details or {}
        if operation:
            error_details["operation"] = operation
        if file_path:
            error_details["file_path"] = file_path
        super().__init__(message, "STATE_ERROR", error_details)


class BatchJobError(ETLError):
    """Raised when batch job operations fail."""
    
    def __init__(self, message: str, job_id: str = None, status: str = None, details: dict = None):
        error_details = details or {}
        if job_id:
            error_details["job_id"] = job_id
        if status:
            error_details["status"] = status
        super().__init__(message, "BATCH_JOB_ERROR", error_details)


class ExportError(ETLError):
    """Raised when export operations fail."""
    
    def __init__(self, message: str, format_type: str = None, competitors: list = None, details: dict = None):
        error_details = details or {}
        if format_type:
            error_details["format"] = format_type
        if competitors:
            error_details["competitors"] = [c.get('name', 'unknown') for c in competitors]
        super().__init__(message, "EXPORT_ERROR", error_details)