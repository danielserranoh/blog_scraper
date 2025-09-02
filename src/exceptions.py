# src/exceptions.py
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