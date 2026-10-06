import logging
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict
from contextlib import contextmanager
import time
import uuid
from functools import wraps

from config import config


class StructuredLogger:
    """Structured logging with JSON format for better monitoring"""
    
    def __init__(self, name: str = "nordigen_ingestion"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, config.log_level))
        
        # Remove existing handlers
        self.logger.handlers = []
        
        # Create JSON formatter
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self.JsonFormatter())
        self.logger.addHandler(handler)
        
        # Correlation ID for tracking requests
        self.correlation_id = None
        
    class JsonFormatter(logging.Formatter):
        """Custom JSON formatter for structured logs"""
        
        def format(self, record):
            log_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": record.levelname,
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno
            }
            
            # Add extra fields if present
            if hasattr(record, "extra_fields"):
                log_data.update(record.extra_fields)
            
            # Add exception info if present
            if record.exc_info:
                log_data["exception"] = self.formatException(record.exc_info)
            
            return json.dumps(log_data, default=str)
    
    def _log(self, level: str, message: str, **kwargs):
        """Internal logging method with extra fields"""
        extra_fields = {
            "correlation_id": self.correlation_id or str(uuid.uuid4()),
            "environment": config.environment,
            "project_id": config.project_id
        }
        extra_fields.update(kwargs)
        
        getattr(self.logger, level)(
            message,
            extra={"extra_fields": extra_fields}
        )
    
    def info(self, message: str, **kwargs):
        self._log("info", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log("warning", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log("error", message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        self._log("debug", message, **kwargs)
    
    @contextmanager
    def timer(self, operation: str, **extra_fields):
        """Context manager for timing operations"""
        start_time = time.time()
        self.info(f"Starting {operation}", operation=operation, **extra_fields)
        
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.info(
                f"Completed {operation}",
                operation=operation,
                duration_seconds=round(duration, 3),
                **extra_fields
            )
    
    def set_correlation_id(self, correlation_id: str):
        """Set correlation ID for request tracking"""
        self.correlation_id = correlation_id
    
    def log_metric(self, metric_name: str, value: float, unit: str = "count", **tags):
        """Log a metric for monitoring"""
        self.info(
            f"Metric: {metric_name}",
            metric_name=metric_name,
            metric_value=value,
            metric_unit=unit,
            metric_type="gauge",
            **tags
        )
    
    def log_error_with_context(self, error: Exception, context: Dict[str, Any]):
        """Log error with additional context"""
        self.error(
            f"Error occurred: {str(error)}",
            error_type=type(error).__name__,
            error_message=str(error),
            **context
        )


class ProcessingMetrics:
    """Track processing metrics for monitoring"""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
        self.metrics = {
            "accounts_processed": 0,
            "accounts_failed": 0,
            "tables_processed": 0,
            "tables_failed": 0,
            "records_processed": 0,
            "records_failed": 0,
            "api_calls": 0,
            "api_errors": 0,
            "processing_time": 0
        }
        self.start_time = time.time()
    
    def increment(self, metric: str, value: int = 1):
        """Increment a metric"""
        if metric in self.metrics:
            self.metrics[metric] += value
    
    def record_processing(self, success: bool, record_count: int = 0, 
                         entity_type: str = "account"):
        """Record processing result"""
        if entity_type == "account":
            if success:
                self.increment("accounts_processed")
            else:
                self.increment("accounts_failed")
        elif entity_type == "table":
            if success:
                self.increment("tables_processed")
            else:
                self.increment("tables_failed")
        
        if success and record_count > 0:
            self.increment("records_processed", record_count)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        self.metrics["processing_time"] = round(time.time() - self.start_time, 2)
        return self.metrics.copy()
    
    def log_summary(self):
        """Log metrics summary"""
        summary = self.get_summary()
        self.logger.info("Processing metrics summary", **summary)
        
        # Log individual metrics for monitoring
        for metric_name, value in summary.items():
            self.logger.log_metric(f"nordigen.{metric_name}", value)


def log_execution(logger: StructuredLogger):
    """Decorator for logging function execution"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            logger.debug(f"Executing {func_name}", function=func_name)
            
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Completed {func_name}", function=func_name)
                return result
            except Exception as e:
                logger.error(
                    f"Error in {func_name}: {str(e)}",
                    function=func_name,
                    error_type=type(e).__name__
                )
                raise
        
        return wrapper
    return decorator


# Global logger instance
logger = StructuredLogger()