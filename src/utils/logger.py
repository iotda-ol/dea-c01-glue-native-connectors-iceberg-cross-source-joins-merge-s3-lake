"""
Logger Utility Module

Provides structured logging with CloudWatch integration.
"""

import logging
import sys
from typing import Optional
import json
from datetime import datetime


class StructuredLogger:
    """
    Structured logger with JSON formatting and CloudWatch support.
    
    Provides consistent logging format with context enrichment
    for better log analysis and monitoring.
    """
    
    def __init__(self, 
                 name: str,
                 level: str = 'INFO',
                 structured: bool = True):
        """
        Initialize structured logger.
        
        Args:
            name: Logger name (typically module name)
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            structured: Whether to use JSON structured logging
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.structured = structured
        
        # Remove existing handlers
        self.logger.handlers = []
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, level.upper()))
        
        # Set formatter
        if structured:
            formatter = StructuredFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional context."""
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message with optional context."""
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional context."""
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with optional context."""
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with optional context."""
        self._log(logging.CRITICAL, message, **kwargs)
    
    def _log(self, level: int, message: str, **kwargs):
        """
        Internal log method with context support.
        
        Args:
            level: Log level
            message: Log message
            **kwargs: Additional context to include in structured log
        """
        if self.structured and kwargs:
            extra = {'context': kwargs}
            self.logger.log(level, message, extra=extra)
        else:
            self.logger.log(level, message)


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add context if present
        if hasattr(record, 'context'):
            log_data['context'] = record.context
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def get_logger(name: str, 
               level: str = 'INFO',
               structured: bool = True) -> StructuredLogger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name
        level: Log level
        structured: Whether to use structured logging
        
    Returns:
        Configured StructuredLogger instance
    """
    return StructuredLogger(name, level, structured)


def configure_glue_logger() -> logging.Logger:
    """
    Configure logger specifically for AWS Glue jobs.
    
    Returns:
        Configured logger for Glue
    """
    logger = logging.getLogger('GlueApp')
    logger.setLevel(logging.INFO)
    
    # Glue uses specific log format
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
Logging utilities for Glue jobs
Provides standardized logging across all jobs
"""
import logging
import sys
from datetime import datetime
from typing import Optional


class GlueLogger:
    """Standardized logger for Glue ETL jobs"""
    
    def __init__(self, job_name: str, log_level: str = "INFO"):
        """
        Initialize logger for Glue job
        
        Args:
            job_name: Name of the Glue job
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.job_name = job_name
        self.logger = self._setup_logger(log_level)
    
    def _setup_logger(self, log_level: str) -> logging.Logger:
        """Configure and return logger"""
        logger = logging.getLogger(self.job_name)
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level.upper()))
        
        # Formatter with timestamp and job name
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        if not logger.handlers:
            logger.addHandler(handler)
        
        return logger
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(self._format_message(message, kwargs))
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(self._format_message(message, kwargs))
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(self._format_message(message, kwargs))
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.logger.error(self._format_message(message, kwargs))
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self.logger.critical(self._format_message(message, kwargs))
    
    def _format_message(self, message: str, context: dict) -> str:
        """Format message with additional context"""
        if context:
            context_str = " | ".join([f"{k}={v}" for k, v in context.items()])
            return f"{message} | {context_str}"
        return message
    
    def log_job_start(self, **params):
        """Log job start with parameters"""
        self.info("Job started", **params)
    
    def log_job_end(self, duration_seconds: float, record_count: Optional[int] = None):
        """Log job completion"""
        context = {"duration_seconds": duration_seconds}
        if record_count is not None:
            context["records_processed"] = record_count
        self.info("Job completed successfully", **context)
    
    def log_transformation(self, transform_name: str, input_count: int, output_count: int):
        """Log transformation details"""
        self.info(
            f"Transformation: {transform_name}",
            input_records=input_count,
            output_records=output_count,
            filtered_records=input_count - output_count
        )
