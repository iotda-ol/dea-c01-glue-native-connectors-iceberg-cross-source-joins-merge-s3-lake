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
