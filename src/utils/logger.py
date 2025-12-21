"""
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
