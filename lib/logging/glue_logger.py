"""
Enhanced Logger for AWS Glue Jobs
Provides structured logging with CloudWatch integration
"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional


class GlueJobLogger:
    """
    Enhanced logger for Glue jobs with structured logging
    """
    
    def __init__(self, job_name: str, log_level: str = "INFO"):
        """
        Initialize logger
        
        Args:
            job_name: Name of the Glue job
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.job_name = job_name
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Configure formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Add console handler
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Metrics storage
        self.metrics = {}
        self.start_time = datetime.now()
    
    def log_structured(self, 
                      level: str, 
                      message: str, 
                      **kwargs):
        """
        Log structured message with additional context
        
        Args:
            level: Log level
            message: Log message
            **kwargs: Additional context to include
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "job_name": self.job_name,
            "message": message,
            **kwargs
        }
        
        log_func = getattr(self.logger, level.lower())
        log_func(json.dumps(log_entry))
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.log_structured("INFO", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.log_structured("ERROR", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.log_structured("WARNING", message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.log_structured("DEBUG", message, **kwargs)
    
    def log_metric(self, metric_name: str, value: Any):
        """
        Log a metric value
        
        Args:
            metric_name: Name of the metric
            value: Metric value
        """
        self.metrics[metric_name] = value
        self.info(f"Metric: {metric_name} = {value}", metric=metric_name, value=value)
    
    def log_dataframe_stats(self, df, df_name: str):
        """
        Log DataFrame statistics
        
        Args:
            df: PySpark DataFrame
            df_name: Name to identify the DataFrame
        """
        row_count = df.count()
        column_count = len(df.columns)
        
        self.info(
            f"DataFrame stats: {df_name}",
            df_name=df_name,
            row_count=row_count,
            column_count=column_count,
            columns=df.columns
        )
        
        self.log_metric(f"{df_name}_row_count", row_count)
        self.log_metric(f"{df_name}_column_count", column_count)
    
    def log_job_start(self, **params):
        """
        Log job start with parameters
        
        Args:
            **params: Job parameters
        """
        self.start_time = datetime.now()
        self.info(
            f"Job started: {self.job_name}",
            event="job_start",
            parameters=params
        )
    
    def log_job_end(self, status: str = "success", **kwargs):
        """
        Log job completion
        
        Args:
            status: Job status (success/failure)
            **kwargs: Additional context
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        self.info(
            f"Job completed: {self.job_name}",
            event="job_end",
            status=status,
            duration_seconds=duration,
            metrics=self.metrics,
            **kwargs
        )
    
    def log_error_with_traceback(self, error: Exception):
        """
        Log error with full traceback
        
        Args:
            error: Exception object
        """
        import traceback
        
        self.error(
            f"Exception occurred: {str(error)}",
            error_type=type(error).__name__,
            error_message=str(error),
            traceback=traceback.format_exc()
        )
