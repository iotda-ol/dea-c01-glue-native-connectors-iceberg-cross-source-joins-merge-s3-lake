"""
Error Handler Module

Centralized error handling and exception management.
"""

import sys
import traceback
from typing import Optional, Callable, Any
import logging
from enum import Enum


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DataPipelineError(Exception):
    """Base exception for data pipeline errors."""
    
    def __init__(self, message: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM):
        self.message = message
        self.severity = severity
        super().__init__(self.message)


class ConnectionError(DataPipelineError):
    """Exception for connection failures."""
    pass


class QueryExecutionError(DataPipelineError):
    """Exception for query execution failures."""
    pass


class ValidationError(DataPipelineError):
    """Exception for data validation failures."""
    pass


class TransformationError(DataPipelineError):
    """Exception for data transformation failures."""
    pass


class IcebergOperationError(DataPipelineError):
    """Exception for Iceberg operation failures."""
    pass


class ErrorHandler:
    """
    Centralized error handler for the data pipeline.
    
    Provides consistent error handling, logging, and notification.
    """
    
    def __init__(self, 
                 logger: Optional[logging.Logger] = None,
                 sns_topic_arn: Optional[str] = None):
        """
        Initialize error handler.
        
        Args:
            logger: Logger instance for error logging
            sns_topic_arn: Optional SNS topic ARN for error notifications
        """
        self.logger = logger or logging.getLogger(__name__)
        self.sns_topic_arn = sns_topic_arn
        self.sns_client = None
        
        if sns_topic_arn:
            import boto3
            self.sns_client = boto3.client('sns')
    
    def handle_error(self,
                    error: Exception,
                    context: Optional[str] = None,
                    notify: bool = False) -> None:
        """
        Handle an error with logging and optional notification.
        
        Args:
            error: Exception that occurred
            context: Additional context about where error occurred
            notify: Whether to send SNS notification
        """
        # Format error message
        error_msg = f"Error: {str(error)}"
        if context:
            error_msg = f"{context}: {error_msg}"
        
        # Get stack trace
        exc_type, exc_value, exc_traceback = sys.exc_info()
        stack_trace = ''.join(traceback.format_tb(exc_traceback))
        
        # Determine severity
        severity = ErrorSeverity.MEDIUM
        if isinstance(error, DataPipelineError):
            severity = error.severity
        
        # Log error
        if severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            self.logger.error(f"{error_msg}\n{stack_trace}")
        else:
            self.logger.warning(error_msg)
        
        # Send notification if requested and configured
        if notify and self.sns_client and self.sns_topic_arn:
            self._send_notification(error_msg, severity, stack_trace)
    
    def _send_notification(self,
                          message: str,
                          severity: ErrorSeverity,
                          stack_trace: str) -> None:
        """
        Send error notification via SNS.
        
        Args:
            message: Error message
            severity: Error severity
            stack_trace: Stack trace
        """
        try:
            subject = f"[{severity.value.upper()}] Data Pipeline Error"
            body = f"{message}\n\nStack Trace:\n{stack_trace}"
            
            self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=subject,
                Message=body
            )
            
            self.logger.info("Error notification sent via SNS")
        except Exception as e:
            self.logger.error(f"Failed to send SNS notification: {str(e)}")
    
    def wrap_function(self, 
                     func: Callable,
                     context: Optional[str] = None,
                     notify_on_error: bool = False) -> Callable:
        """
        Wrap a function with error handling.
        
        Args:
            func: Function to wrap
            context: Context description for errors
            notify_on_error: Whether to send notifications on error
            
        Returns:
            Wrapped function with error handling
        """
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.handle_error(
                    e,
                    context=context or func.__name__,
                    notify=notify_on_error
                )
                raise
        
        return wrapper


def graceful_error_handler(logger: Optional[logging.Logger] = None):
    """
    Decorator for graceful error handling.
    
    Args:
        logger: Optional logger instance
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            log = logger or logging.getLogger(func.__module__)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
                raise
        return wrapper
    return decorator
