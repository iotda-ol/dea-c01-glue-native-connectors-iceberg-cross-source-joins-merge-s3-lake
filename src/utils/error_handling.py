"""
Error handling and retry utilities
"""
import time
import functools
from typing import Callable, Type, Tuple
import logging


def retry_on_exception(max_retries: int = 3,
                       delay: float = 1.0,
                       backoff: float = 2.0,
                       exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    Decorator to retry function on exception
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay (exponential backoff)
        exceptions: Tuple of exception types to catch
    
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        logging.error(f"All {max_retries} retry attempts failed for {func.__name__}")
                        raise
                    
                    logging.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay} seconds..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            
        return wrapper
    return decorator


class GlueJobError(Exception):
    """Base exception for Glue job errors"""
    pass


class ConnectionError(GlueJobError):
    """Exception for connection failures"""
    pass


class DataQualityError(GlueJobError):
    """Exception for data quality issues"""
    pass


class TransformationError(GlueJobError):
    """Exception for transformation failures"""
    pass
