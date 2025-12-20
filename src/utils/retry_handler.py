"""
Retry Handler Module

Implements retry logic with exponential backoff for resilient operations.
"""

import time
import functools
from typing import Callable, Any, Optional, Tuple, Type
import logging


def with_retry(max_attempts: int = 3,
               delay: float = 1.0,
               backoff: float = 2.0,
               exceptions: Tuple[Type[Exception], ...] = (Exception,),
               logger: Optional[logging.Logger] = None):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier (delay *= backoff after each attempt)
        exceptions: Tuple of exception types to catch and retry
        logger: Optional logger for retry messages
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            current_delay = delay
            log = logger or logging.getLogger(func.__module__)
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        log.error(f"Function {func.__name__} failed after {max_attempts} attempts: {str(e)}")
                        raise
                    
                    log.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay}s..."
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1
            
        return wrapper
    return decorator


class RetryHandler:
    """
    Class-based retry handler with more control over retry logic.
    """
    
    def __init__(self,
                 max_attempts: int = 3,
                 delay: float = 1.0,
                 backoff: float = 2.0,
                 max_delay: float = 60.0):
        """
        Initialize retry handler.
        
        Args:
            max_attempts: Maximum number of retry attempts
            delay: Initial delay between retries in seconds
            backoff: Backoff multiplier
            max_delay: Maximum delay between retries
        """
        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff = backoff
        self.max_delay = max_delay
        self.logger = logging.getLogger(__name__)
    
    def execute(self, 
                func: Callable,
                *args,
                exceptions: Tuple[Type[Exception], ...] = (Exception,),
                **kwargs) -> Any:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args: Positional arguments for function
            exceptions: Tuple of exception types to retry on
            **kwargs: Keyword arguments for function
            
        Returns:
            Function result
            
        Raises:
            Last exception if all retries fail
        """
        attempt = 1
        current_delay = self.delay
        last_exception = None
        
        while attempt <= self.max_attempts:
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                last_exception = e
                
                if attempt == self.max_attempts:
                    self.logger.error(f"All {self.max_attempts} attempts failed")
                    raise
                
                self.logger.warning(
                    f"Attempt {attempt}/{self.max_attempts} failed: {str(e)}. "
                    f"Retrying in {current_delay}s..."
                )
                
                time.sleep(current_delay)
                current_delay = min(current_delay * self.backoff, self.max_delay)
                attempt += 1
        
        if last_exception:
            raise last_exception


def retry_on_condition(condition_func: Callable[[Any], bool],
                      max_attempts: int = 3,
                      delay: float = 1.0,
                      backoff: float = 2.0):
    """
    Decorator to retry based on return value condition.
    
    Args:
        condition_func: Function that takes return value and returns True to retry
        max_attempts: Maximum number of attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            current_delay = delay
            
            while attempt <= max_attempts:
                result = func(*args, **kwargs)
                
                if not condition_func(result):
                    return result
                
                if attempt < max_attempts:
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1
                else:
                    return result
            
        return wrapper
    return decorator
