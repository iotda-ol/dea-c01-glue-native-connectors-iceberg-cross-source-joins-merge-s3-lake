"""
Base Transform Module

Provides abstract base class for all data transformation operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging


class BaseTransform(ABC):
    """
    Abstract base class for data transformations.
    
    All transformation operations inherit from this class to ensure
    consistent interface and error handling.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the transform with configuration.
        
        Args:
            config: Optional configuration dictionary for the transform
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def apply(self, data: Any) -> Any:
        """
        Apply the transformation to the input data.
        
        Args:
            data: Input data (DynamicFrame, DataFrame, etc.)
            
        Returns:
            Transformed data in same format as input
        """
        pass
    
    def validate(self, data: Any) -> bool:
        """
        Validate input data before transformation.
        
        Args:
            data: Input data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        if data is None:
            self.logger.error("Input data is None")
            return False
        return True
    
    def get_output_schema(self, input_schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Get the output schema after transformation.
        
        Args:
            input_schema: Input data schema
            
        Returns:
            Output data schema
        """
        # By default, return input schema unchanged
        return input_schema
    
    def __call__(self, data: Any) -> Any:
        """
        Make the transform callable.
        
        Args:
            data: Input data
            
        Returns:
            Transformed data
        """
        if not self.validate(data):
            raise ValueError("Input data validation failed")
        return self.apply(data)
