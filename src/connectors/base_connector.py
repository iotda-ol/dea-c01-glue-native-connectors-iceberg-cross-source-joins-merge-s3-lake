"""
Base Connector Module

This module provides the abstract base class for all database connectors.
All connector implementations (Redshift, Teradata, BigQuery) inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    
    Provides common interface for connecting to various data sources
    and executing queries with consistent error handling and logging.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration.
        
        Args:
            config: Dictionary containing connection configuration
                   (host, port, database, credentials, etc.)
        """
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the data source.
        
        Raises:
            ConnectionError: If connection cannot be established
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection to the data source.
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a SQL query against the data source.
        
        Args:
            query: SQL query string to execute
            params: Optional parameters for query parameterization
            
        Returns:
            Query results (format depends on implementation)
            
        Raises:
            QueryExecutionError: If query execution fails
        """
        pass
    
    @abstractmethod
    def get_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve schema information for a table.
        
        Args:
            table_name: Name of the table to get schema for
            
        Returns:
            List of column definitions with name, type, and metadata
            
        Raises:
            SchemaRetrievalError: If schema cannot be retrieved
        """
        pass
    
    def test_connection(self) -> bool:
        """
        Test if connection to data source is successful.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connect()
            self.disconnect()
            self.logger.info(f"Connection test successful for {self.__class__.__name__}")
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def validate_config(self) -> bool:
        """
        Validate the connector configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields = ['host', 'database']
        for field in required_fields:
            if field not in self.config:
                self.logger.error(f"Missing required configuration field: {field}")
                return False
        return True
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
