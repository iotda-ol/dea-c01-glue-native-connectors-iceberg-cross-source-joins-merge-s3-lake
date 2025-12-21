"""
Base Connector Module

This module provides the abstract base class for all database connectors.
All connector implementations (Redshift, Teradata, BigQuery) inherit from this class.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging
Base Connector Interface
Provides abstract base class for all data source connectors
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame


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
    Abstract base class for data source connectors.
    All connectors should inherit from this class and implement required methods.
    """
    
    def __init__(self, config: Dict[str, Any], glue_context):
        """
        Initialize the connector
        
        Args:
            config: Configuration dictionary containing connection parameters
            glue_context: AWS Glue context object
        """
        self.config = config
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.logger = self._setup_logger()
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source
        
        Returns:
            bool: True if connection successful, False otherwise
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
    def read_table(self, 
                   table_name: str, 
                   schema: Optional[str] = None,
                   predicates: Optional[List[str]] = None,
                   columns: Optional[List[str]] = None) -> DataFrame:
        """
        Read data from a table
        
        Args:
            table_name: Name of the table to read
            schema: Schema/database name (optional)
            predicates: List of filter predicates to push down (optional)
            columns: List of columns to select (optional)
            
        Returns:
            DataFrame: Spark DataFrame containing the data
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
    def get_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, str]:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema/database name (optional)
            
        Returns:
            Dict: Dictionary mapping column names to data types
        """
        pass
    
    def validate_connection(self) -> bool:
        """
        Validate that the connection is working
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        try:
            return self.connect()
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False
    
    def close(self):
        """
        Close the connection and clean up resources
        """
        self.logger.info(f"Closing connector: {self.__class__.__name__}")
    
    def _setup_logger(self):
        """
        Set up logger for the connector
        
        Returns:
            Logger instance
        """
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        return logger
    
    def _apply_predicates(self, df: DataFrame, predicates: Optional[List[str]]) -> DataFrame:
        """
        Apply filter predicates to DataFrame
        
        Args:
            df: Input DataFrame
            predicates: List of SQL-like predicates
            
        Returns:
            DataFrame: Filtered DataFrame
        """
        if predicates:
            for predicate in predicates:
                df = df.filter(predicate)
        return df
    
    def _select_columns(self, df: DataFrame, columns: Optional[List[str]]) -> DataFrame:
        """
        Select specific columns from DataFrame
        
        Args:
            df: Input DataFrame
            columns: List of column names
            
        Returns:
            DataFrame: DataFrame with selected columns
        """
        if columns:
            return df.select(*columns)
        return df
