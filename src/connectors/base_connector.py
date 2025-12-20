"""
Base Connector Interface
Provides abstract base class for all data source connectors
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame


class BaseConnector(ABC):
    """
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
