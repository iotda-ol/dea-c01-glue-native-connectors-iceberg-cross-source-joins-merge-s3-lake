"""
Connection Factory
Factory pattern for creating data source connectors
"""
from typing import Dict, Any
from .base_connector import BaseConnector
from .redshift_connector import RedshiftConnector
from .teradata_connector import TeradataConnector
from .bigquery_connector import BigQueryConnector


class ConnectionFactory:
    """
    Factory class for creating data source connectors
    Implements factory pattern for flexible connector instantiation
    """
    
    # Registry of available connectors
    CONNECTORS = {
        'redshift': RedshiftConnector,
        'teradata': TeradataConnector,
        'bigquery': BigQueryConnector,
    }
    
    @classmethod
    def create_connector(cls, 
                        source_type: str, 
                        config: Dict[str, Any], 
                        glue_context) -> BaseConnector:
        """
        Create a connector instance based on source type
        
        Args:
            source_type: Type of data source ('redshift', 'teradata', 'bigquery')
            config: Configuration dictionary for the connector
            glue_context: AWS Glue context object
            
        Returns:
            BaseConnector: Instance of the appropriate connector
            
        Raises:
            ValueError: If source_type is not supported
        """
        source_type = source_type.lower()
        
        if source_type not in cls.CONNECTORS:
            raise ValueError(
                f"Unsupported source type: {source_type}. "
                f"Supported types: {', '.join(cls.CONNECTORS.keys())}"
            )
        
        connector_class = cls.CONNECTORS[source_type]
        return connector_class(config, glue_context)
    
    @classmethod
    def register_connector(cls, source_type: str, connector_class: type):
        """
        Register a new connector type
        
        Args:
            source_type: Unique identifier for the connector
            connector_class: Connector class (must inherit from BaseConnector)
        """
        if not issubclass(connector_class, BaseConnector):
            raise TypeError(
                f"Connector class must inherit from BaseConnector"
            )
        
        cls.CONNECTORS[source_type.lower()] = connector_class
    
    @classmethod
    def get_supported_types(cls) -> list:
        """
        Get list of supported connector types
        
        Returns:
            list: List of supported source types
        """
        return list(cls.CONNECTORS.keys())
