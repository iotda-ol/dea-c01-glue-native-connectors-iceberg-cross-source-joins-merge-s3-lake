"""
Connectors package
Data source connector implementations
"""
from .base_connector import BaseConnector
from .redshift_connector import RedshiftConnector
from .teradata_connector import TeradataConnector
from .bigquery_connector import BigQueryConnector
from .connection_factory import ConnectionFactory

__all__ = [
    'BaseConnector',
    'RedshiftConnector',
    'TeradataConnector',
    'BigQueryConnector',
    'ConnectionFactory'
]

__version__ = '1.0.0'
Connector package initialization
Exports all connector classes for easy importing
"""
from .redshift_connector import RedshiftConnector
from .teradata_connector import TeradataConnector
from .bigquery_connector import BigQueryConnector

__all__ = [
    'RedshiftConnector',
    'TeradataConnector',
    'BigQueryConnector'
]
