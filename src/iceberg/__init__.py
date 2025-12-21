"""
Iceberg Module Initialization

Exports Iceberg operation classes.
"""

from src.iceberg.table_manager import IcebergTableManager
from src.iceberg.merge_operations import IcebergMergeOperations

__all__ = [
    'IcebergTableManager',
    'IcebergMergeOperations'
]
Iceberg package
Apache Iceberg table management utilities
"""
from .table_manager import IcebergTableManager

__all__ = ['IcebergTableManager']

__version__ = '1.0.0'
