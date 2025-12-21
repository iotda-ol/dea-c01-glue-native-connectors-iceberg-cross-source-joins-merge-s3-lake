"""
Source Package Initialization

This package contains modular, reusable code for the data pipeline.
"""

__version__ = '1.0.0'
__author__ = 'Data Engineering Team'

# Import main modules for easy access
from src import connectors
from src import transforms
from src import iceberg
from src import utils

__all__ = [
    'connectors',
    'transforms',
    'iceberg',
    'utils'
]
